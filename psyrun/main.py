"""Defines the ``psy`` command line interface commands."""

from __future__ import print_function

import argparse
from importlib import import_module
import os
import os.path
import pkg_resources
import shutil
import sys
import warnings

from psyrun.backend.distribute import Splitter
from psyrun.exceptions import TaskWorkdirDirtyWarning
from psyrun.jobs import Clean, Fullname, Submit, Uptodate
from psyrun.store import AutodetectStore
from psyrun.tasks import PackageLoader
from psyrun.utils.venv import init_virtualenv


commands = {}


def psy_main(argv=None, init_venv=True):
    """Runs psyrun tasks.

    Parameters
    ----------
    argv : sequence of str, optional
        ``psy`` command line arguments.
    init_venv : bool, optional
        Use the virtualenv active in the shell environment if set to True.

    Returns
    -------
    int
        Return code.
    """
    if init_venv:
        init_virtualenv()

    parser = argparse.ArgumentParser(
        epilog="available commands:\n{}\n".format(''.join(
            "  {: <11s} {}\n".format(k, v.short_desc)
            for k, v in sorted(commands.items()))),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('cmd', nargs=1, type=str, help="command to run")
    parser.add_argument('args', nargs=argparse.REMAINDER,
                        help="arguments to command")
    args = parser.parse_args(argv)

    return commands[args.cmd[0]](args.cmd[0], args.args).run()


class Command(object):
    """Base class for commands.

    Deriving classes are supposed to implement `run` and may overwrite
    `add_args` to add additional arguments to ``self.parser``.

    Parameters
    ----------
    cmd : str
        Command name.
    argv : sequence
        Arguments to the command.

    Attributes
    ----------
    parser : argparse.ArgumentParser
        Parser for arguments.
    args : argparse.Namespace
        Parsed arguments.
    """
    short_desc = ""
    long_desc = ""

    def __init__(self, cmd, argv):
        prog = os.path.basename(sys.argv[0]) + ' ' + cmd
        self.parser = argparse.ArgumentParser(
            prog=prog, description=self.long_desc)
        self.add_args()
        self.args = self.parser.parse_args(argv)

    def add_args(self):
        """Add command arguments to ``self.parser``."""
        pass

    def run(self):
        """Run the command."""
        raise NotImplementedError()


class TaskdirCmd(Command):
    """Base class for commands that accept a ``--taskdir`` argument."""

    def __init__(self, cmd, argv):
        super(TaskdirCmd, self).__init__(cmd, argv)
        self.package_loader = PackageLoader(self.args.taskdir[0])

    def add_args(self):
        super(TaskdirCmd, self).add_args()
        self.parser.add_argument(
            '--taskdir', nargs=1, type=str, default=['psy-tasks'],
            help="directory to load tasks from")

    def run(self):
        raise NotImplementedError()


class TaskselCmd(TaskdirCmd):
    """Base class for commands that accept a selection of tasks as arguments.

    Attributes
    ----------
    default_to_all : bool
        Indicates whether the command defaults to all (or no) tasks when no
        task names are specified as arguments.
    """

    default_to_all = True

    def add_args(self):
        super(TaskselCmd, self).add_args()
        self.parser.add_argument('task', nargs='*', type=str)

    def run(self):
        tasks = {t.name: t for t in self.package_loader.load_task_defs()}
        run_all = self.default_to_all and len(self.args.task) == 0
        if run_all:
            selected = tasks.keys()
        else:
            selected = self.args.task
        for name in selected:
            self.run_task(tasks[name])

    def run_task(self, task):
        raise NotImplementedError()


class RunCmd(TaskselCmd):
    short_desc = "run one or more tasks"
    long_desc = (
        "Submits the jobs to run one or more tasks. If no task name is "
        "provided, all tasks will be run.")

    def add_args(self):
        super(RunCmd, self).add_args()
        self.parser.add_argument(
            '-c', '--continue', action='store_true',
            help="preserve existing data in the workdir and do not rerun "
            "parameter assignment already existent in there")

    def run_task(self, task):
        cont = vars(self.args)['continue']
        backend = task.backend(task)

        job = backend.create_job(cont=cont)
        names = Fullname(job).names
        uptodate = Uptodate(job, names, task)

        if cont:
            if len(backend.get_missing()) > 0:
                for k in uptodate.status:
                    uptodate.status[k] = False
        else:
            if uptodate.outdated and os.path.exists(backend.resultfile):
                if not task.overwrite_dirty:
                    warnings.warn(TaskWorkdirDirtyWarning(task.name))
                    return

            Clean(job, task, names, uptodate)
        Submit(job, names, uptodate)


class CleanCmd(TaskselCmd):
    short_desc = "clean task data"
    long_desc = (
        "Removes all processing and result files associated with a task.")
    default_to_all = False

    def run_task(self, task):
        path = os.path.join(task.workdir, task.name)
        print('rm', path)
        shutil.rmtree(path)


class KillCmd(TaskselCmd):
    short_desc = "kill task jobs"
    long_desc = "Kill all running and queued task jobs."
    default_to_all = False

    def run_task(self, task):
        for job in task.scheduler.get_jobs():
            status = task.scheduler.get_status(job)
            if status is not None and status != 'D':
                task.scheduler.kill(job)


class ListCmd(TaskdirCmd):
    short_desc = "list tasks"
    long_desc = "Lists all available tasks."

    def run(self):
        for t in self.package_loader.load_task_defs():
            print(t.name)


class MergeCmd(Command):
    short_desc = "merge data files into single file"
    long_desc = "Merges all data files in a directory into a single data file."

    def add_args(self):
        self.parser.add_argument(
            'directory', type=str, help="directory with files to merge")
        self.parser.add_argument(
            'merged', type=str, help="file to write the merged result to")

    def run(self):
        store = AutodetectStore.get_concrete_store(self.args.merged)
        Splitter.merge(self.args.directory, self.args.merged, store=store)


class NewTaskCmd(TaskdirCmd):
    short_desc = "create new task"
    long_desc = "Copy task file template to a new task file."

    def add_args(self):
        super(NewTaskCmd, self).add_args()
        self.parser.add_argument('name', type=str, help="name of new task")
        self.parser.add_argument(
            '--scheduler', '-s', type=str, default='ImmediateRun',
            help="scheduler to use for task and pre-fill scheduler arguments")

    def run(self):
        scheduler = self.args.scheduler.rsplit('.', 1)
        assert len(scheduler) in [1, 2]
        if len(scheduler) == 1:
            scheduler_mod = 'psyrun.scheduler'
            scheduler = scheduler[0]
        elif len(scheduler) == 2:
            scheduler_mod, scheduler = scheduler

        mod = import_module(scheduler_mod)
        scheduler_cls = getattr(mod, scheduler)
        if hasattr(scheduler_cls, 'USER_DEFAULT_ARGS'):
            scheduler_args = scheduler_cls.USER_DEFAULT_ARGS
        else:
            scheduler_args = {}

        template = pkg_resources.resource_string('psyrun', 'task.py.template')
        template = template.decode('utf-8').format(
            scheduler_mod=scheduler_mod, scheduler=scheduler,
            scheduler_args=scheduler_args)

        path = os.path.join(
            self.args.taskdir[0], 'task_' + self.args.name + '.py')
        if not os.path.exists(self.args.taskdir[0]):
            os.makedirs(self.args.taskdir[0])
        elif os.path.exists(path):
            print('Task {} already exists.'.format(self.args.name),
                  file=sys.stderr)
            return -1

        with open(path, 'w') as f:
            f.write(template)


class StatusCmd(TaskselCmd):
    short_desc = "print status of tasks"
    long_desc = (
        "Prints the status (number of completed parameter assignments of the "
        "total). Can also print parameter assignments that have not been "
        "evaluated yet.")

    def add_args(self):
        super(StatusCmd, self).add_args()
        self.parser.add_argument(
            '-v', '--verbose', action='store_true',
            help="print missing parameter sets")

    def run_task(self, task):
        backend = task.backend(task)
        missing = backend.get_missing()
        print("{}:".format(task.name))
        print("  {} out of {} rows completed.".format(
            len(task.pspace) - len(missing), len(task.pspace)))
        if self.args.verbose:
            print("  Missing parameter sets:")
            for pset in missing.iterate():
                print('   ', pset)

            queued = backend.get_queued()
            if queued is not None:
                print("  Queued parameter sets:")
                for pset in queued.iterate():
                    print('   ', pset)

            failed = backend.get_failed()
            if failed is not None:
                if len(failed) > 0:
                    print("  Failed jobs:")
                    for job in failed:
                        print('   ', job)
                else:
                    print("  No failed jobs.")

        print("")


class TestCmd(TaskselCmd):
    short_desc = "test execution of single parameter assignment"
    long_decs = (
        "Tests the task execution by running a single parameter assignment. "
        "The job will not be submitted with to the scheduler, but run "
        "immediatly.")

    def run_task(self, task):
        print(task.name)
        task.execute(**next(task.pspace.iterate()))


commands.update({
    'run': RunCmd,
    'clean': CleanCmd,
    'kill': KillCmd,
    'list': ListCmd,
    'merge': MergeCmd,
    'new-task': NewTaskCmd,
    'status': StatusCmd,
    'test': TestCmd,
})


if __name__ == '__main__':
    sys.exit(psy_main())
