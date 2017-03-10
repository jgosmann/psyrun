"""Defines the ``psy`` command line interface commands."""

import argparse
import os.path
import shutil
import sys
import warnings

from psyrun.backend.distribute import Splitter
from psyrun.exceptions import TaskWorkdirDirtyWarning
from psyrun.jobs import Clean, Fullname, Submit, Uptodate
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

        if not cont:
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
        ext = os.path.splitext(self.args.merged)[1].lower()
        if ext == '.npz':
            from psyrun.store.npz import NpzStore as Store
        elif ext == '.h5':
            from psyrun.store.h5 import H5Store as Store
        else:
            from psyrun.store.pickle import PickleStore as Store
        Splitter.merge(self.args.directory, self.args.merged, store=Store())


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
        missing = task.backend(task).get_missing()
        print("{}:".format(task.name))
        print("  {} out of {} rows completed.".format(
            len(task.pspace) - len(missing), len(task.pspace)))
        if self.args.verbose:
            print("  Missing parameter sets:")
            for pset in missing.iterate():
                print('   ', pset)
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
    'list': ListCmd,
    'merge': MergeCmd,
    'status': StatusCmd,
    'test': TestCmd,
})


if __name__ == '__main__':
    sys.exit(psy_main())
