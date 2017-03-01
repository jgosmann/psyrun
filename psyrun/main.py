import argparse
import os.path
import shutil
import warnings

from psyrun.backend.distribute import Splitter
from psyrun.exceptions import TaskWorkdirDirtyWarning
from psyrun.jobs import Clean, Fullname, Submit, Uptodate
from psyrun.tasks import PackageLoader
from psyrun.utils.venv import init_virtualenv


commands = {}


# TODO document commands
def psy_main(argv=None, init_venv=True):
    """Runs psyrun tasks.

    Parameters
    ----------
    argv : sequence of str, optional
        psyrun command line arguments.
    init_venv : bool, optional
        Use the virtualenv active in the shell environment if set to ``True``.

    Returns
    -------
    int
        Return code.
    """
    if init_venv:
        init_virtualenv()

    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', nargs=1, type=str, help="Command to run.")
    parser.add_argument('args', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    return commands[args.cmd[0]](args.args).run()


class Command(object):
    def __init__(self, argv):
        self.parser = argparse.ArgumentParser()
        self.add_args()
        self.args = self.parser.parse_args(argv)

    def add_args(self):
        pass

    def run(self):
        raise NotImplementedError()


class TaskdirCmd(Command):
    def __init__(self, argv):
        super(TaskdirCmd, self).__init__(argv)
        self.package_loader = PackageLoader(self.args.taskdir[0])

    def add_args(self):
        super(TaskdirCmd, self).add_args()
        self.parser.add_argument(
            '--taskdir', nargs=1, type=str, default=['psy-tasks'],
            help="Directory to load tasks from.")

    def run(self):
        raise NotImplementedError()


class TaskselCmd(TaskdirCmd):
    default_to_all = True

    def add_args(self):
        super(TaskselCmd, self).add_args()
        self.parser.add_argument('task', nargs='*', type=str)

    def run(self):
        run_all = self.default_to_all and len(self.args.task) == 0
        for t in self.package_loader.load_task_defs():
            if run_all or t.name in self.args.task:
                return self.run_task(t)

    def run_task(self, task):
        raise NotImplementedError()


class RunCmd(TaskselCmd):
    def add_args(self):
        super(RunCmd, self).add_args()
        self.parser.add_argument(
            '-c', '--continue', action='store_true',
            help="Preserve existing data in the workdir and do not rerun"
            "parameter assignment already existent in there.")

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
    default_to_all = False

    def run_task(self, task):
        path = os.path.join(task.workdir, task.name)
        print('rm', path)
        shutil.rmtree(path)


class ListCmd(TaskdirCmd):
    def run(self):
        for t in self.package_loader.load_task_defs():
            print(t.name)


class MergeCmd(Command):
    def add_args(self):
        self.parser.add_argument(
            'directory', type=str, help="Directory with files to merge.")
        self.parser.add_argument(
            'merged', type=str, help="File to write the merged result to.")

    def run(self):
        Splitter.merge(self.args.directory, self.args.merged)


class StatusCmd(TaskselCmd):
    def add_args(self):
        super(StatusCmd, self).add_args()
        self.parser.add_argument(
            '-v', '--verbose', action='store_true',
            help="Print missing parameter sets.")

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
