import argparse
import os.path
import shutil

from psyrun.processing import Splitter
from psyrun.tasks import Clean, Fullname, PackageLoader, Submit, Uptodate
from psyrun.venv import init_virtualenv


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
    def run_task(self, task):
        backend = task.backend(task)
        job = backend.create_job()
        names = Fullname(job).names
        uptodate = Uptodate(job, names, task)
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
    def run_task(self, task):
        completed, n_rows = task.backend(task).get_status()
        print("{}: {} out of {} rows completed.".format(
            task.name, completed, n_rows))


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
