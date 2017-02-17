import argparse
import os.path
import shutil

from psyrun.psydoit import Fullname, PackageLoader, Submit, Uptodate
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
    parser.add_argument(
        '--taskdir', nargs=1, type=str, default=['psy-tasks'],
        help="Directory to load tasks from.")
    parser.add_argument('cmd', nargs=1, type=str, help="Command to run.")
    parser.add_argument('args', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    package_loader = PackageLoader(args.taskdir[0])
    return commands[args.cmd[0]](package_loader, args.args)


def cmd_run(package_loader, argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('task', nargs='*', type=str)
    args = parser.parse_args(argv)

    for t in package_loader.load_task_defs():
        if len(args.task) == 0 or t.name in args.task:
            backend = t.backend(t)
            job = backend.create_job()
            names = Fullname(job).names
            uptodate = Uptodate(job, names, t)
            Submit(job, names, uptodate)


def cmd_clean(package_loader, argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('task', nargs='*', type=str)
    args = parser.parse_args(argv)

    for t in package_loader.load_task_defs():
        if t.name in args.task:
            path = os.path.join(t.workdir, t.name)
            print('rm', path)
            shutil.rmtree(path)


def cmd_list(package_loader, argv):
    parser = argparse.ArgumentParser()
    parser.parse_args(argv)

    for t in package_loader.load_task_defs():
        print(t.name)


def cmd_test(package_loader, argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('task', nargs='*', type=str)
    args = parser.parse_args(argv)

    for t in package_loader.load_task_defs():
        if len(args.task) <= 0 or t.name in args.task:
            print(t.name)
            t.execute(**next(t.pspace.iterate()))


commands.update({
    'run': cmd_run,
    'clean': cmd_clean,
    'list': cmd_list,
    'test': cmd_test,
})
