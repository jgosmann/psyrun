"""Psyrun's task loading API."""

from __future__ import print_function

import os
import os.path
import re
import sys
import traceback
import warnings

from psyrun.backend import DefaultBackend
from psyrun.pspace import Param
from psyrun.store import DefaultStore
from psyrun.scheduler import ImmediateRun


class TaskDef(object):
    """Task defined by a Python file.

    Parameters
    ----------
    path : str
        Python file to load as task.
    conf : `Config`
        Default values for task parameters.

    Attributes
    ----------
    TASK_PATTERN : re.RegexObject
        Regular expression to match task filenames.
    """

    TASK_PATTERN = re.compile(r'^task_(.*)$')

    def __init__(self, path, conf=None):
        if conf is None:
            taskdir = os.path.dirname(path)
            conffile = os.path.join(taskdir, 'psy-conf.py')
            if os.path.exists(conffile):
                conf = Config.load_from_file(conffile)
            else:
                conf = Config()

        _set_public_attrs_from_dict(
            self, _load_pyfile(path), only_existing=False)
        self.path = path

        if not hasattr(self, 'name'):
            prefixed_name, _ = os.path.splitext(os.path.basename(path))
            m = self.TASK_PATTERN.match(prefixed_name)
            if m:
                self.name = m.group(1)
            else:
                self.name = prefixed_name

        conf.apply_as_default(self)


def _load_pyfile(filename):
    source = ''
    with open(filename, 'r') as f:
        source += f.read()
    code = compile(source, filename, 'exec')
    loaded = {'__file__': filename}
    exec(code, loaded)  # pylint: disable=exec-used
    return loaded


def _set_public_attrs_from_dict(obj, d, only_existing=True):
    for k, v in d.items():
        if not k.startswith('_') and (not only_existing or hasattr(obj, k)):
            setattr(obj, k, v)


class Config(object):  # pylint: disable=too-many-instance-attributes
    """Task configuration.


    Attributes
    ----------
    backend : `Backend`, default: `DistributeBackend`
        The processing backend which determines how work is distributed across
        jobs.
    file_dep : sequence of str, default: ``[]``
        Additional files the task depends on.
    max_jobs : int, default: 100
        Maximum number of jobs to start. With less jobs each job has to process
        more parameter assignments. It depends on the  scheduler and backend
        used to which degree these will run in parallel.
    min_items : int, default: 1
        Minimum number of parameter assignment to evaluate per job. If a single
        assignment is fast to evaluate, increasing this number can improve
        performance because Psyrun will not start a new job for each parameter
        assignment which can save some overhead.
    overwrite_dirty : bool, default: True
        Whether to overwrite dirty workdirs without a warning.
    pspace : `ParameterSpace`, required
        Parameter space to evaluate.
    python : str, default: ``sys.executable``
        Path to Python interpreter to use.
    resultfile : str or None, default: None
        Path to save the results of the finished task at. If None, this
        defaults to ``'result.<ext>'`` in the *workdir*.
    scheduler : `Scheduler`, default: `ImmediateRun`
        Scheduler to use to submit individual jobs.
    scheduler_args : dict, default: ``{}``
        Additional scheduler arguments. See the documentation of the
        scheduler for details.
    store : `Store`, default: `PickleStore`
        Input/output backend.
    workdir : str, default: ``'psy-work'``
        Working directory to store results and supporting data to process the
        task.

    """

    __slots__ = [
        'backend', 'file_dep', 'max_jobs', 'min_items', 'pspace',
        'overwrite_dirty', 'python', 'resultfile', 'scheduler',
        'scheduler_args', 'store', 'workdir']

    def __init__(self):
        self.backend = DefaultBackend
        self.file_dep = []
        self.max_jobs = 100
        self.min_items = 1
        self.overwrite_dirty = True
        self.pspace = Param()
        self.python = sys.executable
        self.resultfile = None
        self.scheduler = ImmediateRun()
        self.scheduler_args = dict()
        self.store = DefaultStore()
        self.workdir = os.path.abspath('psy-work')

    @classmethod
    def load_from_file(cls, filename):
        """Load the configuration values from a Python file.

        Parameters
        ----------
        filename : str
            Python file to load.
        """
        conf = cls()
        loaded_conf = _load_pyfile(filename)
        _set_public_attrs_from_dict(conf, loaded_conf)
        return conf

    def apply_as_default(self, task):
        """Copies the attributes to a different object given they are not set
        in that object.

        Parameters
        ----------
        task : obj
            Object to copy the attributes to.
        """
        for attr in self.__slots__:
            if not hasattr(task, attr):
                setattr(task, attr, getattr(self, attr))


class PackageLoader(object):
    """Loads tasks from Python files.

    Filenames have to match the regular expression defined in
    `TaskDef.TASK_PATTERN`. See `Config` for supported module
    level variables in the task definition.

    It is possible to set these variables for all tasks by setting them in
    the file ``psy-conf.py`` in the *taskdir*.

    Parameters
    ----------
    taskdir : str
        Directory to load task files from.

    Attributes
    ----------
    taskdir : str
        Directory to load task files from.
    conf : `Config`
        Default values for module level task variables.
    """
    def __init__(self, taskdir):
        super(PackageLoader, self).__init__()
        self.taskdir = taskdir
        conffile = os.path.join(self.taskdir, 'psy-conf.py')
        if os.path.exists(conffile):
            self.conf = Config.load_from_file(conffile)
        else:
            self.conf = Config()

    def load_task_defs(self):
        """Load task definitions.

        Returns
        -------
        list of `TaskDef`
            Task definitions.
        """
        task_defs = []
        for filename in os.listdir(self.taskdir):
            root, ext = os.path.splitext(filename)
            if TaskDef.TASK_PATTERN.match(root) and ext == '.py':
                path = os.path.join(self.taskdir, filename)
                try:
                    task_defs.append(TaskDef(path, self.conf))
                except Exception:  # pylint: disable=broad-except
                    traceback.print_exc()
                    warnings.warn("Task {path!r} could not be loaded.".format(
                        path=path))
        return task_defs
