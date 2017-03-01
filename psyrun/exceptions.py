"""Psyrun specific exceptions and warnings."""

import warnings


class TaskWarning(UserWarning):
    """General warning related to task processing.

    *TaskWarnings* will always be shown by default.
    """
    pass


class JobsRunningWarning(TaskWarning):
    """Warning issued when jobs for a task are still running."""
    def __init__(self, name):
        self.task = name
        super(JobsRunningWarning, self).__init__(
            "Task '{}' has unfinished jobs queued. Not starting new jobs "
            "until these are finished or have been killed.".format(name))


class TaskWorkdirDirtyWarning(TaskWarning):
    """Warning issued when the workdir is dirty and would be overwritten."""
    def __init__(self, name):
        # TODO explain how to solve this
        self.task = name
        super(TaskWorkdirDirtyWarning, self).__init__(
            "Work directory of task '{}' is dirty.".format(name))


warnings.simplefilter('always', category=TaskWarning)
