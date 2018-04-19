"""Psyrun specific exceptions and warnings."""

import warnings


class TaskWarning(UserWarning):
    """General warning related to task processing.

    *TaskWarnings* will always be shown by default.
    """
    def __init__(self, task_name, *args, **kwargs):
        super(TaskWarning, self).__init__(*args, **kwargs)
        self.task_name = task_name


class JobsRunningWarning(TaskWarning):
    """Warning issued when jobs for a task are still running."""
    def __init__(self, task_name):
        super(JobsRunningWarning, self).__init__(
            task_name,
            "Task '{}' has unfinished jobs queued. Not starting new jobs "
            "until these are finished or have been killed.".format(task_name))


class TaskWorkdirDirtyWarning(TaskWarning):
    """Warning issued when the workdir is dirty and would be overwritten."""
    def __init__(self, task_name):
        # TODO explain how to solve this
        super(TaskWorkdirDirtyWarning, self).__init__(
            task_name,
            "Work directory of task '{}' is dirty.".format(task_name))


class IneffectiveExcludeWarning(UserWarning):
    """Warning issued when a key in *exclude_from_result* was not found in the
    result.
    """
    def __init__(self, key):
        super(IneffectiveExcludeWarning, self).__init__(
            "The key '{}' to remove from the result was not found in the "
            "result dictionary.".format(key))
        self.key = key


warnings.simplefilter('always', category=TaskWarning)
