import functools
import pkgutil
import os
import os.path
import re
import sys

from doit.task import dict_to_task
from doit.cmd_base import TaskLoader
from doit.doit_cmd import DoitMain

from psyrun.scheduler import ImmediateRun


def submit(sched, args, depends_on=None):
    # FIXME depends_on
    return {'id': sched.submit(args)}


class PackageLoader(TaskLoader):
    TASK_PATTERN = re.compile(r'^task_(.*)$')

    def __init__(self, taskdir, workdir):
        self.taskdir = taskdir
        self.workdir = workdir

    def load_tasks(self, cmd, opt_values, pos_args):
        sched = ImmediateRun()
        python = 'python'

        task_list = []

        for module_loader, name, _ in pkgutil.iter_modules([self.taskdir]):
            m = self.TASK_PATTERN.match(name)
            if m:
                mod = module_loader.find_module(name).load_module(name)
                task_name = m.group(1)

                task_list.append(dict_to_task({
                    'name': task_name + ':split',
                    'file_dep': [mod.__file__],
                    'actions': [(submit, [sched, [
                        python, '-c', '''
import pkgutil
from psyrun.fanout import SingleItemFanOut as FanOut

pspace = pkgutil.ImpImporter({taskdir!r}).find_module(
    {name!r}).load_module({name!r}).pspace
FanOut({workdir!r}).split(pspace)'''.format(taskdir=self.taskdir, workdir=self.workdir, name=name)]])],
                    'targets': [
                        os.path.join(
                            self.workdir, 'in', '{0}.h5'.format(i))
                        for i in range(len(mod.pspace))],  # FIXME
                }))

                # FIXME targets and file_deps have to be set by fanout
                for i in range(len(mod.pspace)):
                    infile = os.path.join(
                        self.workdir, 'in', '{0}.h5'.format(i))
                    outfile = os.path.join(
                        self.workdir, 'out', '{0}.h5'.format(i))
                    task_list.append(dict_to_task({
                        'name': task_name + ':process:{0}'.format(i),
                        'task_dep': [task_name + ':split'],
                        'file_dep': [infile],
                        'actions': [(submit, [sched, [
                            python, '-c', '''
import pkgutil
from psyrun.worker import SerialWorker as Worker

fn = pkgutil.ImpImporter({taskdir!r}).find_module(
    {name!r}).load_module({name!r}).execute
Worker().start(fn, {infile!r}, {outfile!r})'''.format(
                            taskdir=self.taskdir, name=name,
                            infile=infile, outfile=outfile)]])],
                        'targets': [outfile],
                        'getargs': {'depends_on': (task_name + ':split', 'id')}
                    }))

                result_file = os.path.join(self.workdir, 'result.h5')
                task_list.append(dict_to_task({
                    'name': task_name + ':merge',
                    'task_dep': [task_name + ':process:{0}'.format(i) for i in range(len(mod.pspace))],
                    'file_dep': [
                        os.path.join(
                            self.workdir, 'out', '{0}.h5'.format(i))
                        for i in range(len(mod.pspace))],
                    'actions': [(submit, [sched, [
                        python, '-c', '''
import pkgutil
from psyrun.fanout import SingleItemFanOut as FanOut

FanOut({workdir!r}).merge({filename!r})'''.format(workdir=self.workdir, filename=result_file)]])],
                    
                    'targets': [result_file],
                    #'getargs': {'depends_on': (task_name + ':process', 'id')}
                }))

        return task_list, {}


def psydoit(taskdir, workdir):
    return DoitMain(PackageLoader(taskdir, workdir)).run(sys.argv[1:])
