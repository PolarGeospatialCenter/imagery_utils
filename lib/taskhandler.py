#!/usr/bin/env python

"""
task handler classes and methods
"""

import codecs
import logging
import multiprocessing as mp
import os
import platform
import signal
import subprocess

#### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


class Task(object):

    def __init__(self, task_name, task_abrv, task_exe, task_cmd, task_method=None, task_method_arg_list=None):
        self.name = task_name
        self.abrv = task_abrv
        self.exe = task_exe
        self.cmd = task_cmd
        self.method = task_method
        self.method_arg_list = task_method_arg_list


class PBSTaskHandler(object):

    def __init__(self, qsubscript, qsub_args=""):

        ####  verify PBS is present by calling pbsnodes cmd
        try:
            cmd = "pbsnodes"
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            so, se = p.communicate()
        except OSError:
            raise RuntimeError("PBS job submission is not available on this system")

        self.qsubscript = qsubscript
        if not qsubscript:
            raise RuntimeError("PBS job submission resuires a valid qsub script")
        elif not os.path.isfile(qsubscript):
            raise RuntimeError("Qsub script does not exist: {}".format(qsubscript))

        self.qsub_args = qsub_args

    def run_tasks(self, tasks, dryrun=False):

        for task in tasks:
            cmd = r'qsub {} -N {} -v p1="{} {}" "{}"'.format(
                self.qsub_args,
                task.abrv,
                task.exe,
                escape_problem_jobsubmit_chars(task.cmd),
                self.qsubscript
            )
            if dryrun:
                print(cmd)
            else:
                subprocess.call(cmd, shell=True)


class SLURMTaskHandler(object):

    def __init__(self, qsubscript, qsub_args=""):

        ####  verify SLURM is present by calling sinfo cmd
        try:
            cmd = "sinfo"
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            so, se = p.communicate()
        except OSError:
            raise RuntimeError("SLURM job submission is not available on this system")

        self.qsubscript = qsubscript
        if not qsubscript:
            raise RuntimeError("SLURM job submission requires a valid qsub script")
        elif not os.path.isfile(qsubscript):
            raise RuntimeError("Qsub script does not exist: {}".format(qsubscript))

        self.qsub_args = qsub_args

    def run_tasks(self, tasks):

        for task in tasks:
            cmd = r'sbatch -J {} --export=p1="{} {}" "{}"'.format(
                task.abrv,
                task.exe,
                escape_problem_jobsubmit_chars(task.cmd),
                self.qsubscript
            )
            subprocess.call(cmd, shell=True)
            

class ParallelTaskHandler(object):

    def __init__(self, num_processes=1):
        self.num_processes = num_processes
        if mp.cpu_count() < num_processes:
            raise RuntimeError("Specified number of processes ({0}) is higher than the system cpu count ({1})".
                               format(num_processes, mp.cpu_count()))
        elif num_processes < 1:
            raise RuntimeError("Specified number of processes ({0}) must be greater than 0, using default".
                               format(num_processes))

    def run_tasks(self, tasks):

        task_queue = [[task.name, self._format_task(task)] for task in tasks]
        pool = mp.Pool(self.num_processes)
        try:
            pool.map(exec_cmd_mp, task_queue, 1)
        except KeyboardInterrupt:
            pool.terminate()
            raise RuntimeError("Processes terminated without file cleanup")

    def _format_task(self, task):
        _cmd = r'{} {}'.format(
            task.exe,
            task.cmd,
        )
        return _cmd
                

def exec_cmd_mp(job):
    job_name, cmd = job
    logger.info('Running job: %s', job_name)
    logger.debug('Cmd: %s', cmd)
    if platform.system() == "Windows":
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    else:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
    try:
        (so, se) = p.communicate()
    except KeyboardInterrupt:
        if platform.system() == "Windows":
            p.terminate()
        else:
            os.killpg(p.pid, signal.SIGTERM)

    else:
        logger.debug(so)
        logger.debug(se)


def exec_cmd(cmd):
    logger.debug(cmd)

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (so, se) = p.communicate()
    rc = p.wait()
    err = 0

    if rc != 0:
        logger.error("Error found - Return Code = %s:  %s", rc, cmd)
        err = 1
    else:
        logger.debug("Return Code = %s:  %s", rc, cmd)

    logger.debug("STDOUT:  %s", so)
    logger.debug("STDERR:  %s", se)
    return err, so, se


def argval2str(item):
    if type(item) is str:
        if (   (item.startswith("'") and item.endswith("'"))
            or (item.startswith('"') and item.endswith('"'))):
            item_str = item
        else:
            item_str = '"{}"'.format(item)
    else:
        item_str = '{}'.format(item)
    return item_str


def escape_problem_jobsubmit_chars(str_item):
    str_item = str_item.replace("'", "\\'")
    str_item = str_item.replace('"', '\\"')
    # str_item = str_item.replace(',', '@COMMA@')
    # str_item = str_item.replace(' ', '@SPACE@')
    return str_item


def convert_optional_args_to_string(args, positional_arg_keys, arg_keys_to_remove):

    args_dict = vars(args)
    arg_list = []

    ## Add optional args to arg_list
    for k, v in args_dict.items():
        if k not in positional_arg_keys and k not in arg_keys_to_remove and v is not None:
            k = k.replace('_', '-')
            if isinstance(v, (list, tuple)):
                arg_list.append("--{} {}".format(k, ' '.join([argval2str(item) for item in v])))
            elif isinstance(v, bool):
                if v is True:
                    arg_list.append("--{}".format(k))
            else:
                arg_list.append("--{} {}".format(k, argval2str(v)))

    arg_str_base = " ".join(arg_list)
    return arg_str_base
