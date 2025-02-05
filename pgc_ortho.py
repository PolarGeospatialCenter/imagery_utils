#!/usr/bin/env python

from __future__ import division

import argparse
import logging
import math
import os
import sys
import xml.etree.ElementTree as ET
import datetime

import numpy as np

from lib import ortho_functions, taskhandler, utils
from lib.taskhandler import argval2str

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ARGDEF_SCRATCH = os.path.join(os.path.expanduser('~'), 'scratch', 'task_bundles')


def main():
    ret_code = 0

    #### Set Up Arguments
    parent_parser, pos_arg_keys = ortho_functions.build_parent_argument_parser()
    parser = argparse.ArgumentParser(
        parents=[parent_parser],
        description="Run/submit batch image ortho and conversion tasks"
    )

    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--slurm", action='store_true', default=False,
                        help="submit tasks to SLURM")
    parser.add_argument("--slurm-log-dir", default=None,
                        help="directory path for logs from slurm jobs on the cluster. "
                             "Default is the parent directory of the output. "
                             "To use the current working directory, use 'working_dir'")
    parser.add_argument("--slurm-job-name", default=None,
                        help="assign a name to the slurm job for easier job tracking")
    parser.add_argument("--tasks-per-job", type=int,
                        help="Number of tasks to bundle into a single job. (requires --pbs or --slurm option) (Warning:"
                             " a higher number of tasks per job may require modification of default wallclock limit.)")
    parser.add_argument('--scratch', default=ARGDEF_SCRATCH,
                        help="Scratch space to build task bundle text files. (default={})".format(ARGDEF_SCRATCH))
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="submission script to use in PBS/SLURM submission (PBS default is qsub_ortho.sh, SLURM "
                             "default is slurm_ortho.py, in script root folder)")
    parser.add_argument("-l",
                        help="PBS resources requested (mimicks qsub syntax, PBS only)")
    parser.add_argument("--queue",
                        help="Cluster queue/partition to submit jobs to. Accepted slurm queues: batch (default "
                             "partition, no need to specify it in this arg), big_mem (for large memory jobs), "
                             "and low_priority (for background processes)")
    parser.add_argument("--log", nargs='?', const="default",
                        help="output log file -- top level log is not written without this arg. "
                             "when this flag is used, log will be written to ortho_<timestamp>.log next to the <dst dir>) "
                             "unless a specific file path is provided here")
    parser.add_argument("--dryrun", action='store_true', default=False,
                        help='print actions without executing')
    parser.add_argument("-v", "--verbose", action='store_true', default=False,
                        help='log debug messages')

    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(args.src)
    dstdir = os.path.abspath(args.dst)
    args.scratch = os.path.abspath(args.scratch)
    args.dst = dstdir

    #### Validate Required Arguments
    if os.path.isdir(src):
        srctype = 'dir'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() == '.txt':
        srctype = 'textfile'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() == '.csv':
        srctype = 'csvfile'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() in ortho_functions.exts:
        srctype = 'image'
    elif os.path.isfile(src.replace('msi', 'blu')) and os.path.splitext(src)[1].lower() in ortho_functions.exts:
        srctype = 'image'
    else:
        parser.error("Error arg1 is not a recognized file path or file type: {}".format(src))

    if not os.path.isdir(dstdir):
        parser.error("Error arg2 is not a valid file path: {}".format(dstdir))


    ## Verify qsubscript
    if args.pbs or args.slurm:
        if args.qsubscript is None:
            if args.pbs:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'qsub_ortho.sh')
            if args.slurm:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'slurm_ortho.sh')
        else:
            qsubpath = os.path.abspath(args.qsubscript)
        if not os.path.isfile(qsubpath):
            parser.error("qsub script path is not valid: {}".format(qsubpath))

    # Parse slurm log location
    if args.slurm:
        # by default, the parent directory of the dst dir is used for saving slurm logs
        if args.slurm_log_dir == None:
            slurm_log_dir = os.path.abspath(os.path.join(dstdir, os.pardir))
        # if "working_dir" is passed in the CLI, use the default slurm behavior which saves logs in working dir
        elif args.slurm_log_dir == "working_dir":
            slurm_log_dir = None
        # otherwise, verify that the path for the logs is a valid path
        else:
            slurm_log_dir = os.path.abspath(args.slurm_log_dir)
        # check that partition names are valid
        if args.queue and not args.queue in ortho_functions.slurm_partitions:
            parser.error("--queue argument '{}' is not a valid slurm partition. "
                         "Valid partitions: {}".format(args.queue,
                                                       ortho_functions.slurm_partitions))
        # Verify slurm log path
        if not os.path.isdir(slurm_log_dir):
            parser.error("Error directory for slurm logs is not a valid file path: {}".format(slurm_log_dir))

    ## Verify processing options do not conflict
    requested_threads = ortho_functions.ARGDEF_CPUS_AVAIL if args.threads == "ALL_CPUS" else args.threads
    if args.pbs and args.slurm:
        parser.error("Options --pbs and --slurm are mutually exclusive")
    if (args.pbs or args.slurm) and args.parallel_processes > 1:
        parser.error("HPC Options (--pbs or --slurm) and --parallel-processes > 1 are mutually exclusive")
    if (args.pbs or args.slurm) and requested_threads > 1:
        parser.error("HPC Options (--pbs or --slurm) and --threads > 1 are mutually exclusive")
    if requested_threads < 1:
        parser.error("--threads count must be positive, nonzero integer or ALL_CPUS")
    if args.parallel_processes > 1:
        total_proc_count = requested_threads * args.parallel_processes
        if total_proc_count > ortho_functions.ARGDEF_CPUS_AVAIL:
            parser.error("the (threads * number of processes requested) ({0}) exceeds number of available threads "
                         "({1}); reduce --threads and/or --parallel-processes count"
                         .format(total_proc_count, ortho_functions.ARGDEF_CPUS_AVAIL))

    if args.tasks_per_job:
        if not (args.pbs or args.slurm):
            parser.error("--tasks-per-job option requires the (--pbs or --slurm) option")
        if not os.path.isdir(args.scratch):
            print("Creating --scratch directory: {}".format(args.scratch))
            os.makedirs(args.scratch)

    #### Verify EPSG
    spatial_ref = None
    if srctype == 'csvfile' and args.epsg is None:
        # Check for valid EPSG argument in CSV argument list file
        pass
    elif args.epsg is None:
        parser.error("--epsg argument is required")
    elif args.epsg in ('utm', 'auto'):
        # EPSG code is automatically determined in ortho_functions.get_image_stats function
        pass
    else:
        try:
            args.epsg = int(args.epsg)
        except ValueError:
            parser.error("--epsg must be 'utm', 'auto', or an integer EPSG code")
        try:
            spatial_ref = utils.SpatialRef(args.epsg)
        except RuntimeError as e:
            parser.error(e)

    #### Verify that dem and ortho_height are not both specified
    if args.dem is not None and args.ortho_height is not None:
        parser.error("--dem and --ortho_height options are mutually exclusive.  Please choose only one.")

    #### Test if DEM exists
    if not args.dem == "auto":
        if args.dem is not None and not os.path.isfile(args.dem):
            parser.error("DEM does not exist: {}".format(args.dem))

    #### Set up console logging handler
    if args.verbose:
        lso_log_level = logging.DEBUG
    else:
        lso_log_level = logging.INFO
    lso = logging.StreamHandler()
    lso.setLevel(lso_log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    #### Configure file handler if --log is passed to CLI
    if args.log is not None:
        if args.log == "default":
            log_fn = "ortho_{}.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            logfile = os.path.join(os.path.abspath(os.path.join(args.dst, os.pardir)), log_fn)
        else:
            logfile = os.path.abspath(args.log)
            if not os.path.isdir(os.path.pardir(logfile)):
                parser.warning("Output location for log file does not exist: {}".format(os.path.isdir(os.path.pardir(logfile))))

        lfh = logging.FileHandler(logfile)
        lfh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
        lfh.setFormatter(formatter)
        logger.addHandler(lfh)

    # log input command for reference
    command_str = ' '.join(sys.argv)
    logger.info("Running command: {}".format(command_str))

    if args.slurm:
        logger.info("Slurm output and error log saved here: {}".format(slurm_log_dir))

    #### Handle thread count that exceeds system limits
    if requested_threads > ortho_functions.ARGDEF_CPUS_AVAIL:
        logger.info("threads requested ({0}) exceeds number available on system ({1}), setting thread count to "
                    "'ALL_CPUS'".format(requested_threads, ortho_functions.ARGDEF_CPUS_AVAIL))
        args.threads = 'ALL_CPUS'

    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('l', 'queue', 'qsubscript', 'dryrun', 'pbs', 'slurm', 'parallel_processes', 'tasks_per_job')

    ## Identify source images
    csv_arg_data = None
    csv_header_argname_list = None
    csv_src_array = None
    if srctype == 'dir':
        image_list1 = utils.find_images(src, False, ortho_functions.exts)
    elif srctype == 'textfile':
        image_list1 = utils.find_images(src, True, ortho_functions.exts)
    elif srctype == 'csvfile':
        # Load CSV data
        csv_arg_data = np.char.strip(np.loadtxt(src, dtype=str, delimiter=',', encoding="utf-8-sig"), '\'"')
        csv_header = csv_arg_data[0, :]

        # Adjust CSV header argument names
        # Support ArcGIS export of typical fp.py output
        if 'OID_' in csv_header:
            csv_arg_data = np.delete(csv_arg_data, np.where(csv_header == 'OID_')[0], axis=1)
            csv_header = csv_arg_data[0, :]
        if 'O_FILEPATH' in csv_header:
            csv_header[csv_header == 'O_FILEPATH'] = 'src'
        if 'o_filepath' in csv_header:
            csv_header[csv_header == 'o_filepath'] = 'src'

        # Convert CSV header argument names to argparse namespace variable format
        csv_header_argname_list = [argname.lstrip('-').replace('-', '_').lower() for argname in csv_header]

        # Remove header row
        csv_arg_data = np.delete(csv_arg_data, 0, axis=0)

        # Verify CSV arguments and values
        if len(csv_header_argname_list) >= 1 and 'src' in csv_header_argname_list:
            pass
        else:
            parser.error("'src' should be the header of the first column of source CSV argument list file")
        if 'epsg' in csv_header_argname_list:
            csv_epsg_array = csv_arg_data[:, csv_header_argname_list.index('epsg')].astype(int)
            invalid_epsg_code = False
            for epsg_code in np.unique(csv_epsg_array):
                try:
                    utils.SpatialRef(epsg_code)
                except Exception:
                    logger.error(utils.capture_error_trace())
                    invalid_epsg_code = True
            if invalid_epsg_code:
                parser.error("Source CSV argument list file contains invalid EPSG code(s)")
        elif args.epsg is None:
            parser.error("A valid EPSG argument must be specified")

        # Create subsets of VRT DEM and trim CSV data if applicable
        if args.dem is not None and args.dem.endswith('.vrt') and 'dem' in csv_header_argname_list:
            csv_arg_data = utils.subset_vrt_dem(csv_arg_data, csv_header_argname_list, args)

        # Extract src image paths and send to utils.find_images
        csv_src_array = csv_arg_data[:, csv_header_argname_list.index('src')]
        csv_src_list = csv_src_array.tolist()
        if len(csv_src_list) != len(set(csv_src_list)):
            parser.error("Detected duplicate 'src' items in CSV argument list file."
                         " If meaning to use the subset-VRT-DEM functionality, you must"
                         " provide the path to the full tileset VRT DEM file (.vrt extension)"
                         " with the -d argument.")
        image_list1 = utils.find_images(csv_src_array.tolist(), True, ortho_functions.exts)

        # Trim CSV data to intersection with found image paths
        _, _, csv_rows_src_found = np.intersect1d(np.asarray(image_list1), csv_src_array, return_indices=True)
        csv_arg_data = csv_arg_data[csv_rows_src_found, :]
        csv_src_array = csv_arg_data[:, csv_header_argname_list.index('src')]
        assert set(csv_src_array) == set(image_list1)
    else:
        image_list1 = [src]

    ## Group Ikonos
    image_list2 = []
    for i, srcfp in enumerate(image_list1):
        srcdir, srcfn = os.path.split(srcfp)
        if "IK01" in srcfn and sum([b in srcfn for b in ortho_functions.ikMsiBands]) > 0:
            for b in ortho_functions.ikMsiBands:
                if b in srcfn:
                    newname = os.path.join(srcdir, srcfn.replace(b, "msi"))
                    break
            image_list2.append(newname)
            if srctype == 'csvfile':
                # The csv_src_array is a slice/window into the larger CSV data array;
                # modifications are carried through to the larger CSV data array.
                csv_src_array[i] = newname

        else:
            image_list2.append(srcfp)

    image_list = list(set(image_list2))
    logger.info('Number of src images: %i', len(image_list))
    if len(image_list) == 0:
        logger.info("No images found to process")
        sys.exit(0)

    if srctype == 'csvfile':
        # Trim CSV data to intersection with updated image path names
        # (the number of source images should not have changed, so this
        #  is mainly a check that changes to any image names were also
        #  properly applied to the CSV data array).
        _, _, csv_rows_to_keep = np.intersect1d(np.asarray(image_list), csv_src_array, return_indices=True)
        csv_arg_data = csv_arg_data[csv_rows_to_keep, :]
        csv_src_array = csv_arg_data[:, csv_header_argname_list.index('src')]
        assert set(csv_src_array) == set(image_list)
        # Use the CSV argument array in place of the standard image list
        image_list = csv_arg_data

    ## Build task queue
    images_to_process = []
    image_info_dict = {}
    for task_args in utils.yield_task_args(image_list, args,
                                           argname_1D='src',
                                           argname_2D_list=csv_header_argname_list):
        srcfp = task_args.src
        dstdir = task_args.dst
        lso.setLevel(logging.WARNING)  # temporarily reduce logging level to limit excess terminal text
        try:
            info = ortho_functions.ImageInfo(srcfp, dstdir, args.wd, args)
        except Exception as e:
            logger.error(e)
        else:
            lso.setLevel(lso_log_level)
            dstfp = info.dstfp
            vrtfile1 = os.path.splitext(dstfp)[0] + "_raw.vrt"
            vrtfile2 = os.path.splitext(dstfp)[0] + "_vrt.vrt"

            # Check to see if raw.vrt or vrt.vrt are present
            vrt_exists = os.path.isfile(vrtfile1) or os.path.isfile(vrtfile2)
            tif_done = os.path.isfile(dstfp)
            # If no tif file present, need to make one
            # If tif file is present but one of the vrt files is present, need to rebuild
            if (not tif_done) or vrt_exists:
                images_to_process.append(srcfp)
                image_info_dict[srcfp] = info

    logger.info("Number of incomplete tasks: %i", len(images_to_process))
    if len(images_to_process) == 0:
        logger.info("No incomplete tasks to process")
        sys.exit(0)

    task_queue = []

    if srctype == 'csvfile':
        # Trim CSV data to intersection with images yet to process
        _, _, csv_rows_to_process = np.intersect1d(np.asarray(images_to_process), csv_src_array, return_indices=True)
        csv_arg_data = csv_arg_data[csv_rows_to_process, :]
        csv_src_array = csv_arg_data[:, csv_header_argname_list.index('src')]
        assert set(csv_src_array) == set(images_to_process)
        # Use the CSV argument array in place of the standard image list
        images_to_process = csv_arg_data

    ## Bundle tasks into sets by the number of tasks-per-job
    if args.tasks_per_job and args.tasks_per_job > 1:
        task_srcfp_list = utils.write_task_bundles(
            images_to_process, args.tasks_per_job, args.scratch, 'Or_src',
            header_list=csv_header_argname_list, bundle_ext=('csv' if srctype == 'csvfile' else 'txt')
        )
    else:
        task_srcfp_list = images_to_process

    ## Build task objects
    for job_count, task_args in enumerate(
            utils.yield_task_args(task_srcfp_list, args,
                                  argname_1D='src',
                                  argname_2D_list=csv_header_argname_list),
            1):
        arg_str_base = taskhandler.convert_optional_args_to_string(task_args, pos_arg_keys, arg_keys_to_remove)
        srcfp = task_args.src
        dstdir = task_args.dst
        srcdir, srcfn = os.path.split(srcfp)

        ## If task_srcfp_list = images_to_process, then the image_info_dict is also populated
        if task_srcfp_list is images_to_process:
            info = image_info_dict[srcfp]
            dstfp = info.dstfp
        else:  # this case occurs when there is a textfile or csv to resubmit so dstfp is not needed
            dstfp = None

        # add a custom name to the job
        if not args.slurm_job_name:
            job_name = 'Or{:04g}'.format(job_count)
        else:
            job_name = str(args.slurm_job_name)

        task = taskhandler.Task(
            srcfn,
            job_name,
            'python',
            '{} {} {} {}'.format(
                argval2str(scriptpath),
                arg_str_base,
                argval2str(srcfp),
                argval2str(dstdir)
            ),
            ortho_functions.process_image,
            [srcfp, dstfp, task_args]
        )
        task_queue.append(task)

    ## Run tasks
    if len(task_queue) > 0:
        logger.info("Submitting Tasks")
        if args.pbs:
            qsub_args = ""
            if args.l:
                qsub_args += " -l {}".format(args.l)
            if args.queue:
                qsub_args += " -q {}".format(args.queue)
            try:
                task_handler = taskhandler.PBSTaskHandler(qsubpath, qsub_args)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue, dryrun=args.dryrun)

        elif args.slurm:
            qsub_args = ""
            if not slurm_log_dir == None:
                qsub_args += '-o {}/%x.o%j '.format(slurm_log_dir)
                qsub_args += '-e {}/%x.o%j '.format(slurm_log_dir)
            # adjust wallclock if submitting multiple tasks ro be run in serial for a single slurm job
            # default wallclock for ortho jobs is 1:00:00, refer to slurm_ortho.sh to verify
            if args.tasks_per_job:
                qsub_args += '-t {}:00:00 '.format(args.tasks_per_job)
            if args.queue:
                qsub_args += "-p {} ".format(args.queue)
            try:
                task_handler = taskhandler.SLURMTaskHandler(qsubpath, qsub_args)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)

        elif args.parallel_processes > 1:
            try:
                task_handler = taskhandler.ParallelTaskHandler(args.parallel_processes)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error(e)
            else:
                logger.info("Number of child processes to spawn: %i", task_handler.num_processes)
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)

        else:

            results = {}
            for task in task_queue:

                src, dstfp, task_arg_obj = task.method_arg_list

                #### Set up processing log handler
                logfile = os.path.splitext(dstfp)[0] + ".log"
                lfh = logging.FileHandler(logfile)
                lfh.setLevel(logging.DEBUG)
                formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
                lfh.setFormatter(formatter)
                logger.addHandler(lfh)

                if not args.dryrun:
                    results[task.name] = task.method(src, dstfp, task_arg_obj)

                #### remove existing file handler
                logger.removeHandler(lfh)

            #### Print Images with Errors
            for k, v in results.items():
                if v != 0:
                    logger.warning("Failed Image: %s", k)
                    ret_code = 1

        logger.info("Done")

    else:
        logger.info("No images found to process")

    sys.exit(ret_code)


if __name__ == "__main__":
    main()
