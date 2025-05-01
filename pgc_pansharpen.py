#!/usr/bin/env python

from __future__ import division

import argparse
import copy
import glob
import logging
import math
import os
import platform
import re
import shutil
import sys
import xml.etree.ElementTree as ET
import datetime

from osgeo import gdal, gdalconst, ogr, osr

from lib import ortho_functions, taskhandler, utils
from lib.taskhandler import argval2str

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

ARGDEF_SCRATCH = os.path.join(os.path.expanduser('~'), 'scratch', 'task_bundles')

#TODO: build image pair class
# compare mul and pan extent to find smaller extent and pass in to ortho_functions.process() as a parameter

#### Reg Exs

# WV02_12FEB061315046-P1BS-10300100106FC100.ntf
WV02p = re.compile(r"WV02_\w+-M")

# WV03_12FEB061315046-P1BS-10300100106FC100.ntf
WV03p = re.compile(r"WV03_\w+-M")

# QB02_12FEB061315046-P1BS-10300100106FC100.ntf
QB02p = re.compile(r"QB02_\w+-M")

# GE01_12FEB061315046-P1BS-10300100106FC100.ntf
GE01p_dg = re.compile(r"GE01_\w+-M")

# GE01_111211P0011184144A222000100082M_000754776.ntf
GE01p = re.compile(r"GE01_\w+M0")

# IK01_2009121113234710000011610960_pan_6516S.ntf
IK01p = re.compile(r"IK01_\w+(blu|msi|bgrn)")

dRegExs = {
    WV02p:("WV02"),
    GE01p_dg:("GE01"),
    WV03p:("WV03"),
    QB02p:("QB02"),
    GE01p:("GE01"),
    IK01p:("IK01")
}

class ImagePair(object):
    
    def __init__(self, mul_srcfp, spatial_ref, args):
        self.mul_srcfp = mul_srcfp
        self.srcdir, self.mul_srcfn = os.path.split(mul_srcfp)
        
        ####  Identify name pattern
        self.sensor = None
        for regex in dRegExs:
            match = regex.match(self.mul_srcfn)
            if match is not None:
                self.sensor = dRegExs[regex]
                break
        if self.sensor:
            
            self.pan_srcfn = self._get_panchromatic_name()
            self.pan_srcfp = os.path.join(self.srcdir, self.pan_srcfn)
            if not os.path.isfile(self.pan_srcfp):
                logging.info("checking for date difference between pan and mul scenes")
                try:
                    self.pan_srcfn, self.pan_srcfp = self._check_datetime_dif()
                except:
                    raise RuntimeError("Corresponding panchromatic image not found: {}".format(self.mul_srcfp))
            else:
            ## get extent info for both images and calc intersect
                mul_extent = self._get_image_info(self.mul_srcfp, spatial_ref, args)
                pan_extent = self._get_image_info(self.pan_srcfp, spatial_ref, args)
                self.intersection_geom = mul_extent.Intersection(pan_extent)
                # print(mul_extent)
                # print(mul_extent.Contains(pan_extent))
                # print(pan_extent)
                # print(pan_extent.Contains(mul_extent))
                # print(self.intersection_geom)
                
        else:
            raise RuntimeError("Image does not match multispectral name pattern: {}".format(self.mul_srcfn))
                
    def _get_panchromatic_name(self):
    
        ####  check for pan version
        if self.sensor in ["WV02", "WV03", "QB02"]:
            pan_name = self.mul_srcfn.replace("-M", "-P")
        elif self.sensor == "GE01":
            if "_5V" in self.mul_srcfn:
                
                pan_name_base = self.mul_srcfp[:-24].replace("M0", "P0")
                candidates = glob.glob(pan_name_base + "*")
                candidates2 = [f for f in candidates if f.endswith(('.ntf', '.NTF', '.tif', '.TIF'))]
                if len(candidates2) == 0:
                    pan_name = ''
                elif len(candidates2) == 1:
                    pan_name = os.path.basename(candidates2[0])
                else: #raise error for now. TODO: iterate through candidates for greatest overlap
                    pan_name = ''
                    logger.error('%i panchromatic images match the multispectral image name %s', len(candidates2),
                                 self.mul_srcfn)
            else:
                pan_name = self.mul_srcfn.replace("-M", "-P")
        elif self.sensor == "IK01":
            pan_name = re.sub("blu|msi|bgrn", "pan", self.mul_srcfn)

        return pan_name
    
    def _get_image_info(self, src_image, spatial_ref, args):

        if self.sensor == 'IK01' and "_msi_" in src_image and not os.path.isfile(src_image):
            src_image_name = os.path.basename(src_image).replace("_msi_", "_blu_")
            src_image = os.path.join(self.srcdir, src_image_name)

        return ortho_functions.get_image_geometry_info(src_image, spatial_ref, args,
                                                       return_type='extent_geom')

    def _check_datetime_dif(self):
        # parse date from pan_srcfn (a copy of mul_srcfp with 'M' replaced with 'P')
        # Assumes 5 character prefix to date (sensor code and _)
        # WV03 ref: WV03_20150803153108_104001000F657400_15AUG03153108-P1BS-500445078060_01_P009
        # date str: YYYYMMDDHHMMSS
        # strptime: %Y%m%d%H%M%S
        # strptime v2 (for second date str in DG filenames): %y%b%d%H%M%S .upper()

        # parse date from mul str to datetime object and str format to lookup in fn
        mul_date_parse = datetime.datetime.strptime(self.pan_srcfn[5:19], '%Y%m%d%H%M%S')
        mul_date_form_1 = datetime.datetime.strftime(mul_date_parse, '%Y%m%d%H%M%S')

        # loop through 1 second prior to and 1 second after mul time stamp (Pan is usually 1 sec prior if there is dif)
        for time_dif in [-1, 1]:
            # add 1 second time difference to datetime obj and format it to str
            mul_date_parse_dif_1 = mul_date_parse + datetime.timedelta(seconds=time_dif)
            mul_date_form_dif_1 = datetime.datetime.strftime(mul_date_parse_dif_1, '%Y%m%d%H%M%S')

            # get format for 2nd date str in fn for original time and dif time
            mul_date_2 = datetime.datetime.strftime(mul_date_parse, '%y%b%d%H%M%S').upper()
            mul_date_2_dif_1 = datetime.datetime.strftime(mul_date_parse_dif_1, '%y%b%d%H%M%S').upper()

            # construct filename with updated time stamp
            # some scenes do not have the second time stamp. the second .replace() will have no effect
            pan_name_dif_1 = self.pan_srcfn.replace(mul_date_form_1, mul_date_form_dif_1).replace(mul_date_2, mul_date_2_dif_1)

            # check if filename with dif time stamp exists, if so, return for ImagePair class
            if os.path.isfile(os.path.join(self.srcdir, pan_name_dif_1)):
                pan_fn_w_diff = pan_name_dif_1
                pan_fp_w_diff = os.path.join(self.srcdir, pan_fn_w_diff)
                return pan_fn_w_diff, pan_fp_w_diff

        raise Exception("Cannot find pan scene with 1 sec datetime diff")


def main():

    #### Set Up Arguments
    parent_parser, pos_arg_keys = ortho_functions.build_parent_argument_parser()
    parser = argparse.ArgumentParser(
        parents=[parent_parser],
        description="Run/Submit batch pansharpening in parallel"
    )

    parser.add_argument("--skip-missing-pairs", action='store_true', default=False,
                        help="submit available pan/multi image pairs for pansharpening,"
                             " skipping over cases of image pairs missing a pan or multi image")
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
                        help="submission script to use in PBS/SLURM submission (PBS default is qsub_pansharpen.sh, "
                             "SLURM default is slurm_pansharpen.py, in script root folder)")
    parser.add_argument("-l",
                        help="PBS resources requested (mimicks qsub syntax, PBS only)")
    parser.add_argument("--queue",
                        help="Cluster queue/partition to submit jobs to. Accepted slurm queues: batch (default "
                             "partition, no need to specify it in this arg), big_mem (for large memory jobs), "
                             "and low_priority (for background processes)")
    parser.add_argument("--log", nargs='?', const="default",
                        help="output log file -- top level log is not written without this arg. "
                             "when this flag is used, log will be written to pansharpen_<timestamp>.log next to the <dst dir>) "
                             "unless a specific file path is provided here")
    parser.add_argument("--dryrun", action="store_true", default=False,
                        help="print actions without executing")

    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(args.src)
    dstdir = os.path.abspath(args.dst)
    scratch = os.path.abspath(args.scratch)
    bittype = utils.get_bit_depth(args.outtype)

    #### Validate Required Arguments
    if os.path.isdir(src):
        srctype = 'dir'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() == '.txt':
        srctype = 'textfile'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() in ortho_functions.exts:
        srctype = 'image'
    elif os.path.isfile(src.replace('msi', 'blu')) and os.path.splitext(src)[1].lower() in ortho_functions.exts:
        srctype = 'image'
    else:
        parser.error("Error arg1 is not a recognized file path or file type: {}".format(src))

    if not os.path.isdir(dstdir):
        parser.error("Error arg2 is not a valid file path: {}".format(dstdir))

    # Verify qsubscript
    if args.pbs or args.slurm:
        if args.qsubscript is None:
            if args.pbs:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'qsub_pansharpen.sh')
            if args.slurm:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'slurm_pansharpen.sh')
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
    if args.epsg is None:
        parser.error("--epsg argument is required")
    elif args.epsg in ('utm', 'auto'):
        # EPSG code is automatically determined in ortho_functions.get_image_stats
        # and ortho_functions.get_image_geometry_info functions.
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
    if not args.dem == 'auto':
        if args.dem is not None and not os.path.isfile(args.dem):
            parser.error("DEM does not exist: {}".format(args.dem))

    ## Check the correct number of values are supplied for --resolution
    if args.resolution and len(args.resolution) > 2:
        parser.error("--resolution option requires one or two values")
        
    ## Check GDAL version (2.1.0 minimum)
    gdal_version = gdal.VersionInfo()
    try:
        if int(gdal_version) < 2010000:
            parser.error("gdal_pansharpen requires GDAL version 2.1.0 or higher")
    except ValueError:
        parser.error("Cannot parse GDAL version: {}".format(gdal_version))

    #### Set up console logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    #### Configure file handler if --log is passed to CLI
    if args.log is not None:
        if args.log == "default":
            log_fn = "pansharpen_{}.log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
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

    #### Handle thread count that exceeds system limits
    if requested_threads > ortho_functions.ARGDEF_CPUS_AVAIL:
        logger.info("threads requested ({0}) exceeds number available on system ({1}), setting thread count to "
                    "'ALL_CPUS'".format(requested_threads, ortho_functions.ARGDEF_CPUS_AVAIL))
        args.threads = 'ALL_CPUS'

    if args.slurm:
        logger.info("Slurm output and error log saved here: {}".format(slurm_log_dir))

    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('l', 'queue', 'qsubscript', 'dryrun', 'pbs', 'slurm', 'parallel_processes', 'tasks_per_job')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    ## Identify source images
    if srctype == 'dir':
        image_list1 = utils.find_images(src, False, ortho_functions.exts)
    elif srctype == 'textfile':
        image_list1 = utils.find_images(src, True, ortho_functions.exts)
    else:
        image_list1 = [src]

    logger.info("Pairing src panchromatic and multispectral images")

    pair_list = []
    unmatched_images = set()
    for srcfp in image_list1:
        #print(srcfp)
        try:
            image_pair = ImagePair(srcfp, spatial_ref, args)
        except RuntimeError as e:
            if (   str(e).startswith("Corresponding panchromatic image not found:")
                or str(e).startswith("Image does not match multispectral name pattern:")):
                if str(e).startswith("Corresponding panchromatic image not found:"):
                    logger.error(str(e))
                _, _, non_multi_fn = str(e).partition(':')
                unmatched_images.add(os.path.basename(non_multi_fn.strip()))
            else:
                logger.error(e)
        else:
            # logger.info("Image: %s, Sensor: %s", image_pair.mul_srcfn, image_pair.sensor)
            pair_list.append(image_pair)

    pair_pan_images = set([pair.pan_srcfn for pair in pair_list])
    unmatched_images = unmatched_images.difference(pair_pan_images)
    if len(unmatched_images) > 0:
        unmatched_images_wv01 = set(fn for fn in unmatched_images if fn.startswith('WV01'))
        unmatched_images_not_wv01 = unmatched_images.difference(unmatched_images_wv01)
        if len(unmatched_images_wv01) > 0:
            logger.warning("{} src WV01 images could not be paired, as WV01 images are pan-only:\n{}".format(
                len(unmatched_images_wv01), '\n'.join(sorted(list(unmatched_images_wv01)))
            ))
        if len(unmatched_images_not_wv01) > 0:
            logger.error("{} src non-WV01 images could not be paired:\n{}".format(
                len(unmatched_images_not_wv01), '\n'.join(sorted(list(unmatched_images_not_wv01)))
            ))
            if not args.skip_missing_pairs:
                logger.info("Exiting program without submitting processing tasks due to missing pairs")
                logger.info("Provide the --skip-missing-pairs to bypass this error and process available pairs")
                sys.exit(1)
                
    logger.info("Number of src image pairs: %i", len(pair_list))
    if len(pair_list) == 0:
        logger.info("No images pairs found to process")
        sys.exit(0)
    
    ## Build task queue
    i = 0
    pairs_to_process = []
    for image_pair in pair_list:
        # args.epsg has been converted to an int type if possible already,
        # Look up the correct epsg if it's still a string
        if type(args.epsg) is str:
            img_epsg = ortho_functions.get_image_geometry_info(image_pair.mul_srcfp, spatial_ref, args,
                                                               return_type='epsg_code')
            ## If image cannot be opened, skip it
            if img_epsg is None:
                continue
        else:
            img_epsg = args.epsg
        
        pansh_dstfp = os.path.join(dstdir, "{}_{}{}{}_pansh{}".format(
            os.path.splitext(image_pair.mul_srcfn)[0],
            bittype,
            args.stretch,
            img_epsg,
            ortho_functions.formats[args.format]
        ))

        done = os.path.isfile(pansh_dstfp)
        if done is False:
            i += 1
            pairs_to_process.append(image_pair)
            
    logger.info('Number of incomplete tasks: %i', i)

    if len(pairs_to_process) == 0:
        logger.info("No incomplete tasks to process")
        sys.exit(0)

    task_queue = []

    if args.tasks_per_job and args.tasks_per_job > 1:
        images_to_process = [image_pair.mul_srcfp for image_pair in pairs_to_process]
        task_srcfp_list = utils.write_task_bundles(images_to_process, args.tasks_per_job, scratch, 'Psh_src')
        tasklist_is_text_bundles = True
    else:
        task_srcfp_list = pairs_to_process
        tasklist_is_text_bundles = False

    # Make global variable for resolution if passed in on command line
    if args.resolution:
        orig_res = copy.deepcopy(args.resolution)
    else:
        orig_res = None

    for job_count, task_item in enumerate(task_srcfp_list, 1):

        if not tasklist_is_text_bundles:
            image_pair = task_item
            # args.epsg has been converted to an int type if possible already,
            # Look up the correct epsg if it's still a string
            if type(args.epsg) is str:
                img_epsg = ortho_functions.get_image_geometry_info(image_pair.mul_srcfp, spatial_ref, args,
                                                                   return_type='epsg_code')
                ## If image cannot be opened, skip it
                if img_epsg is None:
                    continue
            else:
                img_epsg = args.epsg
            pansh_dstfp = os.path.join(dstdir, "{}_{}{}{}_pansh.tif".format(
                os.path.splitext(image_pair.mul_srcfn)[0],
                bittype,
                args.stretch,
                img_epsg
            ))
            task_item_srcfp = image_pair.mul_srcfp
            task_item_srcfn = image_pair.mul_srcfn
        else:
            image_pair = None
            pansh_dstfp = None
            task_item_srcfp = task_item
            task_item_srcdir, task_item_srcfn = os.path.split(task_item_srcfp)

        # add a custom name to the job
        if not args.slurm_job_name:
            job_name = 'Psh{:04g}'.format(job_count)
        else:
            job_name = str(args.slurm_job_name)

        task = taskhandler.Task(
            task_item_srcfn,
            job_name,
            'python',
            '{} {} {} {}'.format(
                argval2str(scriptpath),
                arg_str_base,
                argval2str(task_item_srcfp),
                argval2str(dstdir)
            ),
            exec_pansharpen,
            [image_pair, pansh_dstfp, args, orig_res]
        )
        task_queue.append(task)

    ## Run tasks
    if len(task_queue) > 0:
        logger.info("Submitting %s processing jobs", len(task_queue))
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
            # default wallclock for pansharpen jobs is 1:00:00, refer to slurm_pansh.sh to verify
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
            lfh = None
            for task in task_queue:
                           
                src, dstfp, task_arg_obj, orig_res = task.method_arg_list
                
                #### Set up processing log handler
                logfile = os.path.splitext(dstfp)[0] + ".log"
                lfh = logging.FileHandler(logfile)
                lfh.setLevel(logging.DEBUG)
                formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
                lfh.setFormatter(formatter)
                logger.addHandler(lfh)
                
                if not args.dryrun:
                    results[task.name] = task.method(src, dstfp, task_arg_obj, orig_res)
                    
                #### remove existing file handler
                logger.removeHandler(lfh)
            
                #### remove existing file handler
                logger.removeHandler(lfh)
                
            #### Print Images with Errors    
            for k, v in results.items():
                if v != 0:
                    logger.warning("Failed Image: %s", k)
        
        logger.info("Done")
        
    else:
        logger.info("No images found to process")


def exec_pansharpen(image_pair, pansh_dstfp, args, orig_res):

    dstdir = os.path.dirname(pansh_dstfp)

    #### Get working dir
    if args.wd is not None:
        wd = args.wd
    else:
        wd = dstdir
    if not os.path.isdir(wd):
        try:
            os.makedirs(wd)
        except OSError:
            pass
    logger.info("Working Dir: %s", wd)

    ####  Identify name pattern
    print("Multispectral image: {}".format(image_pair.mul_srcfp))
    print("Panchromatic image: {}".format(image_pair.pan_srcfp))

    if args.dem is not None:
        dem_arg = '-d "{}" '.format(args.dem)
    else:
        dem_arg = ""

    # args.epsg has been converted to an int type if possible already,
    # Look up the correct epsg if it's still a string
    if type(args.epsg) is str:
        img_epsg = ortho_functions.get_image_geometry_info(image_pair.mul_srcfp, None, args,
                                                           return_type='epsg_code')
    else:
        img_epsg = args.epsg

    bittype = utils.get_bit_depth(args.outtype)
    out_ext = ortho_functions.formats[args.format]
    pan_basename = os.path.splitext(image_pair.pan_srcfn)[0]
    mul_basename = os.path.splitext(image_pair.mul_srcfn)[0]
    pan_local_dstfp = os.path.join(wd, "{}_{}{}{}{}".format(pan_basename, bittype, args.stretch, img_epsg, out_ext))
    mul_local_dstfp = os.path.join(wd, "{}_{}{}{}{}".format(mul_basename, bittype, args.stretch, img_epsg, out_ext))
    pan_dstfp = os.path.join(dstdir, "{}_{}{}{}{}".format(pan_basename, bittype, args.stretch, img_epsg, out_ext))
    mul_dstfp = os.path.join(dstdir, "{}_{}{}{}{}".format(mul_basename, bittype, args.stretch, img_epsg, out_ext))
    pansh_tempfp = os.path.join(wd, "{}_{}{}{}_pansh_temp{}".format(mul_basename, bittype, args.stretch, img_epsg, out_ext))
    pansh_local_dstfp = os.path.join(wd, "{}_{}{}{}_pansh{}".format(mul_basename, bittype, args.stretch, img_epsg, out_ext))
    pansh_xmlfp = os.path.join(dstdir, "{}_{}{}{}_pansh.xml".format(mul_basename, bittype, args.stretch, img_epsg))
    mul_xmlfp = os.path.join(dstdir, "{}_{}{}{}.xml".format(mul_basename, bittype, args.stretch, img_epsg))
    
    if not os.path.isdir(wd):
        os.makedirs(wd)

    logger.info("-----------------------------------")

    ####  Ortho pan
    logger.info("Orthorectifying panchromatic image")
    if not os.path.isfile(pan_dstfp) and not os.path.isfile(pan_local_dstfp):
        ortho_functions.process_image(image_pair.pan_srcfp, pan_dstfp, args, image_pair.intersection_geom)

    if not os.path.isfile(pan_local_dstfp) and os.path.isfile(pan_dstfp):
        shutil.copy2(pan_dstfp, pan_local_dstfp)

    logger.info("Orthorectifying multispectral image")
    ####  Ortho multi
    if not os.path.isfile(mul_dstfp) and not os.path.isfile(mul_local_dstfp):
        ## If resolution is specified in the command line, assume it's intended for the pansharpened image
        ##    and multiply the multi by 4
        ##    Use the orig_res variable so that multiple passes over the args.resolution does not blow up recursively
        if args.resolution and orig_res is not None:
            args.resolution = [res * 4.0 for res in orig_res]
        ortho_functions.process_image(image_pair.mul_srcfp, mul_dstfp, args, image_pair.intersection_geom)
        # Reset resolution to CLI input
        args.resolution = orig_res
        logger.info("Resetting args.resolution: {}".format(args.resolution))
        logger.info("orig_res: {}".format(orig_res))

    if not os.path.isfile(mul_local_dstfp) and os.path.isfile(mul_dstfp):
        shutil.copy2(mul_dstfp, mul_local_dstfp)

    ####  Pansharpen
    ## get system info for program extension
    if platform.system() == 'Windows':
        py_ext = '.py'
        conda_prefix = "python %CONDA_PREFIX%\\scripts\\"
    else:
        py_ext = '.py'
        conda_prefix = ''

    pan_threading = ''
    if hasattr(args, 'threads'):
        if args.threads != 1:
            pan_threading = '-threads {}'.format(args.threads)

    if args.format == 'GTiff':
        if args.gtiff_compression == 'lzw':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" '
        elif args.gtiff_compression == 'jpeg95':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "compress=jpeg" -co "jpeg_quality=95" -co ' \
                 '"BIGTIFF=YES" '

    elif args.format == 'HFA':
        co = '-co "COMPRESSED=YES" -co "STATISTICS=YES" '

    elif args.format == 'JP2OpenJPEG':   #### add rgb constraint if openjpeg (3 bands only, also test if 16 bit possible)?
        co = '-co "QUALITY=25" '

    elif args.format == 'JPEG':
        co = ''

    else:
        co = ''
    
    logger.info("Pansharpening multispectral image")
    if os.path.isfile(pan_local_dstfp) and os.path.isfile(mul_local_dstfp):
        if not os.path.isfile(pansh_local_dstfp):
            cmd = '{}gdal_pansharpen{} -of {} {} {} "{}" "{}" "{}"'.\
                format(conda_prefix, py_ext, args.format, pan_threading, co, pan_local_dstfp, mul_local_dstfp, pansh_local_dstfp)
            try:
                taskhandler.exec_cmd(cmd)
            except Exception as e:
                logger.warning("There was an error running gdal_pansharpen.py: {}".format(e))
                logger.warning("Please run this script in the recommended mamba/conda environment with GDAL => 3.7.2")
                logger.error(utils.capture_error_trace())
    else:
        logger.warning("Pan or Multi warped image does not exist\n\t{}\n\t{}".format(pan_local_dstfp, mul_local_dstfp))

    #### Make pyramids
    if (not args.no_pyramids) and os.path.isfile(pansh_local_dstfp):
        cmd = 'gdaladdo -r {} "{}" 2 4 8 16'.format(args.pyramid_type, pansh_local_dstfp)
        taskhandler.exec_cmd(cmd)
       
    ## Copy warped multispectral xml to pansharpened output
    shutil.copy2(mul_xmlfp, pansh_xmlfp)

    #### Copy pansharpened output
    if wd != dstdir:
        for local_path, dst_path in [(pansh_local_dstfp, pansh_dstfp), (pan_local_dstfp, pan_dstfp),
                                     (mul_local_dstfp, mul_dstfp)]:
            if os.path.isfile(local_path) and not os.path.isfile(dst_path):
                shutil.copy2(local_path, dst_path)

    #### Delete Temp Files
    wd_files = [
        pansh_local_dstfp,
        pan_local_dstfp,
        mul_local_dstfp
    ]

    if not args.save_temps:
        if wd != dstdir:
            for f in wd_files:
                try:
                    os.remove(f)
                except Exception as e:
                    logger.error(utils.capture_error_trace())
                    logger.warning('Could not remove %s: %s', os.path.basename(f), e)
    
    if os.path.isfile(pansh_dstfp):
        return 0
    else:
        return 0
        

if __name__ == '__main__':
    main()
