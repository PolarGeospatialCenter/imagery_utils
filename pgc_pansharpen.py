#!/usr/bin/env python

from __future__ import division

import argparse
import glob
import logging
import math
import os
import platform
import re
import shutil
import sys
import xml.etree.ElementTree as ET

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
WV02p = re.compile("WV02_\w+-M")

# WV03_12FEB061315046-P1BS-10300100106FC100.ntf
WV03p = re.compile("WV03_\w+-M")

# QB02_12FEB061315046-P1BS-10300100106FC100.ntf
QB02p = re.compile("QB02_\w+-M")

# GE01_12FEB061315046-P1BS-10300100106FC100.ntf
GE01p_dg = re.compile("GE01_\w+-M")

# GE01_111211P0011184144A222000100082M_000754776.ntf
GE01p = re.compile("GE01_\w+M0")

# IK01_2009121113234710000011610960_pan_6516S.ntf
IK01p = re.compile("IK01_\w+(blu|msi|bgrn)")

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

        return ortho_functions.GetImageGeometryInfo(src_image, spatial_ref, args,
                                                    return_type='extent_geom')


def main():

    #### Set Up Arguments
    parent_parser, pos_arg_keys = ortho_functions.buildParentArgumentParser()
    parser = argparse.ArgumentParser(
        parents=[parent_parser],
        description="Run/Submit batch pansharpening in parallel"
    )

    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--slurm", action='store_true', default=False,
                        help="submit tasks to SLURM")
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
        # EPSG code is automatically determined in ortho_functions.GetImageStats
        # and ortho_functions.GetImageGeometryInfo functions.
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
    if args.dem:
        if not os.path.isfile(args.dem):
            parser.error("DEM does not exist: {}".format(args.dem))
        if args.l is None:
            if args.dem.endswith('.vrt'):
                total_dem_filesz_gb = 0.0
                tree = ET.parse(args.dem)
                root = tree.getroot()
                for sourceFilename in root.iter('SourceFilename'):
                    dem_filename = sourceFilename.text
                    if not os.path.isfile(dem_filename):
                        parser.error("VRT DEM component raster does not exist: {}".format(dem_filename))
                    dem_filesz_gb = os.path.getsize(dem_filename) / 1024.0 / 1024 / 1024
                    total_dem_filesz_gb += dem_filesz_gb
                dem_filesz_gb = total_dem_filesz_gb
            else:
                dem_filesz_gb = os.path.getsize(args.dem) / 1024.0 / 1024 / 1024
            pbs_req_mem_gb = int(min(50, max(8, math.ceil(dem_filesz_gb) + 2)))
            args.l = 'mem={}gb'.format(pbs_req_mem_gb)
        
    ## Check GDAL version (2.1.0 minimum)
    gdal_version = gdal.VersionInfo()
    try:
        if int(gdal_version) < 2010000:
            parser.error("gdal_pansharpen requires GDAL version 2.1.0 or higher")
    except ValueError:
        parser.error("Cannot parse GDAL version: {}".format(gdal_version))

    #### Set up console logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    #### Handle thread count that exceeds system limits
    if requested_threads > ortho_functions.ARGDEF_CPUS_AVAIL:
        logger.info("threads requested ({0}) exceeds number available on system ({1}), setting thread count to "
                    "'ALL_CPUS'".format(requested_threads, ortho_functions.ARGDEF_CPUS_AVAIL))
        args.threads = 'ALL_CPUS'
    
    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('l', 'qsubscript', 'dryrun', 'pbs', 'slurm', 'parallel_processes', 'tasks_per_job')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    ## Identify source images
    if srctype == 'dir':
        image_list1 = utils.find_images(src, False, ortho_functions.exts)
    elif srctype == 'textfile':
        image_list1 = utils.find_images(src, True, ortho_functions.exts)
    else:
        image_list1 = [src]

    pair_list = []
    unmatched_images = set()
    for srcfp in image_list1:
        #print(srcfp)
        try:
            image_pair = ImagePair(srcfp, spatial_ref, args)
        except RuntimeError as e:
            if (   str(e).startswith("Corresponding panchromatic image not found:")
                or str(e).startswith("Image does not match multispectral name pattern:")):
                _, _, non_multi_fn = str(e).partition(':')
                unmatched_images.add(os.path.basename(non_multi_fn.strip()))
            else:
                logger.error(e)
        else:
            logger.info("Image: %s, Sensor: %s", image_pair.mul_srcfn, image_pair.sensor)
            pair_list.append(image_pair)

    pair_pan_images = set([pair.pan_srcfn for pair in pair_list])
    unmatched_images = unmatched_images.difference(pair_pan_images)
    if len(unmatched_images) > 0:
        parser.error("{} src images could not be paired:\n{}".format(
            len(unmatched_images), '\n'.join(sorted(list(unmatched_images)))
        ))
                
    logger.info('Number of src image pairs: %i', len(pair_list))
    
    ## Build task queue
    i = 0
    pairs_to_process = []
    for image_pair in pair_list:

        if type(args.epsg) is str:
            img_epsg = ortho_functions.GetImageGeometryInfo(image_pair.mul_srcfp, spatial_ref, args,
                                                            return_type='epsg_code')
        else:
            img_epsg = args.epsg
        
        pansh_dstfp = os.path.join(dstdir, "{}_{}{}{}_pansh.tif".format(
            os.path.splitext(image_pair.mul_srcfn)[0],
            bittype,
            args.stretch,
            img_epsg
        ))

        done = os.path.isfile(pansh_dstfp)
        if done is False:
            i += 1
            pairs_to_process.append(image_pair)
            
    logger.info('Number of incomplete tasks: %i', i)

    if len(pairs_to_process) == 0:
        logger.info("No images pairs found to process")
        sys.exit(0)

    task_queue = []

    if args.tasks_per_job and args.tasks_per_job > 1:
        images_to_process = [image_pair.mul_srcfp for image_pair in pairs_to_process]
        task_srcfp_list = utils.write_task_bundles(images_to_process, args.tasks_per_job, scratch, 'Psh_src')
        tasklist_is_text_bundles = True
    else:
        task_srcfp_list = pairs_to_process
        tasklist_is_text_bundles = False

    for job_count, task_item in enumerate(task_srcfp_list, 1):

        if not tasklist_is_text_bundles:
            image_pair = task_item
            if type(args.epsg) is str:
                img_epsg = ortho_functions.GetImageGeometryInfo(image_pair.mul_srcfp, spatial_ref, args,
                                                                return_type='epsg_code')
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

        task = taskhandler.Task(
            task_item_srcfn,
            'Psh{:04g}'.format(job_count),
            'python',
            '{} {} {} {}'.format(
                argval2str(scriptpath),
                arg_str_base,
                argval2str(task_item_srcfp),
                argval2str(dstdir)
            ),
            exec_pansharpen,
            [image_pair, pansh_dstfp, args]
        )
        task_queue.append(task)

    ## Run tasks
    if len(task_queue) > 0:
        logger.info("Submitting Tasks")
        if args.pbs:
            l = "-l {}".format(args.l) if args.l else ""
            try:
                task_handler = taskhandler.PBSTaskHandler(qsubpath, l)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue, dryrun=args.dryrun)
                
        elif args.slurm:
            try:
                task_handler = taskhandler.SLURMTaskHandler(qsubpath)
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
            
                #### remove existing file handler
                logger.removeHandler(lfh)
                
            #### Print Images with Errors    
            for k, v in results.items():
                if v != 0:
                    logger.warning("Failed Image: %s", k)
        
        logger.info("Done")
        
    else:
        logger.info("No images found to process")


def exec_pansharpen(image_pair, pansh_dstfp, args):

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

    if type(args.epsg) is str:
        img_epsg = ortho_functions.GetImageGeometryInfo(image_pair.mul_srcfp, None, args,
                                                        return_type='epsg_code')
    else:
        img_epsg = args.epsg

    bittype = utils.get_bit_depth(args.outtype)
    pan_basename = os.path.splitext(image_pair.pan_srcfn)[0]
    mul_basename = os.path.splitext(image_pair.mul_srcfn)[0]
    pan_local_dstfp = os.path.join(wd, "{}_{}{}{}.tif".format(pan_basename, bittype, args.stretch, img_epsg))
    mul_local_dstfp = os.path.join(wd, "{}_{}{}{}.tif".format(mul_basename, bittype, args.stretch, img_epsg))
    pan_dstfp = os.path.join(dstdir, "{}_{}{}{}.tif".format(pan_basename, bittype, args.stretch, img_epsg))
    mul_dstfp = os.path.join(dstdir, "{}_{}{}{}.tif".format(mul_basename, bittype, args.stretch, img_epsg))
    pansh_tempfp = os.path.join(wd, "{}_{}{}{}_pansh_temp.tif".format(mul_basename, bittype, args.stretch, img_epsg))
    pansh_local_dstfp = os.path.join(wd, "{}_{}{}{}_pansh.tif".format(mul_basename, bittype, args.stretch, img_epsg))
    pansh_xmlfp = os.path.join(dstdir, "{}_{}{}{}_pansh.xml".format(mul_basename, bittype, args.stretch, img_epsg))
    mul_xmlfp = os.path.join(dstdir, "{}_{}{}{}.xml".format(mul_basename, bittype, args.stretch, img_epsg))
    
    if not os.path.isdir(wd):
        os.makedirs(wd)

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
        if args.resolution:
            args.resolution = args.resolution * 4.0
        ortho_functions.process_image(image_pair.mul_srcfp, mul_dstfp, args, image_pair.intersection_geom)

    if not os.path.isfile(mul_local_dstfp) and os.path.isfile(mul_dstfp):
        shutil.copy2(mul_dstfp, mul_local_dstfp)

    ####  Pansharpen
    ## get system info for program extension
    if platform.system() == 'Windows':
        py_ext = ''
    else:
        py_ext = '.py'

    pan_threading = ''
    if hasattr(args, 'threads'):
        if args.threads != 1:
            pan_threading = '-threads {}'.format(args.threads)
    
    logger.info("Pansharpening multispectral image")
    if os.path.isfile(pan_local_dstfp) and os.path.isfile(mul_local_dstfp):
        if not os.path.isfile(pansh_local_dstfp):
            cmd = 'gdal_pansharpen{} -co BIGTIFF=IF_SAFER -co COMPRESS=LZW -co TILED=YES {} "{}" "{}" "{}"'.\
                format(py_ext, pan_threading, pan_local_dstfp, mul_local_dstfp, pansh_local_dstfp)
            taskhandler.exec_cmd(cmd)
    else:
        logger.warning("Pan or Multi warped image does not exist\n\t{}\n\t{}".format(pan_local_dstfp, mul_local_dstfp))

    #### Make pyramids
    if os.path.isfile(pansh_local_dstfp):
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
