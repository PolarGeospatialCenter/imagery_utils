#!/usr/bin/env python

import argparse
import logging
import math
import os
import shutil
import sys

import numpy
from osgeo import gdal

from lib import ortho_functions, taskhandler, utils
from lib import VERSION

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

outtypes = ['Float32', 'Int16']

def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Run/Submit batch ndvi calculation in parallel"
    )

    parser.add_argument("src", help="source image, text file, or directory")
    parser.add_argument("dst", help="destination directory")
    pos_arg_keys = ["src", "dst"]

    parser.add_argument("-t", "--outtype", choices=outtypes, default='Float32',
                        help="output data type (for Int16, output values are scaled from -1000 to 1000)")
    parser.add_argument("-s", "--save-temps", action="store_true", default=False,
                        help="save temp files")
    parser.add_argument("--wd",
                        help="local working directory for cluster jobs (default is dst dir)")
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
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="submission script to use in PBS/SLURM submission (PBS default is qsub_ndvi.sh, SLURM "
                             "default is slurm_ndvi.py, in script root folder)")
    parser.add_argument("-l", help="PBS resources requested (mimicks qsub syntax, PBS only)")
    parser.add_argument("--skip-cmd-txt", action='store_true', default=False,
                        help='Skip writing the txt file containing the input command.')
    parser.add_argument("--dryrun", action="store_true", default=False,
                        help="print actions without executing")
    parser.add_argument("--version", action='version', version="imagery_utils v{}".format(VERSION))


    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(args.src)
    dstdir = os.path.abspath(args.dst)

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

    ## Verify qsubscript
    if args.pbs or args.slurm:
        if args.qsubscript is None:
            if args.pbs:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'qsub_ndvi.sh')
            if args.slurm:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'slurm_ndvi.sh')
        else:
            qsubpath = os.path.abspath(args.qsubscript)
        if not os.path.isfile(qsubpath):
            parser.error("qsub script path is not valid: {}".format(qsubpath))

    # Parse slurm log location
    if args.slurm:
        # by default, the parent directory of the dst dir is used for saving slurm logs
        if args.slurm_log_dir == None:
            slurm_log_dir = os.path.abspath(os.path.join(dstdir, os.pardir))
            print("slurm log dir: {}".format(slurm_log_dir))
        # if "working_dir" is passed in the CLI, use the default slurm behavior which saves logs in working dir
        elif args.slurm_log_dir == "working_dir":
            slurm_log_dir = None
        # otherwise, verify that the path for the logs is a valid path
        else:
            slurm_log_dir = os.path.abspath(args.slurm_log_dir)
        # Verify slurm log path
        if not os.path.isdir(slurm_log_dir):
            parser.error("Error directory for slurm logs is not a valid file path: {}".format(slurm_log_dir))
        logger.info("Slurm output and error log saved here: {}".format(slurm_log_dir))
        
    ## Verify processing options do not conflict
    if args.pbs and args.slurm:
        parser.error("Options --pbs and --slurm are mutually exclusive")
    if (args.pbs or args.slurm) and args.parallel_processes > 1:
        parser.error("HPC Options (--pbs or --slurm) and --parallel-processes > 1 are mutually exclusive")

    # write input command to text file next to output folder for reference
    command_str = ' '.join(sys.argv)
    logger.info("Running command: {}".format(command_str))
    if not args.skip_cmd_txt and not args.dryrun:
        utils.write_input_command_txt(command_str,dstdir)
        args.skip_cmd_txt = True

    #### Set concole logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('l', 'qsubscript', 'pbs', 'slurm', 'parallel_processes', 'dryrun')
    arg_str = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    ## Identify source images
    if srctype == 'dir':
        image_list = utils.find_images(src, False, ortho_functions.exts)
    elif srctype == 'textfile':
        image_list = utils.find_images(src, True, ortho_functions.exts)
    else:
        image_list = [src]
    logger.info('Number of src images: %i', len(image_list))
    
    ## Build task queue
    i = 0
    task_queue = []
    for srcfp in image_list:
        srcdir, srcfn = os.path.split(srcfp)
        bn, ext = os.path.splitext(srcfn)
        dstfp = os.path.join(dstdir, bn + '_ndvi.tif')
        
        if not os.path.isfile(dstfp):
            i += 1

            # add a custom name to the job
            if not args.slurm_job_name:
                job_name = 'NDVI{:04g}'.format(i)
            else:
                job_name = str(args.slurm_job_name)

            task = taskhandler.Task(
                srcfn,
                job_name,
                'python',
                '{} {} {} {}'.format(scriptpath, arg_str, srcfp, dstdir),
                calc_ndvi,
                [srcfp, dstfp, args]
            )
            task_queue.append(task)
       
    logger.info('Number of incomplete tasks: %i', i)
    
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
                    task_handler.run_tasks(task_queue)
                
        elif args.slurm:
            qsub_args = ""
            if not slurm_log_dir == None:
                qsub_args += '-o {}/%x.o%j '.format(slurm_log_dir)
                qsub_args += '-e {}/%x.o%j '.format(slurm_log_dir)
            # adjust wallclock if submitting multiple tasks ro be run in serial for a single slurm job
            # default wallclock for ortho jobs is 1:00:00, refer to slurm_ndvi.sh to verify
            if args.tasks_per_job:
                qsub_args += '-t {}:00:00 '.format(args.tasks_per_job)
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
                           
                srcfp, dstfp, task_arg_obj = task.method_arg_list
                
                #### Set up processing log handler
                logfile = os.path.splitext(dstfp)[0] + ".log"
                lfh = logging.FileHandler(logfile)
                lfh.setLevel(logging.DEBUG)
                formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
                lfh.setFormatter(formatter)
                logger.addHandler(lfh)
                
                if not args.dryrun:
                    results[task.name] = task.method(srcfp, dstfp, task_arg_obj)
                    
                #### remove existing file handler
                logger.removeHandler(lfh)
            
            #### Print Images with Errors    
            for k, v in results.items():
                if v != 0:
                    logger.warning("Failed Image: %s", k)
        
        logger.info("Done")
        
    else:
        logger.info("No images found to process")
        
    
def calc_ndvi(srcfp, dstfp, args):

    # ndvi nodata value
    ndvi_nodata = -9999

    # tolerance for floating point equality
    tol = 0.00001

    # get basenames for src and dst files, get xml metadata filenames
    srcdir, srcfn = os.path.split(srcfp)
    dstdir, dstfn = os.path.split(dstfp)
    bn, ext = os.path.splitext(srcfn)
    src_xml = os.path.join(srcdir, bn + '.xml')
    dst_xml = os.path.join(dstdir, bn + '_ndvi.xml')

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

    print("Image: {}".format(srcfn))
   
    ## copy source image to working directory
    srcfp_local = os.path.join(wd, srcfn)
    if not os.path.isfile(srcfp_local):
        shutil.copy2(srcfp, srcfp_local)

    ## open image and get band numbers
    ds = gdal.Open(srcfp_local)
    if ds:
        bands = ds.RasterCount
        if bands == 8:
            red_band_num = 5
            nir_band_num = 7
        elif bands == 4:
            red_band_num = 3
            nir_band_num = 4
        else:
            logger.error("Cannot calculate NDVI from a %i band image: %s", bands, srcfp_local)
            clean_up([srcfp_local])
            return 1
    else:
        logger.error("Cannot open target image: %s", srcfp_local)
        clean_up([srcfp_local])
        return 1

    ## check for input data type - must be float or int
    datatype = ds.GetRasterBand(1).DataType
    if datatype not in [1, 2, 3, 4, 5, 6, 7]:
        logger.error("Invalid input data type %s", datatype)
        clean_up([srcfp_local])
        return 1 

    ## get the raster dimensions
    nx = ds.RasterXSize
    ny = ds.RasterYSize

    ## open output file for write and copy proj/geotransform info
    if not os.path.isfile(dstfp):
        dstfp_local = os.path.join(wd, os.path.basename(dstfp))
        gtiff_options = ['TILED=YES', 'COMPRESS=LZW', 'BIGTIFF=YES']
        driver = gdal.GetDriverByName('GTiff')
        out_ds = driver.Create(dstfp_local, nx, ny, 1, gdal.GetDataTypeByName(args.outtype), gtiff_options)
        if out_ds:
            out_ds.SetGeoTransform(ds.GetGeoTransform())
            out_ds.SetProjection(ds.GetProjection())
            ndvi_band = out_ds.GetRasterBand(1)
            ndvi_band.SetNoDataValue(float(ndvi_nodata))
        else:
            logger.error("Couldn't open for write: %s", dstfp_local)
            clean_up([srcfp_local])
            return 1

        ## for red and nir bands, get band data, nodata values, and natural block size
        ## if NoData is None default it to zero.
        red_band = ds.GetRasterBand(red_band_num)
        if red_band is None:
            logger.error("Can't load band %i from %s", red_band_num, srcfp_local)
            clean_up([srcfp_local])
            return 1
        red_nodata = red_band.GetNoDataValue()
        if red_nodata is None:
            logger.info("Defaulting red band nodata to zero")
            red_nodata = 0.0
        (red_xblocksize, red_yblocksize) = red_band.GetBlockSize()
    
        nir_band = ds.GetRasterBand(nir_band_num)
        if nir_band is None:
            logger.error("Can't load band %i from %s", nir_band_num, srcfp_local)
            clean_up([srcfp_local])
            return 1
        nir_nodata = nir_band.GetNoDataValue()
        if nir_nodata is None:
            logger.info("Defaulting nir band nodata to zero")
            nir_nodata = 0.0
        (nir_xblocksize, nir_yblocksize) = nir_band.GetBlockSize()

        ## if different block sizes choose the smaller of the two
        xblocksize = min([red_xblocksize, nir_xblocksize])
        yblocksize = min([red_yblocksize, nir_yblocksize])

        ## calculate the number of x and y blocks to read/write
        nxblocks = int(math.floor(nx + xblocksize - 1) / xblocksize)
        nyblocks = int(math.floor(ny + yblocksize - 1) / yblocksize)

        ## blocks loop
        yblockrange = range(nyblocks)
        xblockrange = range(nxblocks)
        for yblock in yblockrange:
            ## y offset for ReadAsArray
            yoff = yblock * yblocksize

            ## get block actual y size in case of partial block at edge
            if yblock < nyblocks - 1:
                block_ny = yblocksize
            else:
                block_ny = ny - (yblock * yblocksize)

            for xblock in xblockrange:
                ## x offset for ReadAsArray
                xoff = xblock * xblocksize

                ## get block actual x size in case of partial block at edge
                if xblock < (nxblocks - 1):
                    block_nx = xblocksize
                else:
                    block_nx = nx - (xblock * xblocksize)

                ## read a block from each band
                red_array = red_band.ReadAsArray(xoff, yoff, block_nx, block_ny)
                nir_array = nir_band.ReadAsArray(xoff, yoff, block_nx, block_ny)

                ## generate mask for red nodata, nir nodata, and 
                ## (red+nir) less than tol away from zero
                red_mask = (red_array == red_nodata)
                if red_array[red_mask].size > 0:
                    nir_mask = (nir_array == nir_nodata)
                    if nir_array[nir_mask].size > 0:
                        divzero_mask = abs(nir_array + red_array) < tol
                        if red_array[divzero_mask].size > 0:
                            ndvi_mask = red_mask | nir_mask | divzero_mask
                        else:
                            ndvi_mask = red_mask | nir_mask
                    else:
                        divzero_mask = abs(nir_array + red_array) < tol
                        if red_array[divzero_mask].size > 0:
                            ndvi_mask = red_mask | divzero_mask
                        else:
                            ndvi_mask = red_mask
                else:
                    nir_mask = (nir_array == nir_nodata)
                    if nir_array[nir_mask].size > 0:
                        divzero_mask = abs(nir_array + red_array) < tol
                        if red_array[divzero_mask].size > 0:
                            ndvi_mask = nir_mask | divzero_mask
                        else:
                            ndvi_mask = nir_mask
                    else:
                        divzero_mask = abs(nir_array + red_array) < tol
                        if red_array[divzero_mask].size > 0:
                            ndvi_mask = divzero_mask
                        else:
                            ndvi_mask = numpy.full_like(red_array, fill_value=0, dtype=bool)

                ## declare ndvi array, init to nodata value
                ndvi_array = numpy.full_like(red_array, fill_value=ndvi_nodata, dtype=numpy.float32)
                ## cast bands to float for calc
                red_asfloat = numpy.array(red_array, dtype=numpy.float32)
                red_array = None
                nir_asfloat = numpy.array(nir_array, dtype=numpy.float32)
                nir_array = None

                ## calculate ndvi
                if ndvi_array[~ndvi_mask].size > 0:
                    ndvi_array[~ndvi_mask] = numpy.divide(numpy.subtract(nir_asfloat[~ndvi_mask],
                                                                         red_asfloat[~ndvi_mask]),
                                                          numpy.add(nir_asfloat[~ndvi_mask],
                                                                    red_asfloat[~ndvi_mask]))
                red_asfloat = None
                nir_asfloat = None

                ## scale and cast to int if outtype integer
                if args.outtype == 'Int16':
                    ndvi_scaled = numpy.full_like(ndvi_array, fill_value=ndvi_nodata, dtype=numpy.int16)
                    if ndvi_scaled[~ndvi_mask].size > 0:
                        ndvi_scaled[~ndvi_mask] = numpy.array(ndvi_array[~ndvi_mask]*1000.0, dtype=numpy.int16)
                    ndvi_array = ndvi_scaled
                    ndvi_scaled = None

                ndvi_mask = None
               
                ## write valid portion of ndvi array to output file
                ndvi_band.WriteArray(ndvi_array, xoff, yoff)
                ndvi_array = None

        out_ds = None
        ds = None
        
        if os.path.isfile(dstfp_local):
            ## add pyramids
            cmd = 'gdaladdo "{}" 2 4 8 16'.format(dstfp_local)
            taskhandler.exec_cmd(cmd)

            ## copy to dst
            if wd != dstdir:
                shutil.copy2(dstfp_local, dstfp)

            ## copy xml to dst
            if os.path.isfile(src_xml):
                shutil.copy2(src_xml, dst_xml)
            else:
                logger.warning("xml %s not found", src_xml)
        
            ## Delete Temp Files
            temp_files = [srcfp_local]
            wd_files = [dstfp_local]
            if not args.save_temps:
                clean_up(temp_files)
            if wd != dstdir:
                clean_up(wd_files)
        else:
            logger.error("pgc_ndvi.py: %s was not created", dstfp_local)
            return 1 
            
    else:
        logger.info("pgc_ndvi.py: file %s already exists", dstfp)
        
        ## copy xml to dst if missing
        if not os.path.isfile(dst_xml):
            shutil.copy2(src_xml, dst_xml)
            
    return 0

def clean_up(filelist):
    for f in filelist:
        try:
            os.remove(f)
        except Exception as e:
            logger.error(utils.capture_error_trace())
            logger.warning('Could not remove %s: %s', os.path.basename(f), e)


if __name__ == '__main__':
    main()
