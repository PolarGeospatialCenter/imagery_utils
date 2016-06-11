import os, string, sys, shutil, math, glob, re, tarfile, argparse, subprocess, logging
from datetime import datetime, timedelta
import gdal, ogr,osr, gdalconst

from lib import ortho_functions, utils

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Run/Submit batch ndvi calculation in parallel"
    )

    parser.add_argument("src", help="source image, text file, or directory")
    parser.add_argument("dst", help="destination directory")
    pos_arg_keys = ["src","dst"]
    
    parser.add_argument("-s", "--save-temps", action="store_true", default=False,
                    help="save temp files")
    parser.add_argument("--wd",
                    help="local working directory for cluster jobs (default is dst dir)")
    parser.add_argument("--pbs", action='store_true', default=False,
                    help="submit tasks to PBS")
    parser.add_argument("--parallel-processes", type=int, default=1,
                    help="number of parallel processes to spawn (default 1)")
    parser.add_argument("-l", help="PBS resources requested (mimicks qsub syntax)")
    parser.add_argument("--qsubscript",
                    help="qsub script to use in cluster job submission (default is qsub_ndvi.sh in script root folder)")
    parser.add_argument("--dryrun", action="store_true", default=False,
                    help="print actions without executing")

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
    elif os.path.isfile(src.replace('msi','blu')) and os.path.splitext(src)[1].lower() in ortho_functions.exts:
        srctype = 'image'
    else:
        parser.error("Error arg1 is not a recognized file path or file type: %s" %(src))

    if not os.path.isdir(dstdir):
        parser.error("Error arg2 is not a valid file path: %s" %(dstdir))

    ## Verify qsubscript
    if args.qsubscript is None:
        qsubpath = os.path.join(os.path.dirname(scriptpath),'qsub_ndvi.sh')
    else:
        qsubpath = os.path.abspath(args.qsubscript)
    if not os.path.isfile(qsubpath):
        parser.error("qsub script path is not valid: %s" %qsubpath)
        
    ## Verify processing options do not conflict
    if args.pbs and args.parallel_processes > 1:
        parser.error("Options --pbs and --parallel-processes > 1 are mutually exclusive")

    #### Set concole logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('l', 'qsubscript', 'pbs', 'parallel_processes', 'dryrun')
    arg_str = utils.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    ## Identify source images
    if srctype == 'dir':
        image_list = utils.find_images(src, False, ortho_functions.exts)
    elif srctype == 'textfile':
        image_list = utils.find_images(src, True, ortho_functions.exts)
    else:
        image_list = [src]
    logger.info('Number of src images: %i' %len(image_list))
    
    ## Build task queue
    i = 0
    task_queue = []
    for srcfp in image_list:
        srcdir, srcfn = os.path.split(srcfp)
        bn, ext = os.path.splitext(srcfn)
        dstfp = os.path.join(dstdir, bn + '_ndvi.tif')
        
        if not os.path.isfile(dstfp):
            i+=1
            task = utils.Task(
                srcfn,
                'NDVI{:04g}'.format(i),
                'python',
                '{} {} {} {}'.format(scriptpath, arg_str, srcfp, dstdir),
                calc_ndvi,
                [srcfp, dstfp, args]
            )
            task_queue.append(task)
       
    logger.info('Number of incomplete tasks: {}'.format(i))
    
    ## Run tasks
    if len(task_queue) > 0:
        logger.info("Submitting Tasks")
        if args.pbs:
            if args.l:
                task_handler = utils.PBSTaskHandler(qsubpath, "-l {}".format(args.l))
            else:
                task_handler = utils.PBSTaskHandler(qsubpath)
            if not args.dryrun:
                task_handler.run_tasks(task_queue)
            
        elif args.parallel_processes > 1:
            task_handler = utils.ParallelTaskHandler(args.parallel_processes)
            logger.info("Number of child processes to spawn: {0}".format(task_handler.num_processes))
            if not args.dryrun:
                task_handler.run_tasks(task_queue)
    
        else:        
            results = {}
            for task in task_queue:
                           
                srcfp, dstfp, task_arg_obj = task.method_arg_list
                
                #### Set up processing log handler
                logfile = os.path.splitext(dstfp)[0]+".log"
                lfh = logging.FileHandler(logfile)
                lfh.setLevel(logging.DEBUG)
                formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
                lfh.setFormatter(formatter)
                logger.addHandler(lfh)
                
                if not args.dryrun:
                    results[task.name] = task.method(srcfp, dstfp, task_arg_obj)
            
            #### Print Images with Errors    
            for k,v in results.iteritems():
                if v != 0:
                    logger.warning("Failed Image: {}".format(k)) 
        
        logger.info("Done")
        
    else:
        logger.info("No images found to process")
        
    
def calc_ndvi(srcfp, dstfp, args):

    srcdir,srcfn = os.path.split(srcfp)
    dstdir,dstfn = os.path.split(dstfp)
    bn,ext = os.path.splitext(srcfn)
    src_xml = os.path.join(srcdir, bn + '.xml')
    dst_xml = os.path.join(dstdir,bn + '_ndvi.xml')

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
    logger.info("Working Dir: %s" %wd)

    print "Image: %s" %srcfn
    
    ## open image and get band numbers
    ds = gdal.Open(srcfp)
    
    if ds:
        bands = ds.RasterCount
        datatype = ds.GetRasterBand(1).DataType
        if bands == 8:
            red_band = 5
            nir_band = 7
        elif bands == 4:
            red_band = 3
            nir_band = 4
        else:
            logger.error("Cannot calcuclate NDVI from a {} band image: {}".format(bands, srcfp))
            return 1
    else:
        logger.error("Cannot open target image: {}".format(srcfp))
        return 1
    ds = None
                    
    if not os.path.isfile(dstfp):
        ## copy to wd
        srcfp_local = os.path.join(wd,srcfn)
        dstfp_local = os.path.join(wd,os.path.basename(dstfp))
        if not os.path.isfile(srcfp_local):
            shutil.copy2(srcfp, srcfp_local)
        
        ## check if source datatype is floating point.  If not, convert to Float32
        if datatype in [6,7]: # Float32, Float 64
            calc_src = srcfp_local
        elif datatype in [1,2,3,4,5]: # Byte, UInt16, Int16, UInt32, Int32:
            srcfp_float = os.path.join(wd, bn + '_float.tif')
            if not os.path.isfile(srcfp_float):
                cmd = 'gdal_translate -ot Float32 {} {}'.format(srcfp_local, srcfp_float)
                utils.exec_cmd(cmd)
            calc_src = srcfp_float
        else:
            logger.error("Cannot calculate NDVI for datatype {}: {}".format(gdal.GetDataTypeName(datatype), srcfp))
            calc_src = None
            return 1
            
        ## execute gdal_calc
        if calc_src:
            if os.path.isfile(calc_src) and not os.path.isfile(dstfp_local):
                calc = '"(A-B)/(A+B)"'
                cmd = 'gdal_calc.py --calc {4} -A {0} --A_band {1} -B {0} --B_band {2} --outfile {3} --type Float32 --co tiled=yes --co compress=lzw --co bigtiff=if_safer'.format(
                    calc_src,
                    nir_band,
                    red_band,
                    dstfp_local,
                    calc
                )
                utils.exec_cmd(cmd)
            
                ## add pyramids
                if os.path.isfile(dstfp_local):
                    cmd = 'gdaladdo "%s" 2 4 8 16' %(dstfp_local)
                    utils.exec_cmd(cmd)
        
        ## copy to dst
        if wd <> dstdir:
            if os.path.isfile(dstfp_local):
                shutil.copy2(dstfp_local, dstfp)
        
    ## copy xml to dst
    if os.path.isfile(dstfp) and not os.path.isfile(dst_xml):
        shutil.copy2(src_xml, dst_xml)
            
    ## Delete Temp Files
    temp_files = [
        srcfp_local,
        srcfp_float
    ]
    
    wd_files = [
        dstfp_local,
    ]

    if not args.save_temps:
        for f in temp_files:
            try:
                os.remove(f)
            except Exception, e:
                logger.warning('Could not remove %s: %s' %(os.path.basename(f),e))
                
        if wd <> dstdir:
            for f in wd_files:
                try:
                    os.remove(f)
                except Exception, e:
                    logger.warning('Could not remove %s: %s' %(os.path.basename(f),e))
    return 0

if __name__ == '__main__':
    main()
