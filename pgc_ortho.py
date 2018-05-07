import os, sys, logging, argparse
from lib import ortho_functions, utils, taskhandler

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


def main():

    #### Set Up Arguments
    parent_parser, pos_arg_keys = ortho_functions.buildParentArgumentParser()
    parser = argparse.ArgumentParser(
        parents=[parent_parser],
        description="Run/submit batch image ortho and conversion tasks"
    )

    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--slurm", action='store_true', default=False,
                        help="submit tasks to SLURM")
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="submission script to use in PBS/SLURM submission (PBS default is qsub_ortho.sh, SLURM "
                             "default is slurm_ortho.py, in script root folder)")
    parser.add_argument("-l",
                        help="PBS resources requested (mimicks qsub syntax, PBS only)")
    parser.add_argument("--dryrun", action='store_true', default=False,
                        help='print actions without executing')

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
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'qsub_ortho.sh')
            if args.slurm:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'slurm_ortho.sh')
        else:
            qsubpath = os.path.abspath(args.qsubscript)
        if not os.path.isfile(qsubpath):
            parser.error("qsub script path is not valid: {}".format(qsubpath))

    ## Verify processing options do not conflict
    if args.pbs and args.slurm:
        parser.error("Options --pbs and --slurm are mutually exclusive")
    if (args.pbs or args.slurm) and args.parallel_processes > 1:
        parser.error("HPC Options (--pbs or --slurm) and --parallel-processes > 1 are mutually exclusive")

    #### Verify EPSG
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

    #### Set up console logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('l', 'qsubscript', 'dryrun', 'pbs', 'slurm', 'parallel_processes')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)

    ## Identify source images
    if srctype == 'dir':
        image_list1 = utils.find_images(src, False, ortho_functions.exts)
    elif srctype == 'textfile':
        image_list1 = utils.find_images(src, True, ortho_functions.exts)
    else:
        image_list1 = [src]

    ## Group Ikonos
    image_list2 = []
    for srcfp in image_list1:
        srcdir, srcfn = os.path.split(srcfp)
        if "IK01" in srcfn and sum([b in srcfn for b in ortho_functions.ikMsiBands]) > 0:
            for b in ortho_functions.ikMsiBands:
                if b in srcfn:
                    newname = os.path.join(srcdir, srcfn.replace(b, "msi"))
                    break
            image_list2.append(newname)

        else:
            image_list2.append(srcfp)

    image_list = list(set(image_list2))
    logger.info('Number of src images: %i', len(image_list))
    
    ## Build task queue
    i = 0
    task_queue = []
    for srcfp in image_list:
        srcdir, srcfn = os.path.split(srcfp)
        dstfp = os.path.join(dstdir, "{}_{}{}{}{}".format(
            os.path.splitext(srcfn)[0],
            utils.get_bit_depth(args.outtype),
            args.stretch,
            spatial_ref.epsg,
            ortho_functions.formats[args.format]
        ))
        
        done = os.path.isfile(dstfp)
        if done is False:
            i += 1
            task = taskhandler.Task(
                srcfn,
                'Or{:04g}'.format(i),
                'python',
                '{} {} {} {}'.format(scriptpath, arg_str_base, srcfp, dstdir),
                ortho_functions.process_image,
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
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)
                
        elif args.slurm:
            try:
                task_handler = taskhandler.SLURMTaskHandler(qsubpath)
            except RuntimeError as e:
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)
            
        elif args.parallel_processes > 1:
            try:
                task_handler = taskhandler.ParallelTaskHandler(args.parallel_processes)
            except RuntimeError as e:
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
        
        logger.info("Done")
        
    else:
        logger.info("No images found to process")
        

if __name__ == "__main__":
    main()
