import os, string, sys, shutil, math, glob, re, tarfile, logging, shlex, argparse
from datetime import datetime, timedelta

import subprocess
from xml.dom import minidom
from xml.etree import cElementTree as ET

import gdal, ogr,osr, gdalconst
import multiprocessing as mp
from lib.ortho_utils import *

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

SUBMISSION_TYPES = ['HPC','VM']

def main():

    #########################################################
    ####  Handle Options
    #########################################################

    #### Set Up Arguments
    parent_parser, pos_arg_keys = buildParentArgumentParser()
    parser = argparse.ArgumentParser(
	parents=[parent_parser],
	description="Run/Submit batch image ortho and conversion in parallel"
	)

    parser.add_argument("--submission_type", choices=SUBMISSION_TYPES,
			help="job submission type. Default is determined automatically (%s)"%string.join(SUBMISSION_TYPES,','))
    parser.add_argument("--processes", type=int,
			help="number of processes to spawn for orthoing individual images (default 1). Use only on non-HPC runs.")
    parser.add_argument("--qsubscript",
		      help="qsub script to use in cluster job submission (default is qsub_ortho.sh in script root folder)")
    parser.add_argument("-l",
		      help="PBS resources requested (mimicks qsub syntax)")
    parser.add_argument("--dryrun", action='store_true', default=False,
			help='print actions without executing')


    #### Parse Arguments
    opt = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(opt.src)
    dstdir = os.path.abspath(opt.dst)


    #### Validate Required Arguments
    if os.path.isdir(src):
	srctype = 'dir'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() == '.txt':
	srctype = 'textfile'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() in exts:
	srctype = 'image'
    elif os.path.isfile(src.replace('msi','blu')) and os.path.splitext(src)[1].lower() in exts:
	srctype = 'image'
    else:
	parser.error("Error arg1 is not a recognized file path or file type: %s" %(src))


    if not os.path.isdir(dstdir):
	parser.error("Error arg2 is not a valid file path: %s" %(dstdir))


    if opt.qsubscript is None:
	qsubpath = os.path.join(os.path.dirname(scriptpath),'qsub_ortho.sh')
    else:
	qsubpath = os.path.abspath(opt.qsubscript)

    if not os.path.isfile(qsubpath):
	parser.error("qsub script path is not valid: %s" %qsubpath)


    #### Verify EPSG
    try:
	spatial_ref = SpatialRef(opt.epsg)
    except RuntimeError, e:
	parser.error(e)

    #### Verify that dem and ortho_height are not both specified
    if opt.dem is not None and opt.ortho_height is not None:
	parser.error("--dem and --ortho_height options are mutually exclusive.  Please choose only one.")

    #### Test if DEM exists
    if opt.dem:
	if not os.path.isfile(opt.dem):
	    parser.error("DEM does not exist: %s" %opt.dem)
    
    #### Set Up Logging Handlers
    lso = logging.StreamHandler()
    lso.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    
    ###############################
    ####  Submission logic
    ################################

    if srctype in ['dir','textfile']:
	
	    
	####  Determine submission type based on presence of pbsnodes cmd
	try:
	    cmd = "pbsnodes"
	    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	    so, se = p.communicate()
	except OSError,e:
	    is_hpc = False
	else:
	    is_hpc = True
    
	if opt.submission_type is None:
	    submission_type = "HPC" if is_hpc else "VM"
	elif opt.submission_type == "HPC" and is_hpc is False:
	    parser.error("Submission type HPC is not available on this system")
	else:
	    submission_type = opt.submission_type
    
	logger.info("Submission type: {0}".format(submission_type))
	task_queue = []
    
	if opt.processes and not submission_type == 'VM':
	    logger.warning("--processes option will not be used becasue submission type is not VM")
    
	if submission_type == 'VM':
	    processes = 1
	    if opt.processes:
		if mp.cpu_count() < opt.processes:
		    logger.warning("Specified number of processes ({0}) is higher than the system cpu count ({1}), using default".format(opt.proceses,mp.count_cpu()))
    
		elif opt.processes < 1:
		    logger.warning("Specified number of processes ({0}) must be greater than 0, using default".format(opt.proceses,mp.count_cpu()))
    
		else:
		    processes = opt.processes
    
	    logger.info("Number of child processes to spawn: {0}".format(processes))
	

	#### Get args ready to pass through
	#### Get -l args and make a var
	l = ("-l %s" %opt.l) if opt.l is not None else ""

	args_dict = vars(opt)
	arg_list = []
	arg_keys_to_remove = ('l','qsubscript','dryrun')

	## Add optional args to arg_list
	for k,v in args_dict.iteritems():
	    if k not in pos_arg_keys and k not in arg_keys_to_remove and v is not None:
		if isinstance(v,list) or isinstance(v,tuple):
		    arg_list.append("--%s %s" %(k,' '.join([str(item) for item in v])))
		elif isinstance(v,bool):
		    if v is True:
			arg_list.append("--%s" %(k))
		else:
		    arg_list.append("--%s %s" %(k,str(v)))

	arg_str = " ".join(arg_list)


	if srctype == 'dir':
	    image_list = FindImages(src,exts)
	elif srctype == 'textfile':
	    t = open(src,'r')
	    image_list = []
	    for line in t.readlines():
		if os.path.isfile(line.rstrip()):
		    image_list.append(line.rstrip())
		elif not line == '\n':
		    LogMsg('Src image does not exist: %s' %line.rstrip())
	    t.close()


	#### Group Ikonos
	image_list2 = []
	for srcfp in image_list:
	    srcdir,srcfn = os.path.split(srcfp)
	    if "IK01" in srcfn and sum([b in srcfn for b in ikMsiBands]) > 0:
		for b in ikMsiBands:
		    if b in srcfn:
			newname = os.path.join(srcdir,srcfn.replace(b,"msi"))
		image_list2.append(newname)

	    else:
		image_list2.append(srcfp)

	image_list3 = list(set(image_list2))


	#### Iterate Through Found Images
	logger.info('Number of src images: %i' %len(image_list3))
	i = 0
	task_queue = []

	for srcfp in image_list3:

	    srcdir, srcfn = os.path.split(srcfp)
	    dstfp = os.path.join(dstdir,"%s_%s%s%d%s" %(
		os.path.splitext(srcfn)[0],
		getBitdepth(opt.outtype),
		opt.stretch,
		spatial_ref.epsg,
		formats[opt.format]
		))

	    done = os.path.isfile(dstfp)

	    if done is False:

		if submission_type == 'HPC':
		    cmd = r'qsub %s -N Ortho%04i -v p1="%s %s %s %s" "%s"' %(l,i,scriptpath,arg_str,srcfp,dstdir,qsubpath)

		elif submission_type == 'VM':
		    cmd = r'python %s %s %s %s' %(scriptpath,arg_str,srcfp,dstdir)

		else:
		    cmd = None

		job_name = srcfn
		task_queue.append((job_name,cmd))
		i+=1

	logger.info("Number of images to process: %i" %i)

	if not opt.dryrun:
	#logger.info(task_queue)
	    logger.info("Submitting jobs")
	    if submission_type == 'HPC':
		for task in task_queue:
		    job_name,cmd = task
		    subprocess.call(cmd,shell=True)
		logger.info("Images submitted: %i" %i)    
	    elif submission_type == 'VM':
		pool = mp.Pool(processes)
		try:
		    pool.map(ExecCmd_mp,task_queue,1)
		except KeyboardInterrupt:
		    pool.terminate()
		    logger.info("Processes terminated without file cleanup")
		else:
		    logger.info("Done")


    ###############################
    ####  Execution logic
    ################################

    elif srctype == 'image':
	srcdir, srcfn = os.path.split(src)

	#### Derive dstfp
	dstfp = os.path.join(dstdir,"%s_%s%s%d%s" %(
	    os.path.splitext(srcfn)[0],
	    getBitdepth(opt.outtype),
	    opt.stretch,
	    spatial_ref.epsg,
	    formats[opt.format]
	    ))
	
	#### Set Up Logging Handlers
	logfile = os.path.splitext(dstfp)[0]+".log"
	lfh = logging.FileHandler(logfile)
	lfh.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
	lfh.setFormatter(formatter)
	logger.addHandler(lfh)

	done = os.path.isfile(dstfp)

	if done is False:
	    rc = processImage(src,dstfp,opt)



if __name__ == "__main__":
    main()

