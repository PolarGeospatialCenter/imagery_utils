import os, string, sys, shutil, math, glob, re, tarfile, logging, shlex, argparse
from datetime import datetime, timedelta

# from subprocess import *
from xml.dom import minidom
from xml.etree import cElementTree as ET

import gdal, ogr,osr, gdalconst

from lib import ortho_utils as ortho_utils

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

#### Initialize Return Code Dictionary
rc_dict = {}


def main():

    #########################################################
    ####  Handle Options
    #########################################################

    #### Set Up Arguments
    parent_parser, pos_arg_keys = ortho_utils.buildParentArgumentParser()
    parser = argparse.ArgumentParser(parents=[parent_parser],
                                     description="Run batch image ortho and conversion in serial")

    parser.add_argument("--log", help="file to log progress.  Defaults to <output dir>\process.log")


    #### Parse Arguments
    opt = parser.parse_args()
    src = os.path.abspath(opt.src)
    dstdir = os.path.abspath(opt.dst)

    #### Validate Required Arguments
    if os.path.isdir(src):
        srctype = 'dir'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() == '.txt':
        srctype = 'textfile'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() in ortho_utils.exts:
        srctype = 'image'
    elif os.path.isfile(src.replace('msi','blu')) and os.path.splitext(src)[1].lower() in ortho_utils.exts:
        srctype = 'image'
    else:
        parser.error("Arg1 is not a recognized file path or file type: %s" % (src))

    if not os.path.isdir(dstdir):
        parser.error("Error arg2 is not a valid file path: %s" % dstdir)

    #### Verify EPSG
    try:
        spatial_ref = ortho_utils.SpatialRef(opt.epsg)
    except RuntimeError, e:
        parser.error(e)

    #### Verify that dem and ortho_height are not both specified
    if opt.dem is not None and opt.ortho_height is not None:
        parser.error("--dem and --ortho_height options are mutually exclusive.  Please choose only one.")
    
    #### Set Up Logging Handlers
    if opt.log == None:
        logfile = os.path.join(dstdir,"process.log")
    else:
        logfile = os.path.abspath(opt.log)
    if os.path.isdir(os.path.dirname(logfile)) is False:
        parser.error("Logfile directory is not valid: %s" % os.path.dirname(logfile))

    lso = logging.StreamHandler()
    lso.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)

    lfh = logging.FileHandler(logfile)
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)

    #### Print Warning regarding DEM use
    if opt.dem == None:
        ortho_utils.LogMsg("\nWARNING: No DEM is being used in this orthorectification.\nUse the -d flag on the command line to input a DEM\n")
    else:
        #### Test if DEM exists
        if not os.path.isfile(opt.dem):
            ortho_utils.LogMsg("ERROR: DEM does not exist: %s" % opt.dem)
            sys.exit()

    #### Find Images
    if srctype == "dir":
        image_list = ortho_utils.FindImages(src, ortho_utils.exts)
    elif srctype == "textfile":
        t = open(src,'r')
        image_list = []
        for line in t.readlines():
            image_list.append(line.rstrip('\n'))
        t.close()
    elif srctype == "image":
        image_list = [src]


    #### Group Ikonos
    image_list2 = []
    for srcfp in image_list:
        srcdir,srcfn = os.path.split(srcfp)
        if "IK01" in srcfn and sum([b in srcfn for b in ortho_utils.ikMsiBands]) > 0:
            for b in ortho_utils.ikMsiBands:
                if b in srcfn:
                    newname = os.path.join(srcdir,srcfn.replace(b,"msi"))
            image_list2.append(newname)

        else:
            image_list2.append(srcfp)

    image_list3 = list(set(image_list2))

    # Iterate Through Found Images
    for srcfp in image_list3:

        srcdir, srcfn = os.path.split(srcfp)

        #### Derive dstfp
        stretch = opt.stretch

        dstfp = os.path.join(dstdir,"%s_%s%s%d%s" % (os.path.splitext(srcfn)[0],
	    ortho_utils.getBitdepth(opt.outtype),
	    opt.stretch,
	    spatial_ref.epsg,
	    ortho_utils.formats[opt.format]
	    ))

        done = os.path.isfile(dstfp)

        if done is False:
            rc_dict[srcfn] = ortho_utils.processImage(srcfp,dstfp,opt)


    #### Print Images with Errors
    for k,v in rc_dict.iteritems():
        if v != 0:
            ortho_utils.LogMsg("Failed Image: %s" %(k))



if __name__ == "__main__":
    main()
