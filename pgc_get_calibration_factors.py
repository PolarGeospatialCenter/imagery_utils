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

gdal.SetConfigOption('GDAL_PAM_ENABLED','NO')

#### Initialize Return Code Dictionary
rc_dict = {}


def main():

    #########################################################
    ####  Handle Options
    #########################################################

    #### Set Up Arguments
    parser = argparse.ArgumentParser(description="Get Radiometric Correction info from Images")

    parser.add_argument("src", help="source image, text file, or directory")
    parser.add_argument("-c", "--stretch", choices=ortho_utils.stretches, default="rf",
                      help="stretch type [ns: nostretch, rf: reflectance (default), mr: modified reflectance, rd: absolute radiance]")


    #### Parse Arguments
    opt = parser.parse_args()
    src = os.path.abspath(opt.src)

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

    lso = logging.StreamHandler()
    lso.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)


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

    # Iterate Through Found Images
    for srcfp in image_list:
	
	#### Instantiate ImageInfo object
	info = ortho_utils.ImageInfo()
	info.srcfp = srcfp
	info.srcdir,info.srcfn = os.path.split(srcfp)
	info.vendor, info.sat = ortho_utils.getSensor(info.srcfn)
	info.stretch = opt.stretch
	
	#### Find metadata file
        metafile = ortho_utils.GetDGMetadataPath(info.srcfp)
        if metafile is None:
            metafile = ortho_utils.ExtractDGMetadataFile(info.srcfp,wd)
        if metafile is None:
            metafile = ortho_utils.GetIKMetadataPath(info.srcfp)
        if metafile is None:
            metafile = ortho_utils.GetGEMetadataPath(info.srcfp)
        if metafile is None:
            logger.error("Cannot find metadata for image: {0}".format(info.srcfp))
        else:
            info.metapath = metafile
	
	logger.info(info.srcfn)
	CFlist = ortho_utils.GetCalibrationFactors(info)
    

if __name__ == "__main__":
    main()
