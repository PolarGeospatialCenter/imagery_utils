import os, string, sys, shutil, glob, re, tarfile, logging
from datetime import datetime, timedelta

from subprocess import *
from math import *
from xml.etree import cElementTree as ET

from lib.mosaic import *
import numpy
import gdal, ogr,osr, gdalconst
    
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


    
def main():
    
     #########################################################
    ####  Handle args
    #########################################################

    #### Set Up Arguments 
    parent_parser = buildMosaicParentArgumentParser()
    parser = argparse.ArgumentParser(
	parents=[parent_parser],
	description="Create mosaic subtile"
	)
    
    parser.add_argument("tile", help="output tile name")
    parser.add_argument("src", help="textfile of input rasters (tif only)")
    
    parser.add_argument("--wd",
                        help="scratch space (default is mosaic directory)")
    parser.add_argument("--gtiff_compression", choices=GTIFF_COMPRESSIONS, default="lzw",
                        help="GTiff compression type. Default=lzw (%s)"%string.join(GTIFF_COMPRESSIONS,','))
    
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    
    status = 0
        
    bands = args.bands
    inpath = args.src
    tile = args.tile
    ref_xres, ref_yres = args.resolution
    xmin,xmax,ymin,ymax = args.extent
    dims = "-tr %s %s -te %s %s %s %s" %(ref_xres,ref_yres,xmin,ymin,xmax,ymax)
    
    ##### Configure Logger
    logfile = os.path.splitext(tile)[0]+".log"
    lfh = logging.FileHandler(logfile)
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh) 
    
    #### get working directory
    if args.wd:
        if os.path.isdir(args.wd):
            localpath = args.wd
        else:
            parser.error("scratch space directory does not exist: {0}".format(args.wd))
    else:
        localpath = os.path.dirname(tile)
    
    intersects = []
    t = open(inpath,'r')
    for line in t.readlines():
        intersects.append(line.rstrip('\n').rstrip('\r'))
    t.close()
    
    logger.info(tile)
    #logger.info str(intersects))
    
    wd = os.path.join(localpath,os.path.splitext(os.path.basename(tile))[0])
    if not os.path.isdir(wd):
        os.makedirs(wd)
    localtile2 = os.path.join(wd,os.path.basename(tile)) 
    localtile1 = localtile2.replace(".tif","_temp.tif")
    
    del_images = []
    final_intersects = []
    images = {}
    
    for image in intersects:
        ds = gdal.Open(image)
        if ds is not None:
            srcbands = ds.RasterCount
            
            images[image] = srcbands
            final_intersects.append(image)
            logger.info("%s" %(os.path.basename(image)))
    
        ds = None
    
    logger.info("Number of images: %i" %(len(final_intersects)))
    
    
    #### Get Extent geometry 
    poly_wkt = 'POLYGON (( %s %s, %s %s, %s %s, %s %s, %s %s ))' %(xmin,ymin,xmin,ymax,xmax,ymax,xmax,ymin,xmin,ymin)
    tile_geom = ogr.CreateGeometryFromWkt(poly_wkt)
    
    c = 0
    for img in final_intersects:
            
        #### Check if bands number is correct
        mergefile = img
        
        if args.force_pan_to_multi is True and bands > 1:
            srcbands = images[img]
            if srcbands == 1:
                mergefile = os.path.join(wd,os.path.basename(img)[:-4])+"_merge.tif"
                cmd = 'gdal_merge.py -ps %s %s -separate -o "%s" "%s"' %(ref_xres, ref_yres, mergefile, string.join(([img] * bands),'" "'))
                ExecCmd(cmd)
        srcnodata = string.join((['0'] * bands)," ")
            
        if c == 0:
            if os.path.isfile(localtile1):
                logger.info("localtile1 already exists")
                status = 1
                break
            cmd = 'gdalwarp %s -srcnodata "%s" -dstnodata "%s" "%s" "%s"' %(dims,srcnodata,srcnodata,mergefile,localtile1)
            ExecCmd(cmd)
            
        else:
            cmd = 'gdalwarp -srcnodata "%s" "%s" "%s"' %(srcnodata,mergefile,localtile1)
            ExecCmd(cmd)
            
        c += 1
        
        
        if not mergefile == img:
            del_images.append(mergefile)
            
    del_images.append(localtile1)        
    
    if status == 0:
        ####  Write to Compressed file
        if os.path.isfile(localtile1):
            if args.gtiff_compression == 'lzw':
                compress_option = '-co "compress=lzw"'
            elif args.gtiff_compression == 'jpeg95':
                compress_option =  '-co "compress=jpeg" -co "jpeg_quality=95"'
                
            cmd = 'gdal_translate -stats -of GTiff %s -co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "BIGTIFF=IF_SAFER" "%s" "%s"' %(compress_option,localtile1,localtile2)
            ExecCmd(cmd)
        
        ####  Build Pyramids        
        if os.path.isfile(localtile2):
            cmd = 'gdaladdo "%s" 2 4 8 16 30' %(localtile2)
            ExecCmd(cmd)
        
        #### Copy tile to destination
        if os.path.isfile(localtile2):
            logger.info("Copying output files to destination dir")
            copyall(localtile2,os.path.dirname(tile))
            
        del_images.append(localtile2)
    
    
    #### Delete temp files
    deleteTempFiles(del_images)
    os.rmdir(wd)
   
    logger.info("Done")

if __name__ == '__main__':
    main()