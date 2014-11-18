import os, string, sys, shutil, glob, re, tarfile, logging
from datetime import datetime, timedelta

from subprocess import *
from math import *
from xml.etree import cElementTree as ET

from lib.mosaic import *

import gdal, ogr,osr, gdalconst
    

#### Set the working directory.  This may need to be changed on other systems.
localpath = r'/local'

import numpy

    
    
def main():
    
    status = 0
    #### Parse Arguments
    if len(sys.argv) != 12:
        print("incorrect number of arguments: 12 required, %i found \n%s" %(len(sys.argv),str(sys.argv)))
        sys.exit(1)
        
    bands = int(sys.argv[1])
    inpath = sys.argv[2]
    tile = sys.argv[3]
    force_pan_to_multi = bool(sys.argv[4])
    ref_xres = sys.argv[5]
    ref_yres = sys.argv[6]
    minx = sys.argv[7]
    miny = sys.argv[8]
    maxx = sys.argv[9]
    maxy = sys.argv[10]
    gtiff_compression = sys.argv[11]
    
    dims = "-tr %s %s -te %s %s %s %s" %(ref_xres,ref_yres,minx,miny,maxx,maxy)
    
    intersects = []
    t = open(inpath,'r')
    for line in t.readlines():
        intersects.append(line.rstrip('\n').rstrip('\r'))
    t.close()
    
    print (tile)
    #print (str(intersects))
    
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
            tm = datetime.today()
            print ("%s - %s" %(tm.strftime("%d-%b-%Y %H:%M:%S"),os.path.basename(image)))
    
        ds = None
    
    print "Number of images: %i" %(len(final_intersects))
    
    
    #### Get Extent geometry
    #dims = "-tr %f %f -te %f %f %f %f" %(ref_xres,ref_yres,minx,miny,maxx,maxy)
    dl = dims.split(" ")
    #poly_wkt = 'POLYGON (( '+str(minx)+' '+str(miny)+', '+str(minx)+' '+str(maxy)+', '+str(maxx)+' '+str(maxy)+', '+str(maxx)+' '+str(miny)+', '+str(minx)+' '+str(miny)+' ))'
    minx = dl[4]
    maxx = dl[6]
    miny = dl[5]
    maxy = dl[7]
    xsize = dl[1]
    ysize = dl[2]
    poly_wkt = 'POLYGON (( %s %s, %s %s, %s %s, %s %s, %s %s ))' %(minx,miny,minx,maxy,maxx,maxy,maxx,miny,minx,miny)
    tile_geom = ogr.CreateGeometryFromWkt(poly_wkt)
    
    
    
    c = 0
    for img in final_intersects:
            
        #### Check if bands number is correct
        mergefile = img
        
        if force_pan_to_multi is True and bands > 1:
            srcbands = images[img]
            if srcbands == 1:
                tm = datetime.today()
                print tm.strftime("%d-%b-%Y %H:%M:%S"),
                mergefile = os.path.join(wd,os.path.basename(img)[:-4])+"_merge.tif"
                cmd = 'gdal_merge.py -ps %s %s -separate -o "%s" "%s"' %(xsize, ysize, mergefile, string.join(([img] * bands),'" "'))
                ExecCmd(cmd)
        srcnodata = string.join((['0'] * bands)," ")
        
        tm = datetime.today()
        print tm.strftime("%d-%b-%Y %H:%M:%S"),
    
        if c == 0:
            if os.path.isfile(localtile1):
                print "localtile1 already exists.  Run this again later when there is no conflicting job on this node"
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
    
    if status == 0:
        ####  Write to Compressed file
        if os.path.isfile(localtile1):
            if gtiff_compression == 'lzw':
                compress_option = '-co "compress=lzw"'
            elif gtiff_compression == 'jpeg95':
                compress_option =  '-co "compress=jpeg" -co "jpeg_quality=95"'
                
            cmd = 'gdal_translate -stats -of GTiff %s -co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "BIGTIFF=IF_SAFER" "%s" "%s"' %(compress_option,localtile1,localtile2)
            ExecCmd(cmd)
        
        ####  Build Pyramids
        tm = datetime.today()
        print tm.strftime("%d-%b-%Y %H:%M:%S"),
        
        if os.path.isfile(localtile2):
            cmd = 'gdaladdo "%s" 2 4 8 16 30' %(localtile2)
            ExecCmd(cmd)
        
        #### Copy tile to destination
        if os.path.isfile(localtile2):
            print "Copying output files to destination dir"
            copyall(localtile2,os.path.dirname(tile))
            #copyall(localtile1,os.path.dirname(tile))
        del_images.append(localtile1)
        del_images.append(localtile2)
    
    #### Delete temp files
    deleteTempFiles(del_images)
    os.rmdir(wd)
   

if __name__ == '__main__':
    main()