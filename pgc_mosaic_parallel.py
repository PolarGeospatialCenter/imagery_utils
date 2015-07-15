import os, string, sys, shutil, glob, re, tarfile, logging, argparse
from datetime import *

import subprocess
from math import *
from xml.etree import cElementTree as ET

from lib.mosaic import *
import gdal, ogr, osr, gdalconst
import numpy
import multiprocessing as mp

### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

default_qsub_script = "qsub_mosaic.sh"
default_logfile = "mosaic.log"

SUBMISSION_TYPES = ['HPC','VM']

def main():
    
    #########################################################
    ####  Handle Options
    #########################################################

    #### Set Up Arguments 
    parent_parser = buildMosaicParentArgumentParser()
    parser = argparse.ArgumentParser(
	parents=[parent_parser],
	description="Sumbit mosaic jobs to HPC cluster"
	)
    
    parser.add_argument("src", help="textfile or directory of input rasters (tif only)")
    parser.add_argument("mosaic_name", help="output mosaic name excluding extension")
    pos_arg_keys = ["src","mosaic_name"]

    parser.add_argument("--mode", choices=MODES , default="ALL",
                        help=" mode: ALL- all steps (default), SHP- create shapefiles, MOSAIC- create tiled tifs, TEST- create log only")
    parser.add_argument("--log",
                        help="file to log progress (default is <output dir>\%s" %default_logfile)
    parser.add_argument("--wd",
                        help="scratch space (default is mosaic directory)")
    parser.add_argument("--qsubscript",
                        help="qsub script to use in cluster job submission (default is <script_dir>/%s)" %default_qsub_script)
    parser.add_argument("-l",
                        help="PBS resources requested (mimicks qsub syntax). Use only on HPC systems.")
    parser.add_argument("--processes", type=int,
                        help="number of processes to spawn for bulding subtiles (default is cpu count / 4). Use only on non-HPC runs.")
    parser.add_argument("--submission_type", choices=SUBMISSION_TYPES,
                        help="job submission type. Default is determined automatically (%s)"%string.join(SUBMISSION_TYPES,','))
    parser.add_argument("--component_shp", action="store_true", default=False,
                        help="create shp of all componenet images")
    parser.add_argument("--gtiff_compression", choices=GTIFF_COMPRESSIONS, default="lzw",
                        help="GTiff compression type. Default=lzw (%s)"%string.join(GTIFF_COMPRESSIONS,','))
    
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    inpath = os.path.abspath(args.src)
    mosaic = os.path.abspath(args.mosaic_name)
    mosaic = os.path.splitext(mosaic)[0]
    mosaic_dir = os.path.dirname(mosaic)
    
    if args.qsubscript is None: 
        qsubpath = os.path.join(os.path.dirname(scriptpath),default_qsub_script)
    else:
        qsubpath = os.path.abspath(args.qsubscript)
        
    if not os.path.isfile(qsubpath):
        parser.error("qsub script path is not valid: %s" %qsubpath)
    
    cutline_builder_script = os.path.join(os.path.dirname(scriptpath),'pgc_mosaic_build_cutlines.py')
    tile_builder_script = os.path.join(os.path.dirname(scriptpath),'pgc_mosaic_build_tile.py')
    
    #### Validate Arguments
    if os.path.isfile(inpath):
        bTextfile = True
    elif os.path.isdir(inpath):
        bTextfile = False
    else:
        parser.error("Arg1 is not a valid file path or directory: %s" %inpath)
        
    if not os.path.isdir(mosaic_dir):
        os.makedirs(mosaic_dir)
    if not os.path.isfile(qsubpath):
        parser.error("Arg3 is not a valid file path: %s" %qsubpath)
        
    
    #### Validate target day option
    if args.tday is not None:
        try:
            m = int(args.tday.split("-")[0])
            d = int(args.tday.split("-")[1])
            td = date(2000,m,d)
        except ValueError:
            logger.error("Target day must be in mm-dd format (i.e 04-05)")
            sys.exit(1)
            
    else:
        m = 0
        d = 0
    
        
    #### Get -l args and make a var
    l = ("-l %s" %args.l) if args.l is not None else ""
    
    #### Configure Logger
    if args.log is not None:
        logfile = os.path.abspath(args.log)
    else:
        logfile = os.path.join(mosaic_dir,default_logfile)
    
    lfh = logging.FileHandler(logfile)
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)
    
    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)
    
    
    ####  Determine submission type based on presence of pbsnodes cmd
    try:
        cmd = "pbsnodes"
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        so, se = p.communicate()
    except OSError,e:
        is_hpc = False
    else:
        is_hpc = True
        
    if args.submission_type is None:
        submission_type = "HPC" if is_hpc else "VM"
        
    elif args.submission_type == "HPC" and is_hpc is False:
        parser.error("Submission type HPC is not available on this system")
    else:
        submission_type = args.submission_type
    
    logger.info("Submission type: {0}".format(submission_type))
    task_queue = []
    
    if args.processes and not submission_type == 'VM':
        logger.warning("--processes option will not be used becasue submission type is not VM")
    
    if submission_type == 'VM':
        processes = 1
        if args.processes:
            if mp.cpu_count() < args.processes:
                logger.warning("Specified number of processes ({0}) is higher than the system cpu count ({1}), using default".format(args.proceses,mp.count_cpu()))
                
            elif args.processes < 1:
                logger.warning("Specified number of processes ({0}) must be greater than 0, using default".format(args.proceses,mp.count_cpu()))
                
            else:
                processes = args.processes            
            
        logger.info("Number of child processes to spawn: {0}".format(processes))
    

    #### Get exclude list if specified
    if args.exclude is not None:
        if not os.path.isfile(args.exclude):
            parser.error("Value for option --exclude-list is not a valid file")
        
        f = open(args.exclude, 'r')
        exclude_list = set([line.rstrip() for line in f.readlines()])
    else:
        exclude_list = set()

    
    #### Get Images
    logger.info("Reading input images, checking that they conform, and calculating data geometry")
    xs = []
    ys = []
    
    image_list = FindImages(inpath,bTextfile,exclude_list)
        
    if len(image_list) == 0:
        logger.error("No images found in input file or directory: %s" %inpath)
        sys.exit()
    else:
        logger.info("%i existing images found" %len(image_list))
    
    #### gather image info list
    logger.info("Getting image info")
    imginfo_list = [ImageInfo(image,"IMAGE") for image in image_list]
    
    #### Get mosaic parameters
    logger.info("Setting mosaic parameters")
    params = getMosaicParameters(imginfo_list[0],args)
    
    #### Remove images that do not match ref
    logger.info("Applying attribute filter")
    imginfo_list2 = filterMatchingImages(imginfo_list,params)
    
    if len(imginfo_list2) == 0:
        logger.error("No valid images found.  Check input filter parameters.")
        sys.exit()
    else:
        logger.info("%i images match filter" %len(imginfo_list2))

    #### if extent is specified, build tile params and compare extent to input image geom
    if args.extent:
    
        poly_wkt = 'POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))' %(params.xmin,params.ymin,params.xmin,params.ymax,params.xmax,params.ymax,params.xmax,params.ymin,params.xmin,params.ymin)
        params.extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)
        
        #### Check geom overlaps extent
        imginfo_list3 = []
        for iinfo in imginfo_list2:
            if iinfo.geom is not None:
                if params.extent_geom.Intersect(iinfo.geom) is True:
                    imginfo_list3.append(iinfo)
                else:
                    logger.debug("Image does not intersect mosaic extent: %s" %iinfo.srcfn)
            else: # remove from list if no geom
                logger.debug("Null geometry for image: %s" %iinfo.srcfn)
    
    #### else set extent after image geoms computed
    else:
        
        #### Get geom for each image
        imginfo_list3 = []
        for iinfo in imginfo_list2:
            if iinfo.geom is not None:
                xs = xs + iinfo.xs
                ys = ys + iinfo.ys
                imginfo_list3.append(iinfo)
            else: # remove from list if no geom
                logger.debug("Null geometry for image: %s" %iinfo.srcfn)
        
        params.xmin = min(xs)
        params.xmax = max(xs)
        params.ymin = min(ys)
        params.ymax = max(ys)
    
        poly_wkt = 'POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))' %(params.xmin,params.ymin,params.xmin,params.ymax,params.xmax,params.ymax,params.xmax,params.ymin,params.xmin,params.ymin)
        params.extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)
     
    #### Check number of remaining images
    num_images = len(imginfo_list3)

    if num_images > 0:            
        logger.info("%d of %d input images intersect mosaic extent" %(num_images,len(image_list)))
        logger.info("Mosaic parameters: resolution %f x %f, tilesize %f x %f, extent %f %f %f %f" %(params.xres,params.yres,params.xtilesize,params.ytilesize,params.xmin,params.xmax,params.ymin,params.ymax))
        
    else:
        logger.error("No valid images found")
        sys.exit(0)

    
    #####################################################
    ####  Read xmls and order imagery (cloud cover, off-nadir angle, sun elev, acq date, exposure/TDI? )
    ######################################################
    
    logger.info("Reading image metadata and determining sort order")
         
    for iinfo in imginfo_list3:
        iinfo.getScore(params)
            
    ####  Sort by score
    if not args.nosort:
        imginfo_list3.sort(key=lambda x: x.score)
    
    #### Write all intersects file
    intersects_all = []
    
    for iinfo in imginfo_list3:
        if params.extent_geom.Intersect(iinfo.geom) is True:
            if iinfo.score > 0:
                intersects_all.append(iinfo)
            elif args.nosort:
                intersects_all.append(iinfo)
            else:
                logger.debug("Image has an invalid score: %s --> %i" %(iinfo.srcfp, iinfo.score))
        else:
            logger.debug("Image does not intersect mosaic extent: %s" %iinfo.srcfp)  ### this line should never be needed.  non-intersecting images should be removed earlier if extent is provided, otherwise all images are in the extent.
    
    aitpath = mosaic+"_intersects.txt"
    ait = open(aitpath,"w")
    intersects_fps = [intersect.srcfp for intersect in intersects_all]
    ait.write(string.join(intersects_fps,"\n"))
    ait.close()


    ####################################################
    #### Set Tiles - dict of tile name and tuple of extent geom
    #####################################################
    
    logger.info("Creating tiles")
    tiles = []
    
    xtiledim = ceil((params.xmax-params.xmin)/params.xtilesize)
    ytiledim = ceil((params.ymax-params.ymin)/params.ytilesize)
    logger.info("Tiles: %d rows, %d columns" %(ytiledim,xtiledim))
    
    xtdb = len(str(int(xtiledim)))
    ytdb = len(str(int(ytiledim)))
    
    i = 1   
    for x in drange(params.xmin,params.xmax,params.xtilesize):  # Columns
        if x+params.xtilesize > params.xmax:
            x2 = params.xmax

        else:
            x2 = x+params.xtilesize
            
        j = 1
        for y in drange(params.ymin,params.ymax,params.ytilesize):  # Rows
            if y+params.ytilesize > params.ymax:
                y2 = params.ymax
            else:
                y2 = y+params.ytilesize
                        
            tilename = "%s_%s_%s.tif" %(mosaic,buffernum(j,ytdb),buffernum(i,xtdb))
            tile = TileParams(x,x2,y,y2,j,i,tilename)
            tiles.append(tile)
            
            j += 1
        i += 1
    
    num_tiles = len(tiles)
    i = 1
    j = 0
    
    
    #################################################
    ####  Write shapefile of tiles
    #################################################
    
    if args.mode == "ALL" or args.mode == "SHP":
        
        shp = mosaic + "_tiles.shp"
        
        if os.path.isfile(shp):
            logger.info("Tiles shapefile already exists: %s" %os.path.basename(shp))
        else:
            logger.info("Creating shapefile of tiles: %s" %os.path.basename(shp))
        
            fields = [('ROW', ogr.OFTInteger, 4),
                    ('COL', ogr.OFTInteger, 4),
                    ("TILENAME", ogr.OFTString, 100),
                    ('TILEPATH', ogr.OFTString, 254),
                    ('XMIN', ogr.OFTReal, 0),
                    ('XMAX', ogr.OFTReal, 0),
                    ('YMIN', ogr.OFTReal, 0),
                    ('YMAX', ogr.OFTReal, 0)]
                      
            OGR_DRIVER = "ESRI Shapefile"
            
            ogrDriver = ogr.GetDriverByName(OGR_DRIVER)
            if ogrDriver is None:
                logger.error("OGR: Driver %s is not available" % OGR_DRIVER)
                sys.exit(-1)
        
            if os.path.isfile(shp):
                ogrDriver.DeleteDataSource(shp)
            vds = ogrDriver.CreateDataSource(shp)
            if vds is None:
                logger.error("Could not create shp")
                sys.exit(-1)
            
            shpd, shpn = os.path.split(shp)
            shpbn, shpe = os.path.splitext(shpn)
            
            rp = osr.SpatialReference()
            rp.ImportFromWkt(params.proj)
            
            lyr = vds.CreateLayer(shpbn, rp, ogr.wkbPolygon)
            if lyr is None:
                logger.error("ERROR: Failed to create layer: %s" % shpbn)
                sys.exit(-1)
            
            for fld, fdef, flen in fields:
                field_defn = ogr.FieldDefn(fld, fdef)
                if fdef == ogr.OFTString:
                    field_defn.SetWidth(flen)
                if lyr.CreateField(field_defn) != 0:
                    logger.error("ERROR: Failed to create field: %s" % fld)
            
            for t in tiles:
                
                feat = ogr.Feature(lyr.GetLayerDefn())
                
                feat.SetField("TILENAME",os.path.basename(t.name))
                feat.SetField("TILEPATH",t.name)
                feat.SetField("ROW",t.j)
                feat.SetField("COL",t.i)
                feat.SetField("XMIN",t.xmin)
                feat.SetField("XMAX",t.xmax)
                feat.SetField("YMIN",t.ymin)
                feat.SetField("YMAX",t.ymax)
                    
                
                feat.SetGeometry(t.geom)
                
                if lyr.CreateFeature(feat) != 0:
                    logger.error("ERROR: Could not create feature for tile %s" % tile)
                    
                feat.Destroy()
                
                
    ###############################################   
    ####  Write shapefile of mosaic components
    ###############################################
    
    if args.component_shp is True:
        
        arg_keys_to_remove = ('l','qsubscript','processes','log','gtiff_compression','mode','extent','resolution','submission_type','wd')
        shp_arg_str = build_arg_list(args, pos_arg_keys, arg_keys_to_remove)
        
        comp_shp = mosaic + "_components.shp"
    
        if os.path.isfile(comp_shp):
            logger.info("Components shapefile already exists: %s" %os.path.basename(comp_shp))
        else:
            
            logger.info("Processing components: %s" %os.path.basename(comp_shp))
        
            if submission_type == 'HPC':
                cmd = r'qsub %s -N Cutlines -v p1="%s --cutline_step=512 %s -e %f %f %f %f %s %s" "%s"' %(
                    l,
                    cutline_builder_script,
                    shp_arg_str,
                    params.xmin,
                    params.xmax,
                    params.ymin,
                    params.ymax,
                    comp_shp,
                    aitpath,
                    qsubpath
                    )
            
            elif submission_type == 'VM':
                cmd = r'python %s --cutline_step=512 %s -e %f %f %f %f %s %s' %(
                    cutline_builder_script,
                    shp_arg_str,
                    params.xmin,
                    params.xmax,
                    params.ymin,
                    params.ymax,
                    comp_shp,
                    aitpath
                    )
                
            else:
                cmd = None
            
            logger.debug(cmd)
            if args.mode == "ALL" or args.mode == "SHP":
                job_name = "Components"
                task_queue.append((job_name,cmd))
    
    
    ###############################################   
    ####  Write shapefile of image cutlines
    ###############################################
    shp = mosaic + "_cutlines.shp"
    
    arg_keys_to_remove = ('l','qsubscript','processes','log','gtiff_compression','mode','extent','resolution','component_shp','submission_type','wd')
    shp_arg_str = build_arg_list(args, pos_arg_keys, arg_keys_to_remove)
    
    if os.path.isfile(shp):
        logger.info("Cutlines shapefile already exists: %s" %os.path.basename(shp))
    else:
        logger.info("Processing cutlines: %s" %os.path.basename(shp))
        
        if submission_type == 'HPC':
            cmd = r'qsub %s -N Cutlines -v p1="%s %s -e %f %f %f %f %s %s" "%s"' %(
                l,
                cutline_builder_script,
                shp_arg_str,
                params.xmin,
                params.xmax,
                params.ymin,
                params.ymax,
                shp,
                aitpath,
                qsubpath
                )
        
        elif submission_type == 'VM':
            cmd = r'python %s %s -e %f %f %f %f %s %s' %(
                cutline_builder_script,
                shp_arg_str,
                params.xmin,
                params.xmax,
                params.ymin,
                params.ymax,
                shp,
                aitpath
                )
        else:
            cmd = None
        
        logger.debug(cmd)
        if args.mode == "ALL" or args.mode == "SHP":
            job_name = "Cutlines"
            task_queue.append((job_name,cmd))
                
      
      
    ################################################
    ####  For each tile set up mosaic call to qsub
    ################################################
    
    arg_keys_to_remove = ('l','qsubscript','processes','log','mode','extent','resolution','bands','component_shp','submission_type')
    tile_arg_str = build_arg_list(args, pos_arg_keys, arg_keys_to_remove)
    logger.debug("Identifying components of {0} subtiles".format(num_tiles))
    for t in tiles:
        logger.debug("Identifying components of tile %d of %d: %s" %(i,num_tiles,os.path.basename(t.name)))
        
        ####    determine which images in each tile - create geom and query image geoms
        logger.debug("Running intersect with imagery")       
        
        intersects = []
        for iinfo in intersects_all:
            if t.geom.Intersect(iinfo.geom) is True:
                if iinfo.score > 0:
                    logger.debug("intersects! %s - score %f" %(iinfo.srcfn,iinfo.score))
                    intersects.append(iinfo.srcfp)
                elif args.nosort:
                    logger.debug("intersects! %s - score %f" %(iinfo.srcfn,iinfo.score))
                    intersects.append(iinfo.srcfp)
                else:
                    logger.warning("Image has an invalid score: %s --> %i" %(iinfo.srcfp, iinfo.score))
        
        ####  If any images are in the tile, mosaic them        
        if len(intersects) > 0:
            
            tile_basename = os.path.basename(os.path.splitext(t.name)[0])                                    
            itpath = os.path.join(mosaic_dir,tile_basename+"_intersects.txt")
            it = open(itpath,"w")
            it.write(string.join(intersects,"\n"))
            it.close()
            
            #### Submit QSUB job
            logger.debug("Building mosaicking job for tile: %s" %os.path.basename(t.name))
            if os.path.isfile(t.name) is False:
                
                if submission_type == 'HPC':
                    cmd = r'qsub %s -N Mosaic%04i -v p1="%s %s -e %f %f %f %f -r %s %s -b %d %s %s" "%s"' %(
                        l, i,
                        tile_builder_script,
                        tile_arg_str,
                        t.xmin,
                        t.xmax,
                        t.ymin,
                        t.ymax,
                        params.xres,
                        params.yres,
                        params.bands,
                        t.name,
                        itpath,
                        qsubpath
                        )
                       
                elif submission_type == 'VM':
                    cmd = r'python %s %s -e %f %f %f %f -r %s %s -b %d %s %s' %(
                        tile_builder_script,
                        tile_arg_str,
                        t.xmin,
                        t.xmax,
                        t.ymin,
                        t.ymax,
                        params.xres,
                        params.yres,
                        params.bands,
                        t.name,
                        itpath,
                        )
                    
                else:
                    cmd = None
                    
                    
                logger.debug(cmd)    
                if args.mode == "ALL" or args.mode == "MOSAIC":
                    job_name = "Tile {0}".format(os.path.basename(t.name))
                    task_queue.append((job_name,cmd))
                
            else:
                logger.info("Tile already exists: %s" %os.path.basename(t.name))
            
            j += 1
        i += 1
    
    logger.info("Submitting jobs")
    #logger.info(task_queue)
    if submission_type == 'HPC':
        for task in task_queue:
            job_name,cmd = task
            subprocess.call(cmd,shell=True)
        
    elif submission_type == 'VM':
        pool = mp.Pool(processes)
        try:
            pool.map(ExecCmd_mp,task_queue,1)
        except KeyboardInterrupt:
            pool.terminate()
            logger.info("Processes terminated without file cleanup")
        else:
            logger.info("Done")
        
        
    

def FindImages(inpath,bTextfile,exclude_list):
    
    image_list = []
    
    if bTextfile is True:
        t = open(inpath,'r')
        for line in t.readlines():
            image = line.rstrip('\n').rstrip('\r')
            if os.path.isfile(image) and os.path.splitext(image)[1].lower() in EXTS:
                image_list.append(image)
            else:
                logger.debug("File in textfile does not exist or has an invalid extension: %s" %image)
        t.close()
                
    else:
        for root,dirs,files in os.walk(inpath):
            for f in  files:
                if os.path.splitext(f)[1].lower() in EXTS:
                    image_path = os.path.join(root,f)
                    image_path = string.replace(image_path,'\\','/')
                    image_list.append(image_path)
    
    #print len(exclude_list)
    if len(exclude_list) > 0:
        
        image_list2 = []
        for image in image_list:
            include=True
            for pattern in exclude_list:
                if pattern in image:
                    include=False
            
            if include==False:
                logger.debug("Scene ID is matches pattern in exclude_list: %s" %image)
            else:
                image_list2.append(image)
    
        return image_list2

    else:
        return image_list


if __name__ == '__main__':
    main()