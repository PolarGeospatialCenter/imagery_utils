import os, string, sys, shutil, glob, re, tarfile, logging, argparse
from datetime import *

from subprocess import *
from math import *
from xml.etree import cElementTree as ET

from lib.mosaic import *
import gdal, ogr, osr, gdalconst
import numpy

### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

default_qsub_script = "qsub_mosaic.sh"
default_logfile = "mosaic.log"


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
    parser.add_argument("--qsubscript",
                        help="qsub script to use in cluster job submission (default is <script_dir>/%s)" %default_qsub_script)
    parser.add_argument("-l",
                        help="PBS resources requested (mimicks qsub syntax)")
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
    
    #### build args list to pass to builder scripts
    #### Get -l args and make a var
    l = ("-l %s" %args.l) if args.l is not None else ""

    args_dict = vars(args)
    arg_list = []
    arg_keys_to_remove = ('l','qsubscript','log','gtiff_compression','mode')
    
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
    
    
    #### Configure Logger
    if args.log is not None:
        logfile = os.path.abspath(args.log)
    else:
        logfile = os.path.join(mosaic_dir,default_logfile)
    
    lfh = logging.FileHandler(logfile)
    #lfh = logging.StreamHandler()
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)
    
    #### Get exclude list if specified
    if args.exclude is not None:
        if not os.path.isfile(args.exclude):
            parser.error("Value for option --exclude-list is not a valid file")
        
        f = open(args.exclude, 'r')
        exclude_list = set([line.rstrip() for line in f.readlines()])
    else:
        exclude_list = set()

    logger.info("Reading input images, checking that they conform, and calculating data geometry")
    xs = []
    ys = []
    
    #### Get Images
    image_list = FindImages(inpath,bTextfile,exclude_list)
        
    if len(image_list) == 0:
        logger.error("No images found in input file or directory: %s" %inpath)
        sys.exit()
    else:
        logger.info("%i existing images found" %len(image_list))
    
    #### gather image info list
    imginfo_list = [ImageInfo(image,"warped",logger) for image in image_list]
    
    #### Get mosaic parameters
    params = getMosaicParameters(imginfo_list[0],args)
    
    #### Remove images that do not match ref
    logger.info("Applying attribute filter")
    imginfo_list2 = filterMatchingImages(imginfo_list,params,logger)
    
    if len(imginfo_list2) == 0:
        logger.error("No valid images found.  Check input filter parameters.")
        sys.exit()
    else:
        logger.info("%i images match filter" %len(imginfo_list2))
    
    #### Get geom for each image
    
    imginfo_list3 = []
    for iinfo in imginfo_list2:
        iinfo.geom, xs1, ys1 = getGeom(iinfo.srcfp)
        if iinfo.geom is not None:
            xs = xs + xs1
            ys = ys + ys1
            imginfo_list3.append(iinfo)
        else: # remove from list if no geom
            logger.warning("Cannot get geometry for image: %s" %iinfo.srcfp)
    
    #### set extent if not already set
    if args.extent is None:
        params.xmin = min(xs)
        params.xmax = max(xs)
        params.ymin = min(ys)
        params.ymax = max(ys)
    
    poly_wkt = 'POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))' %(params.xmin,params.ymin,params.xmin,params.ymax,params.xmax,params.ymax,params.xmax,params.ymin,params.xmin,params.ymin)
    params.extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)
        
    logger.info("Resolution: %f x %f, Tilesize: %f x %f, Extent: %f %f %f %f" %(params.xres,params.yres,params.xtilesize,params.ytilesize,params.xmin,params.xmax,params.ymin,params.ymax))
    
    #### Check number of remaining images
    num_images = len(imginfo_list3)

    if num_images > 0:            
        logger.info("%d of %d input images images are valid" %(num_images,len(image_list)))
    else:
        logger.error("No valid images found")
        sys.exit(0)
    
    
    #####################################################
    ####  Read xmls and order imagery (cloud cover, off-nadir angle, sun elev, acq date, exposure/TDI? )
    ######################################################
    
    logger.info("Reading image metadata and determining sort order")
         
    for iinfo in imginfo_list3:
        iinfo.score, iinfo.attribs = iinfo.getScore(params,logger)
            
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
                logger.warning("Image has an invalid score: %s --> %i" %(iinfo.srcfp, iinfo.score))
        else:
            logger.warning("Image does not intersect mosaic extent: %s" %iinfo.srcfp)
    
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
    logger.info("Tiles: %d x %d" %(xtiledim,ytiledim))
    
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
            logger.info("Tiles shapefile already exists: %s" %shp)
        else:
            logger.info("Creating shapefile of tiles: %s" %shp)
        
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
                feat.SetField("XMIN",t.minx)
                feat.SetField("XMAX",t.maxx)
                feat.SetField("YMIN",t.miny)
                feat.SetField("YMAX",t.maxy)
                    
                
                feat.SetGeometry(t.geom)
                
                if lyr.CreateFeature(feat) != 0:
                    logger.error("ERROR: Could not create feature for tile %s" % tile)
                    
                feat.Destroy()
                
                
    ###############################################   
    ####  Write shapefile of mosaic components
    ###############################################
    if args.component_shp is True:
        
        comp_shp = mosaic + "_components.shp"
    
        if os.path.isfile(comp_shp):
            logger.info("Components shapefile already exists: %s" %comp_shp)
        else:
            
            logger.info("Creating shapefile of components: %s" %comp_shp)
        
            if args.extent:
                 cmd = r'qsub -N Cutlines -v p1="%s --cutline-step=512 %s %s %s" "%s"' %(cutline_builder_script,arg_str,comp_shp,aitpath,qsubpath)
            else:
                cmd = r'qsub -N Cutlines -v p1="%s --cutline-step=512 %s -e %f %f %f %f %s %s" "%s"' %(cutline_builder_script,arg_str,params.xmin,params.xmax,params.ymin,params.ymax,comp_shp,aitpath,qsubpath)
            logger.debug(cmd)
            if args.mode == "ALL" or args.mode == "SHP":
                p = Popen(cmd,shell=True)
                p.wait()  
    
    
    ###############################################   
    ####  Write shapefile of image cutlines
    ###############################################
    shp = mosaic + "_cutlines.shp"
    
    if os.path.isfile(shp):
        logger.info("Cutlines shapefile already exists: %s" %shp)
    else:
        logger.info("Creating shapefile of cutlines: %s" %shp)
        
        arg_str2 = arg_str.replace("--component_shp","")
        if args.extent:
            cmd = r'qsub -N Cutlines -v p1="%s %s %s %s" "%s"' %(cutline_builder_script,arg_str2,shp,aitpath,qsubpath)
        else:
            cmd = r'qsub -N Cutlines -v p1="%s %s -e %f %f %f %f %s %s" "%s"' %(cutline_builder_script,arg_str2,params.xmin,params.xmax,params.ymin,params.ymax,shp,aitpath,qsubpath)
        logger.debug(cmd)
        if args.mode == "ALL" or args.mode == "SHP":
            p = Popen(cmd,shell=True)
            p.wait()
      
      
    ################################################
    ####  For each tile set up mosaic call to qsub
    ################################################
    
    for t in tiles:
        logger.info("Processing tile %d of %d: %s" %(i,num_tiles,t.name))
        
        ####    determine which images in each tile - create geom and query image geoms
        logger.info("Running intersect with imagery")       
        
        intersects = []
        for iinfo in intersects_all:
            if t.geom.Intersect(iinfo.geom) is True:
                if iinfo.score > 0:
                    logger.info("intersects! %s - score %f" %(iinfo.srcfn,iinfo.score))
                    intersects.append(iinfo.srcfp)
                elif args.nosort:
                    logger.info("intersects! %s - score %f" %(iinfo.srcfn,iinfo.score))
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
            logger.info("Submitting mosaicking job for tile: %s" %os.path.basename(t.name))
            if os.path.isfile(t.name) is False:
                                
                cmd = r'qsub -N Mosaic%04i -v p1="%s %s %s %s %s %f %f %f %f %f %f %s" "%s"' %(i,tile_builder_script,params.bands,itpath,t.name,int(params.force_pan_to_multi),params.xres,params.yres,t.minx,t.miny,t.maxx,t.maxy,args.gtiff_compression,qsubpath)
                logger.debug(cmd)
                if args.mode == "ALL" or args.mode == "MOSAIC":
                    p = Popen(cmd,shell=True)
                    p.wait()
                
            else:
                logger.info("Tile already exists: %s" %t.name)
            
            j += 1
        
        i += 1
    

def FindImages(inpath,bTextfile,exclude_list):
    
    image_list = []
    
    if bTextfile is True:
        t = open(inpath,'r')
        for line in t.readlines():
            image = line.rstrip('\n').rstrip('\r')
            if os.path.isfile(image) and os.path.splitext(image)[1].lower() in EXTS:
                image_list.append(image)
            else:
                logger.warning("File in textfile does not exist or has an invalid extension: %s" %image)
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
        p = re.compile("(?P<sceneid>(?:QB02|WV01|WV02|GE01|IK01)_[\w_-]+)_u\d{2}\w{2}\d+.tif")
        
        image_list2 = []
        for image in image_list:
            m = p.search(os.path.basename(image))
            if not m:
                logger.warning("Cannot get scene ID from image name: %s" %os.path.basename(image))
            elif m.group("sceneid") in exclude_list:
                logger.warning("Scene ID is in exclude_list: %s" %image)
            else:
                image_list2.append(image)
    
        return image_list2

    else:
        return image_list


if __name__ == '__main__':
    main()