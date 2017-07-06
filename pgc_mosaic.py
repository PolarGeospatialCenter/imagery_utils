import os, string, sys, shutil, glob, re, tarfile, logging, argparse, subprocess, math
from datetime import datetime, timedelta
from xml.etree import cElementTree as ET
import gdal, ogr, osr, gdalconst
import numpy

from lib import mosaic, utils, taskhandler

### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

default_qsub_script = "qsub_mosaic.sh"
default_logfile = "mosaic.log"

def main():

    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="Sumbit/run batch mosaic tasks"
    )
    
    parser.add_argument("src", help="textfile or directory of input rasters (tif only)")
    parser.add_argument("mosaicname", help="output mosaic name excluding extension")
    pos_arg_keys = ["src","mosaicname"]

    parser.add_argument("-r", "--resolution", nargs=2, type=float,
                        help="output pixel resolution -- xres yres (default is same as first input file)")
    parser.add_argument("-e", "--extent", nargs=4, type=float,
                        help="extent of output mosaic -- xmin xmax ymin ymax (default is union of all inputs)")
    parser.add_argument("-t", "--tilesize", nargs=2, type=float,
                        help="tile size in coordinate system units -- xsize ysize (default is 40,000 times output resolution)")
    parser.add_argument("--force-pan-to-multi", action="store_true", default=False,
                        help="if output is multiband, force script to also use 1 band images")
    parser.add_argument("-b", "--bands", type=int,
                        help="number of output bands( default is number of bands in the first image)")
    parser.add_argument("--tday",
                        help="month and day of the year to use as target for image suitability ranking -- 04-05")
    parser.add_argument("--nosort", action="store_true", default=False,
                        help="do not sort images by metadata. script uses the order of the input textfile or directory (first image is first drawn).  Not recommended if input is a directory; order will be random")
    parser.add_argument("--use-exposure", action="store_true", default=False,
                        help="use exposure settings in metadata to inform score")
    parser.add_argument("--exclude",
                        help="file of file name patterns (text only, no wildcards or regexs) to exclude")
    parser.add_argument("--max-cc", type=float, default=0.5,
                        help="maximum fractional cloud cover (0.0-1.0, default 0.5)")
    parser.add_argument("--include-all-ms", action="store_true", default=False,
                        help="include all multispectral imagery, even if the imagery has differing numbers of bands")
    parser.add_argument("--min-contribution-area", type=int, default=20000000,
                      help="minimum area contribution threshold in target projection units (default=20000000). Higher values remove more image slivers from the resulting mosaic") 
    parser.add_argument("--median-remove", action="store_true", default=False,
                        help="subtract the median from each input image before forming the mosaic in order to correct for contrast")
    parser.add_argument("--mode", choices=mosaic.MODES , default="ALL",
                        help=" mode: ALL- all steps (default), SHP- create shapefiles, MOSAIC- create tiled tifs, TEST- create log only")
    parser.add_argument("--wd",
                        help="scratch space (default is mosaic directory)")
    parser.add_argument("--component-shp", action="store_true", default=False,
                        help="create shp of all componenet images")
    parser.add_argument("--cutline-step", type=int, default=2,
                       help="cutline calculator pixel skip interval (default=2)")
    parser.add_argument("--calc-stats", action="store_true", default=False,
                       help="calculate image stats and record them in the index")
    parser.add_argument("--gtiff-compression", choices=mosaic.GTIFF_COMPRESSIONS, default="lzw",
                        help="GTiff compression type. Default=lzw (%s)"%(",".join(mosaic.GTIFF_COMPRESSIONS)))
    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--slurm", action='store_true', default=False,
                        help="submit tasks to SLURM")
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="submission script to use in PBS/SLURM submission (PBS default is qsub_mosaic.sh, SLURM default is slurm_mosaic.py, in script root folder)")
    parser.add_argument("-l",
                        help="PBS resources requested (mimicks qsub syntax). Use only on HPC systems.")
    parser.add_argument("--log",
                        help="file to log progress (default is <output dir>\%s" %default_logfile)
    
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    inpath = os.path.abspath(args.src)
    mosaicname = os.path.abspath(args.mosaicname)
    mosaicname = os.path.splitext(mosaicname)[0]
    mosaic_dir = os.path.dirname(mosaicname)
    cutline_builder_script = os.path.join(os.path.dirname(scriptpath),'pgc_mosaic_build_cutlines.py')
    tile_builder_script = os.path.join(os.path.dirname(scriptpath),'pgc_mosaic_build_tile.py')
    
    ## Verify qsubscript
    if args.pbs or args.slurm:
        if args.qsubscript is None:
            if args.pbs:
                qsubpath = os.path.join(os.path.dirname(scriptpath),'qsub_mosaic.sh')
            if args.slurm:
                qsubpath = os.path.join(os.path.dirname(scriptpath),'slurm_mosaic.sh')
        else:
            qsubpath = os.path.abspath(args.qsubscript)
        if not os.path.isfile(qsubpath):
            parser.error("qsub script path is not valid: %s" %qsubpath)
        
    ## Verify processing options do not conflict
    if args.pbs and args.slurm:
        parser.error("Options --pbs and --slurm are mutually exclusive")
    if (args.pbs or args.slurm) and args.parallel_processes > 1:
        parser.error("HPC Options (--pbs or --slurm) and --parallel-processes > 1 are mutually exclusive")

    #### Validate Arguments
    if os.path.isfile(inpath):
        bTextfile = True
    elif os.path.isdir(inpath):
        bTextfile = False
    else:
        parser.error("Arg1 is not a valid file path or directory: %s" %inpath)    
    if not os.path.isdir(mosaic_dir):
        os.makedirs(mosaic_dir)
    
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
  
    #### Get exclude list if specified
    if args.exclude is not None:
        if not os.path.isfile(args.exclude):
            parser.error("Value for option --exclude-list is not a valid file")
        
        f = open(args.exclude, 'r')
        exclude_list = set([line.rstrip() for line in f.readlines()])
    else:
        exclude_list = set()

    #### Get Images
    #logger.info("Reading input images")
    xs = []
    ys = []
    
    image_list = utils.find_images_with_exclude_list(inpath, bTextfile, mosaic.EXTS, exclude_list)
        
    if len(image_list) == 0:
        logger.error("No images found in input file or directory: %s" %inpath)
        sys.exit()
    else:
        logger.info("%i existing images found" %len(image_list))
    
    #### gather image info list
    logger.info("Getting image info")
    imginfo_list = [mosaic.ImageInfo(image,"IMAGE") for image in image_list]
    
    #### Get mosaic parameters
    logger.info("Setting mosaic parameters")
    params = mosaic.getMosaicParameters(imginfo_list[0],args)
    logger.info("Mosaic Params: band count={}, datatype={}".format(params.bands,params.datatype))
    logger.info("Mosaic Params: projection={}".format(params.proj))
     
    #### Remove images that do not match ref
    logger.info("Applying attribute filter")
    imginfo_list2 = mosaic.filterMatchingImages(imginfo_list,params)
    
    if len(imginfo_list2) == 0:
        logger.error("No valid images found.  Check input filter parameters.")
        sys.exit()
    else:
        logger.info("%i images match filter" %len(imginfo_list2))

    #### if extent is specified, build tile params and compare extent to input image geom
    if args.extent:
        imginfo_list3 = mosaic.filter_images_by_geometry(imginfo_list2, params)
    
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

    ## Sort images by score
    logger.info("Reading image metadata and determining sort order")
    for iinfo in imginfo_list3:
        iinfo.getScore(params)
       
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
    
    aitpath = mosaicname+"_intersects.txt"
    ait = open(aitpath,"w")
    intersects_fps = [intersect.srcfp for intersect in intersects_all]
    ait.write(string.join(intersects_fps,"\n"))
    ait.close()

    ## Create tiles
    logger.info("Creating tiles")
    tiles = []
    
    xtiledim = math.ceil((params.xmax-params.xmin)/params.xtilesize)
    ytiledim = math.ceil((params.ymax-params.ymin)/params.ytilesize)
    logger.info("Tiles: %d rows, %d columns" %(ytiledim,xtiledim))
    
    xtdb = len(str(int(xtiledim)))
    ytdb = len(str(int(ytiledim)))
    
    i = 1   
    for x in mosaic.drange(params.xmin,params.xmax,params.xtilesize):  # Columns
        if x+params.xtilesize > params.xmax:
            x2 = params.xmax
        else:
            x2 = x+params.xtilesize
      
        j = 1
        for y in mosaic.drange(params.ymin,params.ymax,params.ytilesize):  # Rows
            if y+params.ytilesize > params.ymax:
                y2 = params.ymax
            else:
                y2 = y+params.ytilesize
                        
            tilename = "%s_%s_%s.tif" %(mosaicname,mosaic.buffernum(j,ytdb),mosaic.buffernum(i,xtdb))
            tile = mosaic.TileParams(x,x2,y,y2,j,i,tilename)
            tiles.append(tile)
            j += 1
        i += 1
    num_tiles = len(tiles)
       
    ####  Write shapefile of tiles
    if args.mode == "ALL" or args.mode == "SHP":
        
        shp = mosaicname + "_tiles.shp"
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
     
    ## Build tasks
    task_queue = []
            
    ####  Create task for shapefile of image cutlines/components
    shp = mosaicname + "_cutlines.shp"
    
    arg_keys_to_remove = (
        'l',
        'wd',
        'qsubscript',
        'parallel_processes',
        'log',
        'gtiff_compression',
        'mode',
        'extent',
        'tilesize',
        'exclude',
        'nosort',
        'pbs',
        'slurm'
    )
    shp_arg_str = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    if os.path.isfile(shp):
        logger.info("Cutlines shapefile already exists: %s" %os.path.basename(shp))
    else:
        logger.info("Processing cutlines: %s" %os.path.basename(shp))
        
        ## Make task and add to queue
        cmd = '{} {} -e {} {} {} {} {} {}'.format(
            cutline_builder_script,
            shp_arg_str,
            params.xmin,
            params.xmax,
            params.ymin,
            params.ymax,
            shp,
            aitpath
        )
        
        task = taskhandler.Task(
            'Cutlines',
            'Cutlines',
            'python',
            cmd
        )
        
        if args.mode == "ALL" or args.mode == "SHP":
            logger.debug(cmd)
            task_queue.append(task)
   
    ####  Create task for each tile
    arg_keys_to_remove = (
        'l',
        'qsubscript',
        'parallel_processes',
        'log',
        'mode',
        'extent',
        'resolution',
        'bands',
        'max_cc',
        'exclude',
        'nosort',
        'component_shp',
        'cutline_step',
        'min_contribution_area',
        'pbs',
        'slurm'
    )
    tile_arg_str = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    logger.debug("Identifying components of {0} subtiles".format(num_tiles))
    i = 0
    for t in tiles:
        logger.debug("Identifying components of tile %d of %d: %s" %(i,num_tiles,os.path.basename(t.name)))
        
        ####    determine which images in each tile - create geom and query image geoms
        logger.debug("Running intersect with imagery")       
        
        intersects = []
        for iinfo in intersects_all:
            if t.geom.Intersect(iinfo.geom) is True:
                if iinfo.score > 0:
                    logger.debug("intersects tile: %s - score %f" %(iinfo.srcfn,iinfo.score))
                    intersects.append(iinfo.srcfp)
                elif args.nosort:
                    logger.debug("intersects tile: %s - score %f" %(iinfo.srcfn,iinfo.score))
                    intersects.append(iinfo.srcfp)
                else:
                    logger.warning("Invalid score: %s --> %i" %(iinfo.srcfp, iinfo.score))
        
        ####  If any images are in the tile, mosaic them        
        if len(intersects) > 0:
            
            tile_basename = os.path.basename(os.path.splitext(t.name)[0])                                    
            itpath = os.path.join(mosaic_dir,tile_basename+"_intersects.txt")
            it = open(itpath,"w")
            it.write(string.join(intersects,"\n"))
            it.close()
            
            #### Submit QSUB job
            logger.debug("Building mosaicking job for tile: %s" %os.path.basename(t.name))
            if not os.path.isfile(t.name):
                
                cmd = r'{} {} -e {} {} {} {} -r {} {} -b {} {} {}'.format(
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
                    itpath
                )
                
                task = taskhandler.Task(
                    'Tile {0}'.format(os.path.basename(t.name)),
                    'Mos{:04g}'.format(i),
                    'python',
                    cmd
                )
                    
                if args.mode == "ALL" or args.mode == "MOSAIC":
                    logger.debug(cmd)
                    task_queue.append(task)
                
            else:
                logger.info("Tile already exists: %s" %os.path.basename(t.name))
        i += 1
    
    logger.info("Submitting Tasks")
    #logger.info(task_queue)
    if len(task_queue) > 0:
        if args.pbs:
            if args.l:
                l = "-l {}".format(args.l)
            else:
                l = ""
            try:
                task_handler = taskhandler.PBSTaskHandler(qsubpath, l)
            except RuntimeError, e:
                logger.error(e)
            else:
                task_handler.run_tasks(task_queue)
                
        elif args.slurm:
            try:
                task_handler = taskhandler.SLURMTaskHandler(qsubpath)
            except RuntimeError, e:
                logger.error(e)
            else:
                task_handler.run_tasks(task_queue)
            
        else:
            try:
                task_handler = taskhandler.ParallelTaskHandler(args.parallel_processes)
            except RuntimeError, e:
                logger.error(e)
            else:
                if task_handler.num_processes > 1:
                    logger.info("Number of child processes to spawn: {0}".format(task_handler.num_processes))
                task_handler.run_tasks(task_queue)
            
        logger.info("Done")
        
    else:
        logger.info("No tasks to process")

if __name__ == '__main__':
    main()