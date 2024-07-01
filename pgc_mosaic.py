#!/usr/bin/env python

import argparse
import logging
import math
import os
import sys
from datetime import date, datetime

from osgeo import ogr, osr

from lib import mosaic, taskhandler, utils
from lib import VERSION


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
    pos_arg_keys = ["src", "mosaicname"]

    parser.add_argument("-r", "--resolution", nargs=2, type=float,
                        help="output pixel resolution -- xres yres (default is same as first input file)")
    parser.add_argument("-e", "--extent", nargs=4, type=float,
                        help="extent of output mosaic -- xmin xmax ymin ymax (default is union of all inputs)")
    parser.add_argument("-t", "--tilesize", nargs=2, type=float,
                        help="tile size in coordinate system units -- xsize ysize (default is 40,000 times output "
                             "resolution)")
    parser.add_argument("--force-pan-to-multi", action="store_true", default=False,
                        help="if output is multiband, force script to also use 1 band images")
    parser.add_argument("-b", "--bands", type=int,
                        help="number of output bands( default is number of bands in the first image)")
    parser.add_argument("--tday",
                        help="month and day of the year to use as target for image suitability ranking -- 04-05")
    parser.add_argument("--tyear",
                        help="year (or year range) to use as target for image suitability ranking -- 2017 or 2015-2017")
    parser.add_argument("--nosort", action="store_true", default=False,
                        help="do not sort images by metadata. script uses the order of the input textfile or directory "
                             "(first image is first drawn).  Not recommended if input is a directory; order will be "
                             "random")
    parser.add_argument("--use-exposure", action="store_true", default=False,
                        help="use exposure settings in metadata to inform score")
    parser.add_argument("--exclude",
                        help="file of file name patterns (text only, no wildcards or regexs) to exclude")
    parser.add_argument("--max-cc", type=float, default=0.2,
                        help="maximum fractional cloud cover (0.0-1.0, default 0.5)")
    parser.add_argument("--include-all-ms", action="store_true", default=False,
                        help="include all multispectral imagery, even if the imagery has differing numbers of bands")
    parser.add_argument("--min-contribution-area", type=int, default=20000000,
                        help="minimum area contribution threshold in target projection units (default=20000000). "
                             "Higher values remove more image slivers from the resulting mosaic")
    parser.add_argument("--median-remove", action="store_true", default=False,
                        help="subtract the median from each input image before forming the mosaic in order to correct "
                             "for contrast")
    parser.add_argument("--allow-invalid-geom", action="store_true", default=False,
                        help="normally, if 1 or more images has a invalid geometry, a tile will not be created. this "
                             "option will attempt to create a mosaic with the remaining valid geometries, if any.")
    parser.add_argument("--mode", choices=mosaic.MODES, default="ALL",
                        help=" mode: ALL- all steps (default), SHP- create shapefiles, MOSAIC- create tiled tifs, "
                             "TEST- create log only")
    parser.add_argument("--wd",
                        help="scratch space (default is mosaic directory)")
    parser.add_argument("--component-shp", action="store_true", default=False,
                        help="create shp of all componenet images")
    parser.add_argument("--cutline-step", type=int, default=2,
                        help="cutline calculator pixel skip interval (default=2)")
    parser.add_argument("--calc-stats", action="store_true", default=False,
                        help="calculate image stats and record them in the index")
    parser.add_argument("--gtiff-compression", choices=mosaic.GTIFF_COMPRESSIONS, default="lzw",
                        help="GTiff compression type. Default=lzw ({})".format(",".join(mosaic.GTIFF_COMPRESSIONS)))
    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--slurm", action='store_true', default=False,
                        help="submit tasks to SLURM")
    parser.add_argument("--slurm-job-name", default=None,
                        help="assign a name to the slurm job for easier job tracking")
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="submission script to use in PBS/SLURM submission (PBS default is qsub_mosaic.sh, SLURM "
                             "default is slurm_mosaic.py, in script root folder)")
    parser.add_argument("-l",
                        help="PBS resources requested (mimicks qsub syntax). Use only on HPC systems.")
    parser.add_argument("--log",
                        help="file to log progress (default is <output dir>\{}".format(default_logfile))
    parser.add_argument("--skip-cmd-txt", action='store_true', default=False,
                        help='Skip writing the txt file containing the input command.')
    parser.add_argument("--version", action='version', version="imagery_utils v{}".format(VERSION))

    
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    inpath = os.path.abspath(args.src)
    mosaicname = os.path.abspath(args.mosaicname)
    mosaicname = os.path.splitext(mosaicname)[0]
    mosaic_dir = os.path.dirname(mosaicname)
    tile_builder_script = os.path.join(os.path.dirname(scriptpath), 'pgc_mosaic_build_tile.py')
    
    ## Verify qsubscript
    if args.pbs or args.slurm:
        if args.qsubscript is None:
            if args.pbs:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'qsub_mosaic.sh')
            if args.slurm:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'slurm_mosaic.sh')
        else:
            qsubpath = os.path.abspath(args.qsubscript)
        if not os.path.isfile(qsubpath):
            parser.error("qsub script path is not valid: {}".format(qsubpath))

    ## Verify processing options do not conflict
    if args.pbs and args.slurm:
        parser.error("Options --pbs and --slurm are mutually exclusive")

    #### Validate Arguments
    if os.path.isfile(inpath):
        bTextfile = True
    elif os.path.isdir(inpath):
        bTextfile = False
    else:
        parser.error("Arg1 is not a valid file path or directory: {}".format(inpath))

    #### Validate target day option
    if args.tday is not None:
        try:
            m = int(args.tday.split("-")[0])
            d = int(args.tday.split("-")[1])
            td = date(2000, m, d)
        except ValueError:
            parser.error("Target day must be in mm-dd format (i.e 04-05)")
            sys.exit(1)
    else:
        m = 0
        d = 0

    #### Validate target year/year range option
    if args.tyear is not None:
        if len(str(args.tyear)) == 4:
            ## ensure single year is valid
            try:
                tyear_test = datetime(year=int(args.tyear), month=1, day=1)
            except ValueError:
                parser.error("Supplied year {0} is not valid".format(args.tyear))
                sys.exit(1)

        elif len(str(args.tyear)) == 9:
            if '-' in args.tyear:
                ## decouple range and build year
                yrs = args.tyear.split('-')
                yrs_range = range(int(yrs[0]), int(yrs[1]) + 1)
                for yy in yrs_range:
                    try:
                        tyear_test = datetime(year=yy, month=1, day=1)
                    except ValueError:
                        parser.error("Supplied year {0} in range {1} is not valid".format(yy, args.tyear))
                        sys.exit(1)

            else:
                parser.error("Supplied year range {0} is not valid; should be like: 2015 OR 2015-2017"
                             .format(args.tyear))
                sys.exit(1)

        else:
            parser.error("Supplied year {0} is not valid, or its format is incorrect; should be 4 digits for single "
                         "year (e.g., 2017), eight digits and dash for range (e.g., 2015-2017)".format(args.tyear))
            sys.exit(1)

    # write input command to text file next to output folder for reference
    command_str = ' '.join(sys.argv)
    logger.info("Running command: {}".format(command_str))
    if not args.skip_cmd_txt:
        utils.write_input_command_txt(command_str,mosaic_dir)
        args.skip_cmd_txt = True

    #### Get exclude list if specified
    if args.exclude is not None:
        if not os.path.isfile(args.exclude):
            parser.error("Value for option --exclude-list is not a valid file")
    
    ## Build tasks
    task_queue = []
            
    ####  Create task for mosaic 
    arg_keys_to_remove = (
        'l',
        'qsubscript',
        'pbs',
        'slurm'
    )
    mos_arg_str = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
     
    cmd = r'{} {} {} {}'.format(
        scriptpath,
        mos_arg_str,
        inpath,
        mosaicname
    )

    # add a custom name to the job
    if not args.slurm_job_name:
        job_name = 'Mos{:04g}'.format(1)
    else:
        job_name = str(args.slurm_job_name)

    task = taskhandler.Task(
        'Mosaic {}'.format(os.path.basename(mosaicname)),
        job_name,
        'python',
        cmd
    )
    
    task_queue.append(task)

    #logger.info(task_queue)
    if len(task_queue) > 0:
        if args.pbs:
            l = "-l {}".format(args.l) if args.l else ""
            try:
                task_handler = taskhandler.PBSTaskHandler(qsubpath, l)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error(e)
            else:
                task_handler.run_tasks(task_queue)
                
        elif args.slurm:
            try:
                task_handler = taskhandler.SLURMTaskHandler(qsubpath)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error(e)
            else:
                task_handler.run_tasks(task_queue)
            
        else:
            try:
                run_mosaic(tile_builder_script, inpath, mosaicname, mosaic_dir, args, pos_arg_keys)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error(e)

    else:
        logger.info("No tasks to process")
        

def run_mosaic(tile_builder_script, inpath, mosaicname, mosaic_dir, args, pos_arg_keys):
    
    if os.path.isfile(inpath):
        bTextfile = True
    elif os.path.isdir(inpath):
        bTextfile = False
    if not os.path.isdir(mosaic_dir):
        os.makedirs(mosaic_dir)
    
    ## TODO: verify logger woks for both interactive and hpc jobs
    #### Configure Logger
    if args.log is not None:
        logfile = os.path.abspath(args.log)
    else:
        logfile = os.path.join(mosaic_dir, default_logfile)
    
    lfh = logging.FileHandler(logfile)
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)
    
    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)
  
    #### Get exclude list if specified
    if args.exclude is not None:
        if not os.path.isfile(args.exclude):
            logger.error("Value for option --exclude-list is not a valid file")
        
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
        raise RuntimeError("No images found in input file or directory: {}".format(inpath))

    # remove duplicate images
    image_list_unique = list(set(image_list))
    dupes = [x for n, x in enumerate(image_list) if x in image_list[:n]]
    if len(dupes) > 0:
        logger.info("Removed %i duplicate image paths", len(dupes))
        logger.debug("Dupes: %s", dupes)
    image_list = image_list_unique

    logger.info("%i existing images found", len(image_list))
    
    #### gather image info list
    logger.info("Getting image info")
    imginfo_list = [mosaic.ImageInfo(image, "IMAGE") for image in image_list]
    
    #### Get mosaic parameters
    logger.info("Setting mosaic parameters")
    params = mosaic.getMosaicParameters(imginfo_list[0], args)
    logger.info("Mosaic parameters: band count=%i, datatype=%s", params.bands, params.datatype)
    logger.info("Mosaic parameters: projection=%s", params.proj)
     
    #### Remove images that do not match ref
    logger.info("Applying attribute filter")
    imginfo_list2 = mosaic.filterMatchingImages(imginfo_list, params)
    
    if len(imginfo_list2) == 0:
        raise RuntimeError("No valid images found.  Check input filter parameters.")

    logger.info("%i of %i images match filter", len(imginfo_list2), len(image_list))

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
                logger.debug("Null geometry for image: %s", iinfo.srcfn)
        
        params.xmin = min(xs)
        params.xmax = max(xs)
        params.ymin = min(ys)
        params.ymax = max(ys)
    
        poly_wkt = 'POLYGON (( {} {}, {} {}, {} {}, {} {}, {} {} ))'.format(params.xmin, params.ymin, params.xmin,
                                                                            params.ymax, params.xmax, params.ymax,
                                                                            params.xmax, params.ymin, params.xmin,
                                                                            params.ymin)
        params.extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)

    if len(imginfo_list3) == 0:
        raise RuntimeError("No images found that intersect mosaic extent")
    
    logger.info("Mosaic parameters: resolution %f x %f, tilesize %f x %f, extent %f %f %f %f", params.xres,
                params.yres, params.xtilesize, params.ytilesize, params.xmin, params.xmax, params.ymin, params.ymax)
    logger.info("%d of %d input images intersect mosaic extent", len(imginfo_list3), len(imginfo_list2))
    
    ## Sort images by score
    logger.info("Reading image metadata and determining sort order")
    for iinfo in imginfo_list3:
        iinfo.getScore(params)
       
    if not args.nosort:
        imginfo_list3.sort(key=lambda x: x.score)
    
    logger.info("Getting Exact Image geometry")
    imginfo_list4 = []
    all_valid = True

    for iinfo in imginfo_list3:
        if iinfo.score > 0 or args.nosort:
            simplify_tolerance = 2.0 * ((params.xres + params.yres) / 2.0) ## 2 * avg(xres, yres), should be 1 for panchromatic mosaics where res = 0.5m
            geom, xs1, ys1 = mosaic.GetExactTrimmedGeom(iinfo.srcfp, step=args.cutline_step, tolerance=simplify_tolerance)
                
            if geom is None:
                logger.warning("%s: geometry could not be determined, verify image is valid", iinfo.srcfn)
                all_valid = False
            elif geom.IsEmpty():
                logger.warning("%s: geometry is empty", iinfo.srcfn)
                all_valid = False
            else:
                iinfo.geom = geom
                tm = datetime.today()
                imginfo_list4.append(iinfo)
                centroid = geom.Centroid()
                logger.info("%s: geometry acquired - centroid: %f, %f", iinfo.srcfn, centroid.GetX(), centroid.GetY())
                #print(geom)
        else:
            logger.debug("Image has an invalid score: %s --> %i", iinfo.srcfp, iinfo.score)

    if not all_valid:
        if not args.allow_invalid_geom:
            raise RuntimeError("Some source images do not have valid geometries.  Cannot proceeed")
        else:
            logger.info("--allow-invalid-geom used; mosaic will be created using %i valid images (%i invalid \
                        images not used.)".format(len(imginfo_list4), len(imginfo_list3)-len(imginfo_list4)))

    # Get stats if needed
    logger.info("Getting image metadata")
    for iinfo in imginfo_list4:
        logger.info(iinfo.srcfn)
        if args.calc_stats or args.median_remove:
            iinfo.get_raster_stats(args.calc_stats, args.median_remove)
    
    # Build componenet index
    if args.component_shp:
        
        if args.mode == "ALL" or args.mode == "SHP":
            contribs = [(iinfo, iinfo.geom) for iinfo in imginfo_list4]
            logger.info("Number of contributors: %d", len(contribs))
            
            logger.info("Building component index")
            comp_shp = mosaicname + "_components.shp"
            if len(contribs) > 0:
                if os.path.isfile(comp_shp):
                    logger.info("Components shapefile already exists: %s", comp_shp)
                else:
                    build_shp(contribs, comp_shp, args, params)
    
            else:
                logger.error("No contributing images")

    # Build cutlines index                    
    ####  Overlay geoms and remove non-contributors
    logger.info("Overlaying images to determine contribution geom")
    contribs = mosaic.determine_contributors(imginfo_list4, params.extent_geom, args.min_contribution_area)
    logger.info("Number of contributors: %d", len(contribs))
    
    if args.mode == "ALL" or args.mode == "SHP":
        logger.info("Building cutlines index")
        shp = mosaicname + "_cutlines.shp"
        if len(contribs) > 0:
            if os.path.isfile(shp):
                logger.info("Cutlines shapefile already exists: %s", shp)
            else:
                build_shp(contribs, shp, args, params)
    
        else:
            logger.error("No contributing images")
     
        
    ## Create tile objects
    tiles = []
    
    xtiledim = math.ceil((params.xmax-params.xmin) / params.xtilesize)
    ytiledim = math.ceil((params.ymax-params.ymin) / params.ytilesize)
    logger.info("Tiles: %d rows, %d columns", ytiledim, xtiledim)
    
    xtdb = len(str(int(xtiledim)))
    ytdb = len(str(int(ytiledim)))
    
    i = 1   
    for x in mosaic.drange(params.xmin, params.xmax, params.xtilesize):  # Columns
        if x + params.xtilesize > params.xmax:
            x2 = params.xmax
        else:
            x2 = x + params.xtilesize
      
        j = 1
        for y in mosaic.drange(params.ymin, params.ymax, params.ytilesize):  # Rows
            if y + params.ytilesize > params.ymax:
                y2 = params.ymax
            else:
                y2 = y + params.ytilesize
                        
            tilename = "{}_{}_{}.tif".format(mosaicname, mosaic.buffernum(j, ytdb), mosaic.buffernum(i, xtdb))
            tile = mosaic.TileParams(x, x2, y, y2, j, i, tilename)
            tiles.append(tile)
            j += 1
        i += 1
      
    ####  Write shapefile of tiles
    if len(tiles) == 0:
        raise RuntimeError("No tile objects created")
    
    if args.mode == "ALL" or args.mode == "SHP":
        build_tiles_shp(mosaicname, tiles, params)
       
    ## Build tile tasks
    task_queue = []
            
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
        'calc_stats',
        'pbs',
        'slurm',
        'tday',
        'tyear',
        'allow_invalid_geom'
    )
    tile_arg_str = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    logger.debug("Identifying components of %i subtiles", len(tiles))
    i = 0
    for t in tiles:
        logger.debug("Identifying components of tile %i of %i: %s", i, len(tiles), os.path.basename(t.name))
        
        ####    determine which images in each tile - create geom and query image geoms
        logger.debug("Running intersect with imagery")       
        
        intersects = []
        for iinfo, contrib_geom in contribs:
            if contrib_geom.Intersects(t.geom):
                if args.median_remove:
                    ## parse median dct into text
                    median_string = ";".join(["{}:{}".format(k, v) for k, v in iinfo.median.items()])
                    intersects.append("{},{}".format(iinfo.srcfp, median_string))
                else:
                    intersects.append(iinfo.srcfp)
                                
        ####  If any images are in the tile, mosaic them
        if len(intersects) > 0:

            tile_basename = os.path.basename(os.path.splitext(t.name)[0])                                    
            logger.info("Number of contributors to subtile %s: %i", tile_basename, len(intersects))
            itpath = os.path.join(mosaic_dir, tile_basename + "_intersects.txt")
            it = open(itpath, "w")
            it.write("\n".join(intersects))
            it.close()
            
            #### Submit QSUB job
            logger.debug("Building mosaicking job for tile: %s", os.path.basename(t.name))
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
                logger.info("Tile already exists: %s", os.path.basename(t.name))
        i += 1

    if args.mode == "ALL" or args.mode == "MOSAIC":
        logger.info("Submitting Tasks")
        #logger.info(task_queue)
        if len(task_queue) > 0:
            
            try:
                task_handler = taskhandler.ParallelTaskHandler(args.parallel_processes)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error(e)
            else:
                if task_handler.num_processes > 1:
                    logger.info("Number of child processes to spawn: %i", task_handler.num_processes)
                task_handler.run_tasks(task_queue)
                
            logger.info("Done")
            
        else:
            logger.info("No tasks to process")
       

def build_shp(contribs, shp, args, params):
    logger.info("Creating shapefile of image boundaries: %s", shp)
    
    fields = (
        ("IMAGENAME", ogr.OFTString, 100),
        ("SENSOR", ogr.OFTString, 10),
        ("ACQDATE", ogr.OFTString, 10),
        ("CAT_ID", ogr.OFTString, 30),
        ("RESOLUTION", ogr.OFTReal, 0),
        ("OFF_NADIR", ogr.OFTReal, 0),
        ("SUN_ELEV", ogr.OFTReal, 0),
        ("SUN_AZ", ogr.OFTReal, 0),
        ("SAT_ELEV", ogr.OFTReal, 0),
        ("SAT_AZ", ogr.OFTReal, 0),
        ("CLOUDCOVER", ogr.OFTReal, 0),
        ("TDI", ogr.OFTReal, 0),
        ("DATE_DIFF", ogr.OFTReal, 0),
        ("SCORE", ogr.OFTReal, 0),
    )
    
    if args.calc_stats is True:
        fields = fields + (
            ("STATS_MIN", ogr.OFTString, 80),
            ("STATS_MAX", ogr.OFTString, 80),
            ("STATS_STD", ogr.OFTString, 80),
            ("STATS_MEAN", ogr.OFTString, 80),
            ("STATS_PXCT", ogr.OFTString, 80)
        )
    
    if params.median_remove is True:
        fields = fields + (
            ("MEDIAN", ogr.OFTString, 80),
        )

    OGR_DRIVER = "ESRI Shapefile"
    
    ogrDriver = ogr.GetDriverByName(OGR_DRIVER)
    if ogrDriver is None:
        logger.info("OGR: Driver %s is not available", OGR_DRIVER)
        sys.exit(-1)
    
    if os.path.isfile(shp):
        ogrDriver.DeleteDataSource(shp)
    vds = ogrDriver.CreateDataSource(shp)
    if vds is None:
        logger.info("Could not create shp")
        sys.exit(-1)
    
    shpd, shpn = os.path.split(shp)
    shpbn, shpe = os.path.splitext(shpn)
    
    rp = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
    rp.ImportFromWkt(params.proj)
    
    lyr = vds.CreateLayer(shpbn, rp, ogr.wkbPolygon)
    if lyr is None:
        logger.info("ERROR: Failed to create layer: %s", shpbn)
        sys.exit(-1)
    
    for fld, fdef, flen in fields:
        field_defn = ogr.FieldDefn(fld, fdef)
        if fdef == ogr.OFTString:
            field_defn.SetWidth(flen)
        if lyr.CreateField(field_defn) != 0:
            logger.info("ERROR: Failed to create field: %s", fld)
    
    for iinfo, geom in contribs:
                
        feat = ogr.Feature(lyr.GetLayerDefn())
        
        feat.SetField("IMAGENAME", iinfo.srcfn)
        feat.SetField("SENSOR", iinfo.sensor)
        feat.SetField("ACQDATE", iinfo.acqdate.strftime("%Y-%m-%d"))
        feat.SetField("CAT_ID", iinfo.catid)
        feat.SetField("OFF_NADIR", iinfo.ona)
        feat.SetField("SUN_ELEV", iinfo.sunel)
        feat.SetField("SUN_AZ", iinfo.sunaz)
        feat.SetField("SAT_ELEV", iinfo.satel)
        feat.SetField("SAT_AZ", iinfo.sataz)
        feat.SetField("CLOUDCOVER", iinfo.cloudcover)
        feat.SetField("SCORE", iinfo.score)
        
        tdi = iinfo.tdi if iinfo.tdi else 0
        feat.SetField("TDI", tdi)
        
        date_diff = iinfo.date_diff if iinfo.date_diff else -9999
        feat.SetField("DATE_DIFF", date_diff)
        
        res = ((iinfo.xres+iinfo.yres)/2.0) if iinfo.xres else 0
        feat.SetField("RESOLUTION", res)
        
        if args.calc_stats:
            if len(iinfo.stat_dct) > 0:
                min_list = []
                max_list = []
                mean_list = []
                stdev_list = []
                px_cnt_list = []
                keys = list(iinfo.stat_dct.keys())
                keys.sort()
                for band in keys:
                    imin, imax, imean, istdev = iinfo.stat_dct[band]
                    ipx_cnt = iinfo.datapixelcount_dct[band]
                    min_list.append(str(imin))
                    max_list.append(str(imax))
                    mean_list.append(str(imean))
                    stdev_list.append(str(istdev))
                    px_cnt_list.append(str(ipx_cnt))
                
                feat.SetField("STATS_MIN", ",".join(min_list))
                feat.SetField("STATS_MAX", ",".join(max_list))
                feat.SetField("STATS_MEAN", ",".join(mean_list))
                feat.SetField("STATS_STD", ",".join(stdev_list))
                feat.SetField("STATS_PXCT", ",".join(px_cnt_list))

        if params.median_remove is True:
            keys = list(iinfo.median.keys())
            keys.sort()
            median_list = [str(iinfo.median[band]) for band in keys]
            feat.SetField("MEDIAN", ",".join(median_list))
            #logger.info("median = %s", ",".join(median_list))
            
        feat.SetGeometry(geom)
        
        if lyr.CreateFeature(feat) != 0:
            logger.info("ERROR: Could not create feature for image %s", iinfo.srcfn)
            
        feat.Destroy()
    
    
def build_tiles_shp(mosaicname, tiles, params):
    tiles_shp = mosaicname + "_tiles.shp"
    if os.path.isfile(tiles_shp):
        logger.info("Tiles shapefile already exists: %s", os.path.basename(tiles_shp))
    else:
        logger.info("Creating shapefile of tiles: %s", os.path.basename(tiles_shp))
        fields = [('ROW', ogr.OFTInteger, 4),
                  ('COL', ogr.OFTInteger, 4),
                  ("TILENAME", ogr.OFTString, 100),
                  ('XMIN', ogr.OFTReal, 0),
                  ('XMAX', ogr.OFTReal, 0),
                  ('YMIN', ogr.OFTReal, 0),
                  ('YMAX', ogr.OFTReal, 0)]
                  
        OGR_DRIVER = "ESRI Shapefile"
        ogrDriver = ogr.GetDriverByName(OGR_DRIVER)
        if ogrDriver is None:
            logger.error("OGR: Driver %s is not available", OGR_DRIVER)
            sys.exit(-1)
    
        if os.path.isfile(tiles_shp):
            ogrDriver.DeleteDataSource(tiles_shp)
        vds = ogrDriver.CreateDataSource(tiles_shp)
        if vds is None:
            logger.error("Could not create shp")
            sys.exit(-1)
        
        shpd, shpn = os.path.split(tiles_shp)
        shpbn, shpe = os.path.splitext(shpn)
        
        rp = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
        rp.ImportFromWkt(params.proj)
        
        lyr = vds.CreateLayer(shpbn, rp, ogr.wkbPolygon)
        if lyr is None:
            logger.error("ERROR: Failed to create layer: %s", shpbn)
            sys.exit(-1)
        
        for fld, fdef, flen in fields:
            field_defn = ogr.FieldDefn(fld, fdef)
            if fdef == ogr.OFTString:
                field_defn.SetWidth(flen)
            if lyr.CreateField(field_defn) != 0:
                logger.error("ERROR: Failed to create field: %s", fld)
        
        for t in tiles:
            feat = ogr.Feature(lyr.GetLayerDefn())
            feat.SetField("TILENAME", os.path.basename(t.name))
            feat.SetField("ROW", t.j)
            feat.SetField("COL", t.i)
            feat.SetField("XMIN", t.xmin)
            feat.SetField("XMAX", t.xmax)
            feat.SetField("YMIN", t.ymin)
            feat.SetField("YMAX", t.ymax)
            feat.SetGeometry(t.geom)
            
            if lyr.CreateFeature(feat) != 0:
                logger.error("ERROR: Could not create feature for tile %s", t)
            feat.Destroy()
            

if __name__ == '__main__':
    main()
