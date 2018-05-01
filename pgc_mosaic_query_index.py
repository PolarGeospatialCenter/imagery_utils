import os, string, sys, logging, argparse, numpy, glob
from datetime import datetime, date
import gdal, ogr,osr, gdalconst

from lib import ortho_functions, mosaic, utils, taskhandler

### Create Logger
logger = logging.getLogger("logger")

EPSG_WGS84 = 4326

def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="query PGC index for images contributing to a mosaic"
    )
    
    parser.add_argument("index", help="PGC index shapefile")
    parser.add_argument("tile_csv", help="tile schema csv")
    parser.add_argument("dstdir", help="textfile output directory")
    parser.add_argument("mosaic", help="mosaic name without extension")
    #pos_arg_keys = ["index","tile_csv","dstdir"]
    
    parser.add_argument("-e", "--extent", nargs=4, type=float,
                        help="extent of output mosaic -- xmin xmax ymin ymax (default is union of all inputs)")
    parser.add_argument("--force-pan-to-multi", action="store_true", default=False,
                        help="if output is multiband, force script to also use 1 band images")
    parser.add_argument("-b", "--bands", type=int,
                        help="number of output bands( default is number of bands in the first image)")
    parser.add_argument("--tday",
                        help="month and day of the year to use as target for image suitability ranking -- 04-05")
    parser.add_argument("--tyear",
                        help="year (or year range) to use as target for image suitability ranking -- 2017 or 2015-2017")
    parser.add_argument("--nosort", action="store_true", default=False,
                        help="do not sort images by metadata. script uses the order of the input textfile or directory (first image is first drawn).  Not recommended if input is a directory; order will be random")
    parser.add_argument("--use-exposure", action="store_true", default=False,
                        help="use exposure settings in metadata to inform score")
    parser.add_argument("--exclude",
                        help="file of file name patterns (text only, no wildcards or regexs) to exclude")
    parser.add_argument("--max-cc", type=float, default=0.2,
                        help="maximum fractional cloud cover (0.0-1.0, default 0.2)")
    parser.add_argument("--include-all-ms", action="store_true", default=False,
                        help="include all multispectral imagery, even if the imagery has differing numbers of bands")
    parser.add_argument("--min-contribution-area", type=int, default=20000000,
                      help="minimum area contribution threshold in target projection units (default=20000000). Higher values remove more image slivers from the resulting mosaic") 
    parser.add_argument("--log",
                      help="output log file (default is queryFP.log in the output folder)")
    parser.add_argument("--ttile",
                      help="target tile (default is to compute all valid tiles. multiple tiles should be delimited by a comma [ex: 23_24,23_25])")
    parser.add_argument("--overwrite", action="store_true", default=False,
                      help="overwrite any existing files")
    parser.add_argument("--stretch", choices=ortho_functions.stretches, default="rf",
                      help="stretch abbreviation used in image processing (default=rf)")
    parser.add_argument("--build-shp", action='store_true', default=False,
                      help="build shapefile of intersecting images (only invoked if --no_sort is not used)")
    parser.add_argument("--require-pan", action='store_true', default=False,
                      help="limit search to imagery with both a multispectral and a panchromatic component")
 
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])

    src = os.path.abspath(args.index)
    csvpath = os.path.abspath(args.tile_csv)
    dstdir = os.path.abspath(args.dstdir)
    
    #### Validate Required Arguments
    try:
        dsp, lyrn = utils.get_source_names(src)
    except RuntimeError as e:
        parser.error(e)
    if not os.path.isfile(csvpath):
        parser.error("Arg2 is not a valid file path: %s" %csvpath)
    
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

    #### Validate target year/year range option
    if args.tyear is not None:
        if len(str(args.tyear)) == 4:
            ## ensure single year is valid
            try:
                tyear_test = datetime(year=args.tyear, month=1, day=1)
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

    ##### Configure Logger
    if args.log is not None:
        logfile = os.path.abspath(args.log)
    else:
        logfile = os.path.join(dstdir,"queryFP_%s.log" %datetime.today().strftime("%Y%m%d%H%M%S"))
    
    lfh = logging.FileHandler(logfile)
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)
    
    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)
    
    #### Get exclude_list if specified
    if args.exclude is not None:
        if not os.path.isfile(args.exclude):
            parser.error("Value for option --exclude-list is not a valid file")
        
        f = open(args.exclude, 'r')
        exclude_list = set([line.rstrip() for line in f.readlines()])
    else:
        exclude_list = set()
    
    logger.debug("Exclude list: %s", str(exclude_list))
    
    #### Parse csv, validate tile ID and get tilegeom
    tiles = {}
    csv = open(csvpath,'r')
    for line in csv:
        tile = line.rstrip().split(",")
        if len(tile) != 9:
            logger.warning("funny csv line: %s", line.strip('\n'))
        else:
            name = tile[2]
            if name != "name":
                ### Tile csv schema: row, column, name, status, xmin, xmax, ymin, ymax, epsg code
                t = mosaic.TileParams(float(tile[4]),float(tile[5]),float(tile[6]),float(tile[7]),int(tile[0]),int(tile[1]),tile[2])
                t.status = tile[3]
                t.epsg = int(tile[8])
                tiles[name] = t
    csv.close()
    
    if args.ttile is not None:
        if "," in args.ttile:
            ttiles = args.ttile.split(",")
        else:          
            ttiles = [args.ttile]
    
        for ttile in ttiles:
            if ttile not in tiles:
                logger.info("Target tile is not in the tile csv: %s", ttile)
                
            else:
                t = tiles[ttile]
                if t.status == "0":
                    logger.error("Tile status indicates it should not be created: %s, %s", ttile,t.status)
                else:
                    try:
                        HandleTile(t,src,dstdir,csvpath,args,exclude_list)
                    except RuntimeError as e:
                        logger.error(e)
    
    else:
        keys = tiles.keys()
        keys.sort()
        for tile in keys:
            t = tiles[tile]
            if t.status == "1":
                try:
                    HandleTile(t,src,dstdir,csvpath,args,exclude_list)
                except RuntimeError as e:
                    logger.error(e)
        
        
def HandleTile(t,src,dstdir,csvpath,args,exclude_list):
    
    
    otxtpath = os.path.join(dstdir,"%s_%s_orig.txt" %(os.path.basename(csvpath)[:-4],t.name))
    mtxtpath = os.path.join(dstdir,"%s_%s_ortho.txt" %(os.path.basename(csvpath)[:-4],t.name))
    
    if os.path.isfile(otxtpath) and os.path.isfile(mtxtpath) and args.overwrite is False:
        logger.info("Tile %s processing files already exist", t.name)
    else:
        logger.info("Tile %s", t.name)
    
        t_srs = osr.SpatialReference()
        t_srs.ImportFromEPSG(t.epsg)
        
        #### Open mfp
        dsp, lyrn = utils.get_source_names(src)
        
        ds = ogr.Open(dsp)
        if ds is None:
            logger.error("Open failed")
            
        else:
            lyr = ds.GetLayerByName( lyrn )
            
            if not lyr:
                raise RuntimeError("Layer {} does not exist in dataset {}".format(lyrn, dsp))
            else:

                s_srs = lyr.GetSpatialRef()
                #logger.debug(str(s_srs))
                #logger.debug(str(t.geom))
                
                tile_geom_in_s_srs = t.geom.Clone()

                if not t_srs.IsSame(s_srs):
                    ict = osr.CoordinateTransformation(t_srs, s_srs)
                    ct = osr.CoordinateTransformation(s_srs, t_srs)
                    tile_geom_in_s_srs.Transform(ict)

                # if the geometry crosses meridian, split it into multipolygon (else this breaks SetSpatialFilter)
                if utils.doesCross180(tile_geom_in_s_srs):
                    logger.debug("tile_geom_in_s_srs crosses 180 meridian; splitting to multiple polygons...")
                    tile_geom_in_s_srs = utils.getWrappedGeometry(tile_geom_in_s_srs)

                lyr.ResetReading()
                lyr.SetSpatialFilter(tile_geom_in_s_srs)
                feat = lyr.GetNextFeature()
                
                imginfo_list1 = []
                
                while feat:
                    
                    iinfo = mosaic.ImageInfo(feat,"RECORD",srs=s_srs)
                    
                    if iinfo.geom is not None and iinfo.geom.GetGeometryType() in (ogr.wkbPolygon,ogr.wkbMultiPolygon):
                        if not t_srs.IsSame(s_srs):
                            iinfo.geom.Transform(ct)
                            ## fix self-intersection errors caused by reprojecting over 180
                            temp = iinfo.geom.Buffer(0.1) # assumes a projected coordinate system with meters or feet as units
                            iinfo.geom = temp
                        
                        if iinfo.geom.Intersects(t.geom):
                            
                            if iinfo.scene_id in exclude_list:
                                logger.debug("Scene in exclude list, excluding: %s", iinfo.srcfp)
                                
                            elif not os.path.isfile(iinfo.srcfp):
                                logger.warning("Scene path is invalid, excluding %s (path = %s)", iinfo.scene_id,
                                               iinfo.srcfp)
                            elif args.require_pan:
                                srcfp = iinfo.srcfp
                                srcdir, mul_name = os.path.split(srcfp)
                                if iinfo.sensor in ["WV02","WV03","QB02"]:
                                    pan_name = mul_name.replace("-M","-P")
                                elif iinfo.sensor == "GE01":
                                    if "_5V" in mul_name:
                                        pan_name_base = srcfp[:-24].replace("M0","P0")
                                        candidates = glob.glob(pan_name_base + "*")
                                        candidates2 = [f for f in candidates if f.endswith(('.ntf','.NTF','.tif','.TIF'))]
                                        if len(candidates2) == 0:
                                            pan_name = ''
                                        elif len(candidates2) == 1:
                                            pan_name = os.path.basename(candidates2[0])
                                        else:
                                            pan_name = ''
                                            logger.error('%i panchromatic images match the multispectral image name '
                                                         '%s', len(candidates2), mul_name)
                                    else:
                                        pan_name = mul_name.replace("-M","-P")
                                elif iinfo.sensor == "IK01":
                                    pan_name = mul_name.replace("blu","pan")
                                    pan_name = mul_name.replace("msi","pan")
                                    pan_name = mul_name.replace("bgrn","pan")
                                pan_srcfp = os.path.join(srcdir,pan_name)
                                if not os.path.isfile(pan_srcfp):
                                    logger.debug("Image does not have a panchromatic component, excluding: %s",
                                                 iinfo.srcfp)
                                else:
                                    logger.debug("Intersect %s, %s: %s", iinfo.scene_id, iinfo.srcfp, str(iinfo.geom))
                                    imginfo_list1.append(iinfo)
                                
                            else:
                                logger.debug( "Intersect %s, %s: %s", iinfo.scene_id, iinfo.srcfp, str(iinfo.geom))
                                imginfo_list1.append(iinfo)                                
                                
                    feat = lyr.GetNextFeature()
            
            ds = None
        
            logger.info("Number of intersects in tile %s: %i", t.name, len(imginfo_list1))
            
            if len(imginfo_list1) > 0:
                
                
                #### Get mosaic parameters
                logger.debug("Getting mosaic parameters")
                params = mosaic.getMosaicParameters(imginfo_list1[0],args)
                
                #### Remove images that do not match ref
                logger.debug("Setting image pattern filter")
                imginfo_list2 = mosaic.filterMatchingImages(imginfo_list1,params)
                logger.info("Number of images matching filter: %i", len(imginfo_list2))
                    
                if args.nosort is False:    
                    #### Sort by quality
                    logger.debug("Sorting images by quality")
                    imginfo_list3 = []
                    for iinfo in imginfo_list2:
                        
                        iinfo.getScore(params)
                        if iinfo.score > 0:
                            imginfo_list3.append(iinfo)
                    
                    # sort so highest score is last
                    imginfo_list3.sort(key=lambda x: x.score)
                    
                else:
                    imginfo_list3 = list(imginfo_list2)
                    
                ####  Overlay geoms and remove non-contributors
                logger.debug("Overlaying images to determine contributors")
                contribs = mosaic.determine_contributors(imginfo_list3,t.geom,args.min_contribution_area)
                                            
                logger.info("Number of contributing images: %i", len(contribs))
            
                if len(contribs) > 0:
                    
                    if args.build_shp:
                        
                        #######################################################
                        #### Create Shp
                        
                        shp = os.path.join(dstdir,"{}_{}_imagery.shp".format(args.mosaic, t.name))
                   
                        logger.debug("Creating shapefile of geoms: %s", shp)
                    
                        fields = [("IMAGENAME", ogr.OFTString, 100),
                            ("SCORE", ogr.OFTReal, 0)]
                        
                        OGR_DRIVER = "ESRI Shapefile"
                        
                        ogrDriver = ogr.GetDriverByName(OGR_DRIVER)
                        if ogrDriver is None:
                            logger.debug("OGR: Driver %s is not available", OGR_DRIVER)
                            sys.exit(-1)
                        
                        if os.path.isfile(shp):
                            ogrDriver.DeleteDataSource(shp)
                        vds = ogrDriver.CreateDataSource(shp)
                        if vds is None:
                            logger.debug("Could not create shp")
                            sys.exit(-1)
                        
                        shpd, shpn = os.path.split(shp)
                        shpbn, shpe = os.path.splitext(shpn)
                        
                        lyr = vds.CreateLayer(shpbn, t_srs, ogr.wkbPolygon)
                        if lyr is None:
                            logger.debug("ERROR: Failed to create layer: %s", shpbn)
                            sys.exit(-1)
                        
                        for fld, fdef, flen in fields:
                            field_defn = ogr.FieldDefn(fld, fdef)
                            if fdef == ogr.OFTString:
                                field_defn.SetWidth(flen)
                            if lyr.CreateField(field_defn) != 0:
                                logger.debug("ERROR: Failed to create field: %s", fld)
                        
                        for iinfo, geom in contribs:
                        
                            logger.debug("Image: %s", iinfo.srcfn)
                            
                            feat = ogr.Feature(lyr.GetLayerDefn())
                            
                            feat.SetField("IMAGENAME",iinfo.srcfn)
                            feat.SetField("SCORE",iinfo.score)
    
                            feat.SetGeometry(geom)
                            if lyr.CreateFeature(feat) != 0:
                                logger.debug("ERROR: Could not create feature for image %s", iinfo.srcfn)
                            else:
                                logger.debug("Created feature for image: %s", iinfo.srcfn)
                                
                            feat.Destroy()
                    
                    #### Write textfiles
                    if not os.path.isdir(dstdir):
                        os.makedirs(dstdir)
                    
                    otxtpath = os.path.join(dstdir, "{}_{}_orig.txt".format(args.mosaic, t.name))
                    mtxtpath = os.path.join(dstdir, "{}_{}_ortho.txt".format(args.mosaic, t.name))
                    otxt = open(otxtpath,'w')
                    mtxt = open(mtxtpath,'w')
                    
                    for iinfo, geom in contribs:
                        
                        if not os.path.isfile(iinfo.srcfp):
                            logger.warning("Image does not exist: %s", iinfo.srcfp)
                            
                        otxt.write("{}\n".format(iinfo.srcfp))
                        m_fn = "{0}_u08{1}{2}.tif".format(
                            os.path.splitext(iinfo.srcfn)[0],
                            args.stretch,
                            t.epsg
                        )
                        
                        mtxt.write(os.path.join(dstdir,'ortho',t.name,m_fn)+"\n")
 
                    otxt.close()




if __name__ == '__main__':
    main()
