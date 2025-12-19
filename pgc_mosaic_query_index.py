#!/usr/bin/env python

import argparse
import glob
import logging
import os
import sys
from datetime import date, datetime

from osgeo import ogr, osr

from lib import mosaic, ortho_functions, utils
from lib import VERSION

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
                        help="do not sort images by metadata. script uses the order of the input textfile or directory "
                             "(first image is first drawn).  Not recommended if input is a directory; order will be "
                             "random")
    parser.add_argument("--use-exposure", action="store_true", default=False,
                        help="use exposure settings in metadata to inform score")
    parser.add_argument("--exclude", default=None,
                        help="options: pgc_exclude_list: from pgc database; "
                             "a filepath: of file name patterns (text only, no wildcards or regexs) to exclude;"
                             "None: no exclude list")
    parser.add_argument("--max-cc", type=float, default=0.2,
                        help="maximum fractional cloud cover (0.0-1.0)")
    parser.add_argument("--min-sunel", type=int, default=10,
                        help="minimum sun angle in degrees (default=10)")
    parser.add_argument("--include-all-ms", action="store_true", default=False,
                        help="include all multispectral imagery, even if the imagery has differing numbers of bands")
    parser.add_argument("--min-contribution-area", type=int, default=20000000,
                        help="minimum area contribution threshold in target projection units (default=20000000). "
                             "Higher values remove more image slivers from the resulting mosaic")
    parser.add_argument("--log",
                        help="output log file (default is queryFP.log in the output folder)")
    parser.add_argument("--ttile",
                        help="target tile (default is to compute all valid tiles. multiple tiles should be delimited "
                             "by a comma [ex: 23_24,23_25])")
    parser.add_argument("--overwrite", action="store_true", default=False,
                        help="overwrite any existing files")
    parser.add_argument("--stretch", choices=ortho_functions.stretches, default="rf",
                        help="stretch abbreviation used in image processing (default=rf)")
    parser.add_argument("--build-shp", action='store_true', default=False,
                        help="build shapefile of intersecting images (only invoked if --no_sort is not used)")
    parser.add_argument("--require-pan", action='store_true', default=False,
                        help="limit search to imagery with both a multispectral and a panchromatic component")
    parser.add_argument("--bit-depth", default="Byte",
                        help="bit depth for batch mosaic processing, default is Byte (u08), other option is UInt16 (u16)")
    parser.add_argument("--skip-cmd-txt", action='store_true', default=True,
                        help='THIS OPTION IS DEPRECATED - '
                             'By default this arg is True and the cmd text file will not be written. '
                             'Input commands are written to the log for reference.')
    parser.add_argument("--version", action='version', version="imagery_utils v{}".format(VERSION))

 
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
        logger.error(utils.capture_error_trace())
        parser.error(e)
    if not os.path.isfile(csvpath):
        parser.error("Arg2 is not a valid file path: %s" %csvpath)
    
    #### Validate target day option
    if args.tday is not None:
        try:
            m = int(args.tday.split("-")[0])
            d = int(args.tday.split("-")[1])
            td = date(2000, m, d)
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
                tyear_test = datetime(year=int(args.tyear), month=1, day=1)
            except ValueError:
                parser.error("Supplied year {0} is not valid".format(args.tyear))

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

            else:
                parser.error("Supplied year range {0} is not valid; should be like: 2015 OR 2015-2017"
                             .format(args.tyear))

        else:
            parser.error("Supplied year {0} is not valid, or its format is incorrect; should be 4 digits for single "
                         "year (e.g., 2017), eight digits and dash for range (e.g., 2015-2017)".format(args.tyear))

    ## validate bit depth options
    bit_depth_options = {"Byte":"u08",
                         "UInt16":"u16"}
    if args.bit_depth in bit_depth_options.keys():
        args.bit_depth = bit_depth_options[args.bit_depth]
    else:
        parser.error("Supplied bit depth {0} is not valid; "
                     "should be one of the following {1}".format(args.bit_depth, bit_depth_options.keys()))

    ##### Configure Logger
    if args.log is not None:
        logfile = os.path.abspath(args.log)
    else:
        logfile = os.path.join(dstdir, "queryFP_{}.log".format(datetime.today().strftime("%Y%m%d%H%M%S")))
    
    lfh = logging.FileHandler(logfile)
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)
    
    lsh = logging.StreamHandler()
    lsh.setLevel(logging.INFO)
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)

    # log input command for reference
    command_str = ' '.join(sys.argv)
    logger.info("Running command: {}".format(command_str))

    #### Get exclude_list if specified
    exclude_list = mosaic.getExcludeList(args.exclude)

    #### Parse csv, validate tile ID and get tilegeom
    tiles = {}
    csv = open(csvpath, 'r')
    for line in csv:
        tile = line.rstrip().split(",")
        if len(tile) != 9:
            logger.warning("funny csv line: %s", line.strip('\n'))
        else:
            name = tile[2]
            if name != "name":
                ### Tile csv schema: row, column, name, status, xmin, xmax, ymin, ymax, epsg code
                t = mosaic.TileParams(float(tile[4]), float(tile[5]), float(tile[6]), float(tile[7]), int(tile[0]),
                                      int(tile[1]), tile[2])
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
                    logger.error("Tile status indicates it should not be created: %s, %s", ttile, t.status)
                else:
                    try:
                        HandleTile(t, src, dstdir, csvpath, args, exclude_list)
                    except RuntimeError as e:
                        logger.error(utils.capture_error_trace())
                        logger.error(e)
    
    else:
        keys = list(tiles.keys())
        keys.sort()
        for tile in keys:
            t = tiles[tile]
            if t.status == "1":
                try:
                    HandleTile(t, src, dstdir, csvpath, args, exclude_list)
                except RuntimeError as e:
                    logger.error(utils.capture_error_trace())
                    logger.error(e)
        
        
def HandleTile(t, src, dstdir, csvpath, args, exclude_list):

    querypath = os.path.join(dstdir, "query")
    otxtpath = os.path.join(querypath, "{}_{}_orig.txt".format(args.mosaic, t.name))
    otxtpath_ontape = os.path.join(querypath, "{}_{}_orig_ontape.csv".format(args.mosaic, t.name))
    mtxtpath = os.path.join(querypath, "{}_{}_ortho.txt".format(args.mosaic, t.name))

    if os.path.isfile(otxtpath) and os.path.isfile(mtxtpath) and args.overwrite is False:
        logger.info("Tile %s processing files already exist", t.name)
    else:
        logger.info("Tile %s", t.name)
    
        t_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
        t_srs.ImportFromEPSG(t.epsg)
        
        #### Open mfp
        dsp, lyrn = utils.get_source_names(src)
        
        ds = ogr.Open(dsp)
        if ds is None:
            logger.error("Open failed")
            
        else:
            lyr = ds.GetLayerByName(lyrn)
            
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

                # if running the pansharpen process, check if the multispectral images have a panchromatic component
                if args.require_pan:
                    lyr.ResetReading()
                    lyr.SetSpatialFilter(tile_geom_in_s_srs)
                    # uncomment to review the fields in the input mfp layer
                    # layerDef = lyr.GetLayerDefn()
                    # logger.info([layerDef.GetFieldDefn(i).GetName() for i in range(layerDef.GetFieldCount())])
                    logger.info("Total input feature count with spatial filter: {}".format(len(lyr)))
                    lyr.SetAttributeFilter("prod_code = 'P1BS'")
                    logger.info("P1BS subset of selected features: {}".format(len(lyr)))

                    # when looping through features below, if args.require_pan: check if iinfo.pair_scene_id in scene_ids list,
                    # add it to pairs list, then loop through pairs list to add those images to
                    pansh_pair_lookup = set()
                    pansh_pair_matches = []
                    # feat = lyr.GetNextFeature()
                    logger.info("Starting panchromatic lookup process")
                    for feat in lyr:
                        pansh_pair_lookup.add(feat.GetField("scene_id"))

                    logger.info("scene IDs in pansh pair lookup list: {}".format(len(pansh_pair_lookup)))
                    if len(pansh_pair_lookup) == 0:
                        logger.warning("No panchromatic images were found, check the input MFP layer you are using")

                # reset the OGR object
                lyr.SetAttributeFilter("")
                lyr.ResetReading()
                lyr.SetSpatialFilter(tile_geom_in_s_srs)
                feat = lyr.GetNextFeature()
                
                imginfo_list1 = []
                while feat:
                    iinfo = mosaic.ImageInfo(feat, "RECORD", srs=s_srs)
                    # skip panchromatic if require_pan
                    # evaluate multispectral images for mosaic coverage
                    # panchromatic component will be pulled from look up list when writing .txt files below
                    if args.require_pan:
                        # logger.info("require-pan arg passed in: only evaluating multispectral images with pan component")
                        if not iinfo.spec_type == "Multispectral":
                            feat = lyr.GetNextFeature()
                            continue
                    
                    if iinfo.geom is not None and iinfo.geom.GetGeometryType() in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
                        if not t_srs.IsSame(s_srs):
                            iinfo.geom.Transform(ct)
                            ## fix self-intersection errors caused by reprojecting over 180
                            temp = iinfo.geom.Buffer(0.1) # assumes a projected coordinate system with meters or feet as units
                            iinfo.geom = temp
                        
                        if iinfo.geom.Intersects(t.geom):
                            
                            if iinfo.scene_id in exclude_list:
                                logger.debug("Scene in exclude list, excluding: %s", iinfo.srcfp)
                                
                            elif not os.path.isfile(iinfo.srcfp) and iinfo.status != "tape":
                                #logger.info("iinfo.status != tape: {0}".format(iinfo.status != "tape"))
                                logger.warning("Scene path is invalid, excluding %s (path = %s) (status = %s)",
                                               iinfo.scene_id, iinfo.srcfp, iinfo.status)

                            elif args.require_pan:
                                # check that mul has pan component
                                logger.debug("Checking for panchromatic component")
                                if not iinfo.pan_scene_id in pansh_pair_lookup:
                                    # check if the pan component has a 1-second time difference
                                    if iinfo.pan_scene_id_datetime_dif in pansh_pair_lookup:
                                        logger.debug("Image panchromatic component has 1-second time dif")
                                        logger.debug("Intersect %s, %s: %s", iinfo.scene_id, iinfo.srcfp,
                                                     str(iinfo.geom))
                                        iinfo.pan_scene_id = iinfo.pan_scene_id_datetime_dif
                                        pansh_pair_matches.append(iinfo.pan_scene_id)
                                        imginfo_list1.append(iinfo)
                                    else:
                                        logger.debug("Image does not have a panchromatic component, excluding: %s",
                                                 iinfo.srcfp)
                                else:
                                    logger.debug("Intersect %s, %s: %s", iinfo.scene_id, iinfo.srcfp, str(iinfo.geom))
                                    pansh_pair_matches.append(iinfo.pan_scene_id)
                                    imginfo_list1.append(iinfo)
                                
                            else:
                                logger.debug("Intersect %s, %s: %s", iinfo.scene_id, iinfo.srcfp, str(iinfo.geom))
                                imginfo_list1.append(iinfo)                                
                                
                    feat = lyr.GetNextFeature()
            
            ds = None
        
            logger.info("Number of intersects in tile %s: %i", t.name, len(imginfo_list1))
            
            if len(imginfo_list1) > 0:

                #### Get mosaic parameters
                logger.debug("Getting mosaic parameters")
                params = mosaic.getMosaicParameters(imginfo_list1[0], args)
                
                #### Remove images that do not match ref
                logger.debug("Setting image pattern filter")
                imginfo_list2 = mosaic.filterMatchingImages(imginfo_list1, params)
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
                    
                ## Overlay geoms and remove non-contributors
                logger.debug("Overlaying images to determine contributors")
                contribs = mosaic.determine_contributors(imginfo_list3, t.geom, args.min_contribution_area)
                                            
                logger.info("Number of contributing images: %i", len(contribs))
            
                if len(contribs) > 0:
                    os.makedirs(querypath, exist_ok=True)
                    if args.build_shp:
                        
                        ## Create Shp
                        shp = os.path.join(querypath, "{}_{}_imagery.shp".format(args.mosaic, t.name))
                        logger.debug("Creating shapefile of geoms: %s", shp)
                        fields = [("IMAGENAME", ogr.OFTString, 100), ("SCORE", ogr.OFTReal, 0)]
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
                            feat.SetField("IMAGENAME", iinfo.srcfn)
                            feat.SetField("SCORE", iinfo.score)
                            feat.SetGeometry(geom)
                            try:
                                lyr.CreateFeature(feat)
                            except RuntimeError as e:
                                logger.warning("Could not create feature for image %s: %s", iinfo.srcfn, e)
                            else:
                                logger.debug("Created feature for image: %s", iinfo.srcfn)
                            feat.Destroy()
                    
                    #### Write textfiles
                    rn_fromtape_basedir = os.path.join(dstdir, "renamed_fromtape")
                    rn_fromtape_path = os.path.join(rn_fromtape_basedir, t.name)

                    otxt = open(otxtpath, 'w')
                    ttxt = open(otxtpath_ontape, 'w')
                    mtxt = open(mtxtpath, 'w')

                    # write header
                    ttxt.write("{0},{1},{2},{3},{4}\n".format("SCENE_ID", "STRIP_ID", "CATALOG_ID", "S_FILEPATH", "STATUS"))

                    tape_ct = 0

                    if args.require_pan:
                        # add pan component to contribs
                        pan_contribs = []
                        ds = ogr.Open(dsp)
                        lyr = ds.GetLayerByName(lyrn)
                        lyr.ResetReading()
                        lyr.SetSpatialFilter(tile_geom_in_s_srs)
                        lyr.SetAttributeFilter("")
                        empty_geom = None
                        logger.info("Adding panchromatic component images to output files")

                        for iinfo, geom in contribs:
                            # TODO: speed this part up
                            pan_component_id = iinfo.pan_scene_id
                            lyr.SetAttributeFilter("SCENE_ID = '{}'".format(pan_component_id))
                            pan_feat = lyr.GetNextFeature()
                            pan_iinfo = mosaic.ImageInfo(pan_feat, "RECORD", srs=s_srs)
                            pan_contribs.append([pan_iinfo, empty_geom])

                        logger.info("Found {} pan components to go with {} multispectral images".format(
                            len(pan_contribs), len(contribs)))
                        contribs = contribs + pan_contribs

                    for iinfo, geom in contribs:
                        
                        if not os.path.isfile(iinfo.srcfp) and iinfo.status != "tape":
                            logger.warning("Image does not exist: %s", iinfo.srcfp)
                            
                        if iinfo.status == "tape":
                            # TODO: this "tape" logic does not belong in the public repo
                            tape_ct += 1
                            ttxt.write("{0},{1},{2},{3},{4}\n".format(iinfo.scene_id, iinfo.strip_id, iinfo.catid, iinfo.srcfp, iinfo.status))
                            # get srcfp with file extension
                            srcfp_file = os.path.basename(iinfo.srcfn)
                            otxt.write("{}\n".format(os.path.join(rn_fromtape_path, srcfp_file)))

                        else:
                            otxt.write("{}\n".format(iinfo.srcfp))

                        # add "_pansh" to the files name written to ortho.txt if running pansharpened
                        pansh_suf = ""
                        if args.require_pan:
                            pansh_suf = "_pansh"
                            # skip P1BS images since pansharpened outputs use M1BS in the name
                            if "P1BS" in iinfo.srcfp:
                                continue


                        m_fn = "{0}_{4}{1}{2}{3}.tif".format(
                            os.path.splitext(iinfo.srcfn)[0],
                            args.stretch,
                            t.epsg,
                            pansh_suf,
                            args.bit_depth
                        )
                        
                        mtxt.write(os.path.join(dstdir, 'ortho', t.name, m_fn) + "\n")
 
                    otxt.close()

                    if tape_ct == 0:
                        logger.debug("No files need to be pulled from tape.")
                        os.remove(otxtpath_ontape)

                    else:
                        # Prompt user to pull scenes from tape
                        logger.info("{0} scenes are not accessible, as they are on tape. Please use ir.py to pull "
                                       "scenes using file '{1}'. They must be put in directory '{2}', as file '{3}' "
                                       "contains hard-coded paths to said files (necessary to perform "
                                       "orthorectification).".
                                       format(tape_ct, otxtpath_ontape, rn_fromtape_path, otxtpath))


if __name__ == '__main__':
    main()
