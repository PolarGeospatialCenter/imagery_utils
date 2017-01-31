import os, string, sys, logging, argparse, numpy, re
from datetime import datetime
import gdal, ogr,osr, gdalconst

from lib import mosaic, utils


### Create Logger
logger = logging.getLogger("logger")

EPSG_WGS84 = 4326

def main():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="query PGC index for images contributing to a mosaic"
        )
    
    parser.add_argument("index", help="DG stereo index shapefile")
    parser.add_argument("aoi", help="aoi shapefile")
    parser.add_argument("aoifield", help="aoi shapefile feature ID field (used for output file name)")
    parser.add_argument("dstdir", help="textfile output directory")
    #pos_arg_keys = ["index","tile_csv","dstdir"]
    
    parser.add_argument("--log",
                      help="output log file (default is queryStereoFP.log in the output folder)")
    parser.add_argument("--target-aoi-id",
                      help="target feature ID value in AOI shapefile (multiple IDs should be comma-separated)")
    parser.add_argument("--overwrite", action="store_true", default=False,
                      help="overwrite any existing files")
    parser.add_argument("--build-shp", action='store_true', default=False,
                      help="build shapefile of intersecting stereopairs")
    parser.add_argument("--join-mfp",
                      help="join to the mfp (path required) and export matching imagery")
    parser.add_argument("--tday",
                        help="month and day of the year to use as target for image suitability ranking -- 04-05")
    parser.add_argument("--exclude",
                        help="file of pairname patterns (text only, no wildcards or regexs) to exclude")
    parser.add_argument("--remove-duplicates", action='store_true', default=False,
                        help="remove duplication of pairs that occur in multiple AOI features, pair in lowest lexical feature ID is kept")
    parser.add_argument("--no-filter", action='store_true', default=False,
                        help="do not filter out non-contributing DEMs")
    
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    
    stereo_index_path = os.path.abspath(args.index)
    aoi_path = os.path.abspath(args.aoi)
    dstdir = os.path.abspath(args.dstdir)
    
    #### Validate Required Arguments
    if not os.path.isfile(stereo_index_path):
        parser.error("Arg1 is not a valid file path: %s" %stereo_index_path)
    if not os.path.isfile(aoi_path):
        parser.error("Arg2 is not a valid file path: %s" %aoi_path)
    if not os.path.isdir(dstdir):
        parser.error("Arg2 is not a valid file path: %s" %dstdir)
    
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
            target_date = (m,d)
            
    else:
        target_date = None
    
    #### Verify mfp if given
    if args.join_mfp:
        try:
            mfp_dsp, mfp_lyr = get_source_names(args.join_mfp)
        except RuntimeError, e:
            parser.error(e)
    
    
    ##### Configure Logger
    if args.log is not None:
        logfile = os.path.abspath(args.log)
    else:
        logfile = os.path.join(dstdir,"query_stereoFP_%s.log" %datetime.today().strftime("%Y%m%d%H%M%S"))
    
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
    
    logger.debug("Exclude list: %s" %str(exclude_list))
    
    
    #### Parse aoi shp, validate IDs, and get geoms
    features = {}
    #### Open Shp
    shpd, shpn = os.path.split(aoi_path)
    shpbn, shpe = os.path.splitext(shpn)
    
    ds = ogr.Open(aoi_path)
    if ds is None:
        logger.warning("Open failed")
        
    else:
        lyr = ds.GetLayerByName( shpbn )
        
        aoi_srs = lyr.GetSpatialRef()
        logger.debug(str(aoi_srs))
        
        lyr.ResetReading()
        feat = lyr.GetNextFeature()
        
        while feat:
           
            i = feat.GetFieldIndex(args.aoifield)
            if i != -1:
                feature_id = feat.GetFieldAsString(i)
            else:
                logger.error("{} field not found in AOI shp".format(args.aoifield))
                break
            
            geom = feat.GetGeometryRef().Clone()
            if geom.IsValid():
                features[feature_id] = geom
            else:
                logger.info( "Feature {} does not have a valid geometry".format(feature_id))
            
            feat = lyr.GetNextFeature()
        
        ds = None
    
    
    #### If Target ID supplied, run only those aoi features
    selected_pairnames = set()
    
    if args.target_aoi_id:
        
        if "," in args.target_aoi_id:
            target_features = args.target_aoi_id.split(",")
        else:          
            target_features = [args.target_aoi_id]
    
        for target_feature in target_features:
            if target_feature not in features:
                logger.info("Target feature is not in the list of valid aoi features: %s" %target_feature)
                
            else:
                dstfn = os.path.join(dstdir,"{}_{}".format(os.path.basename(aoi_path)[:-4],target_feature))
                target_feature_geom = features[target_feature]
                feature_selection = HandleTile(target_feature, target_feature_geom, stereo_index_path, dstdir, dstfn, aoi_srs, args, exclude_list, target_date,selected_pairnames)
                if len(feature_selection) > 0:
                    selected_pairnames.update(feature_selection)
    
    else:
        keys = features.keys()
        keys.sort()
        for feature_id in keys:
            dstfn = os.path.join(dstdir,"{}_{}".format(os.path.basename(aoi_path)[:-4],feature_id))
            feature_geom = features[feature_id]
            feature_selection = HandleTile(feature_id, feature_geom, stereo_index_path, dstdir, dstfn, aoi_srs, args, exclude_list, target_date, selected_pairnames)
            if len(feature_selection) > 0:
                selected_pairnames.update(feature_selection)
        
        
def HandleTile(feature_id, feature_geom, stereo_index_path, dstdir, dstfn, aoi_srs, args, exclude_list, target_date, selected_pairnames=None):
    
    feature_selection = set()
    txtpath = dstfn+"_stereopairs.txt"
    txtpath_catalogids = dstfn+"_stereo_catalogIDs.txt"
    
    if os.path.isfile(txtpath) and args.overwrite is False:
        logger.info("Feature %s processing files already exist" %os.path.basename(dstfn))
    else:
        logger.debug("Feaure: %s" %(os.path.basename(dstfn)))
        
        #### Open Shp
        shpd, shpn = os.path.split(stereo_index_path)
        shpbn, shpe = os.path.splitext(shpn)
        
        ds = ogr.Open(stereo_index_path)
        if ds is None:
            logger.warning("Open failed")
            
        else:
            lyr = ds.GetLayerByName( shpbn )
            
            s_srs = lyr.GetSpatialRef()
            #logger.debug(str(s_srs))
            #logger.debug(str(feature_geom))
        
            if not aoi_srs.IsSame(s_srs):
                ict = osr.CoordinateTransformation(aoi_srs, s_srs)
                ct = osr.CoordinateTransformation(s_srs, aoi_srs)
            
            lyr.ResetReading()
            feat = lyr.GetNextFeature()
            
            demInfo_list1 = []
            
            while feat:
                
                demInfo = mosaic.DemInfo(feat,"RECORD",srs=s_srs)
                
                if demInfo.geom is not None and demInfo.geom.GetGeometryType() == ogr.wkbPolygon:
                    if not aoi_srs.IsSame(s_srs):
                        demInfo.geom.Transform(ct)
                    
                    if demInfo.geom.Intersect(feature_geom):
                        
                        if demInfo.pairname in exclude_list:
                            logger.debug("Pair in exclude list, excluding: %s" %demInfo.pairname)
                            
                        else:
                            intersect_area = demInfo.geom.Intersection(feature_geom).Area()
                            dem_area = demInfo.geom.Area()
                            percent_intersect = intersect_area / dem_area
                            
                            if percent_intersect > 0.1:
                            
                                logger.debug( "Intersect %s: %s" %(demInfo.pairname, str(demInfo.geom)))
                                demInfo_list1.append(demInfo)
                            
                            else:
                                logger.debug("Intersection is less than 10% of total stereopair area: {} ({} / {} = {})".format(demInfo.pairname,intersect_area,dem_area,percent_intersect))
                            
                feat = lyr.GetNextFeature()
            
            ds = None
        
            logger.info("Number of intersects in aoi_feature %s: %i" %(feature_id,len(demInfo_list1)))
            
            if len(demInfo_list1) > 0:
                
                #### Get score for image
                logger.debug("Sorting stereopairs by quality")
                demInfo_list2 = []
                for demInfo in demInfo_list1:
                    
                    demInfo.getScore(target_date)
                    if demInfo.score > 0:
                        demInfo_list2.append(demInfo)
            
                if not args.no_filter:    
                
                    ####  Overlay geoms and remove non-contributors
                    logger.debug("Overlaying images to determine contributors")
                    contribs = []
                    
                    feature_geom1 = feature_geom.Clone()
                    feature_geom2 = feature_geom.Clone()
                    
                    demInfo_list2.sort(key=lambda x: x.score, reverse=True)
                    
                    for demInfo in demInfo_list2:
                        geom = demInfo.geom
                        
                        
                        #### If geom intersects first coverage
                        if not feature_geom1.IsEmpty() and geom.Intersects(feature_geom1):
                            #logger.info(feature_geom1.Area()/1000000)
                            
                            #### add to contribs
                            contribs.append(demInfo)
                            
                            #### get remainder of pair geometry to compare to second coverage
                            geom_remainder = geom.Difference(feature_geom1)
                            
                            #### subtract geom from first coverage since that area is now covered
                            feature_geom1 = feature_geom1.Difference(geom)
                            
                        else:
                            geom_remainder = geom
                        
                        #### If remainder exists and intersects second coverage
                        if not geom_remainder.IsEmpty() and not feature_geom2.IsEmpty() and geom_remainder.Intersects(feature_geom2):
                            
                            #### add to contribs
                            if not demInfo in contribs:
                                contribs.append(demInfo)
                            
                            #### subtract geom from second coverage since that area is now covered
                            feature_geom2 = feature_geom2.Difference(geom_remainder)
                    
                else:
                    contribs = demInfo_list2
        
                logger.info("Number of contributing pairs: %i" %(len(contribs)))
                
                if args.remove_duplicates and selected_pairnames:
                    contribs2 = []
                    for demInfo in contribs:
                        if demInfo.pairname in selected_pairnames:
                            logger.debug("Pair already selected in a previous AOI feature: %s" %demInfo.pairname)
                        else:
                            contribs2.append(demInfo)
                    contribs = list(contribs2)
                    logger.info("Number of contributing pairs after removing duplicates: %i" %(len(contribs)))
                    
                if len(contribs) > 0:
                    
                    if not args.no_filter:
                        contribs.sort(key=lambda x: x.score)
                    
                    #### Create Shp
                    if args.build_shp:
                        
                        index_path = os.path.join(dstdir,"%s_stereopairs.shp" %(dstfn))
                   
                        logger.debug("Creating shapefile of geoms: %s" %index_path)
                    
                        fields = [
                            ("REGION", ogr.OFTString, 50),
                            ("PAIRNAME", ogr.OFTString, 100),
                            ("CATALOGID", ogr.OFTString, 20),
                            ("STEREOPAIR", ogr.OFTString, 20),
                            ("SCORE", ogr.OFTReal, 0),
                            ("SENSOR", ogr.OFTString, 8),
                            ("CLOUDCOVER", ogr.OFTReal, 0),
                            ("SUNEL", ogr.OFTReal, 0),
                            ("DATEDIFF", ogr.OFTReal, 0),
                            ("PREVIEWURL", ogr.OFTString, 254),
                        ]
                        
                        OGR_DRIVER = "ESRI Shapefile"
                        
                        ogrDriver = ogr.GetDriverByName(OGR_DRIVER)
                        if ogrDriver is None:
                            logger.debug("OGR: Driver %s is not available" % OGR_DRIVER)
                            sys.exit(-1)
                        
                        if os.path.isfile(index_path):
                            ogrDriver.DeleteDataSource(index_path)
                        vds = ogrDriver.CreateDataSource(index_path)
                        if vds is None:
                            logger.debug("Could not create index_path")
                            sys.exit(-1)
                        
                        shpd, shpn = os.path.split(index_path)
                        shpbn, shpe = os.path.splitext(shpn)
                        
                        
                        lyr = vds.CreateLayer(shpbn, aoi_srs, ogr.wkbPolygon)
                        if lyr is None:
                            logger.debug("ERROR: Failed to create layer: %s" % shpbn)
                            sys.exit(-1)
                        
                        for fld, fdef, flen in fields:
                            field_defn = ogr.FieldDefn(fld, fdef)
                            if fdef == ogr.OFTString:
                                field_defn.SetWidth(flen)
                            if lyr.CreateField(field_defn) != 0:
                                logger.debug("ERROR: Failed to create field: %s" % fld)
                        
                        for demInfo in contribs:
                        
                            logger.debug("Pairname: %s" %(demInfo.pairname))
                            
                            feat = ogr.Feature(lyr.GetLayerDefn())
                            
                            feat.SetField("REGION",feature_id)
                            feat.SetField("PAIRNAME",demInfo.pairname)
                            feat.SetField("CATALOGID",demInfo.catid)
                            feat.SetField("STEREOPAIR",demInfo.catid2)
                            feat.SetField("SCORE",demInfo.score)
                            feat.SetField("SENSOR",demInfo.sensor)
                            feat.SetField("CLOUDCOVER",demInfo.cloudcover)
                            feat.SetField("SUNEL",demInfo.sunel)
                            feat.SetField("DATEDIFF",demInfo.date_diff)
                            feat.SetField("PREVIEWURL",r"http://browse.digitalglobe.com/imagefinder/showBrowseMetadata?buffer=1.0&catalogId={}&imageHeight=512&imageWidth=512".format(demInfo.catid))
    
                            feat.SetGeometry(demInfo.geom)
                            if lyr.CreateFeature(feat) != 0:
                                logger.debug("ERROR: Could not create feature for stereopair %s" % demInfo.pairname)
                            else:
                                logger.debug("Created feature for stereopair: %s" %demInfo.pairname)
                                
                            feat.Destroy()
                    
                    #### Create intersecting imagery shp
                    if args.join_mfp:
                        mfp_path = args.join_mfp
                        img_index_path = os.path.join(dstdir,"%s_stereo_imagery.shp" %(dstfn))
                        
                        ## Open mfp
                        mfp_dsp,mfp_lyrname = get_source_names(mfp_path)
                        
                        mfp = ogr.Open(mfp_dsp)
                        if mfp is None:
                            logger.warning("Open failed")
                            
                        else:
                            mfp_lyr = mfp.GetLayerByName( mfp_lyrname )
                            mfp_srs = mfp_lyr.GetSpatialRef()
                            
                        
                            ### Create stereo_imagery shp
                            logger.debug("Creating shapefile: %s" %img_index_path)
                        
                            fields = [
                                ("SCENE_ID", ogr.OFTString, 100),
                                ("STRIP_ID", ogr.OFTString, 50),
                                ("CATALOG_ID", ogr.OFTString, 20),
                                ("PAIRNAME", ogr.OFTString, 50),
                                ("SENSOR", ogr.OFTString, 10),
                                ("ACQ_TIME", ogr.OFTString, 30),
                                ("PROD_CODE", ogr.OFTString, 4),
                                ("O_FILENAME", ogr.OFTString, 254),
                                ("O_FILEPATH", ogr.OFTString, 254),
                                ("O_DRIVE", ogr.OFTString, 20),
                                ("S_FILEPATH", ogr.OFTString, 254),
                                ("REGION", ogr.OFTString, 50),
                            ]
                            
                            OGR_DRIVER = "ESRI Shapefile"
                            
                            ogrDriver = ogr.GetDriverByName(OGR_DRIVER)
                            if ogrDriver is None:
                                logger.debug("OGR: Driver %s is not available" % OGR_DRIVER)
                                sys.exit(-1)
                            
                            if os.path.isfile(img_index_path):
                                ogrDriver.DeleteDataSource(img_index_path)
                            vds = ogrDriver.CreateDataSource(img_index_path)
                            if vds is None:
                                logger.debug("Could not create index_path")
                                sys.exit(-1)
                            
                            shpd, shpn = os.path.split(img_index_path)
                            shpbn, shpe = os.path.splitext(shpn)
                            
                            lyr = vds.CreateLayer(shpbn, mfp_srs, ogr.wkbPolygon)
                            if lyr is None:
                                logger.debug("ERROR: Failed to create layer: %s" % shpbn)
                            
                            else:
                            
                                for fld, fdef, flen in fields:
                                    field_defn = ogr.FieldDefn(fld, fdef)
                                    if fdef == ogr.OFTString:
                                        field_defn.SetWidth(flen)
                                    if lyr.CreateField(field_defn) != 0:
                                        logger.debug("ERROR: Failed to create field: %s" % fld)
                            
                            
                                #### build list of catalogs to search for:
                                catid1s = [(demInfo.catid,demInfo.pairname) for demInfo in contribs]
                                catid2s = [(demInfo.catid2,demInfo.pairname) for demInfo in contribs]
                                catid_list = catid1s+catid2s
                                catids = dict(catid_list)
                                
                                
                                #### Set mfp selection (search for product code P1BS only))
                                mfp_lyr.SetAttributeFilter("PROD_CODE = 'P1BS'")
                                
                                #### Iterate mfp selection
                                mfp_lyr.ResetReading()
                                image_count=0
                                for feat in mfp_lyr:
                                    #### test if catalog_id in catid dict
                                    i = feat.GetFieldIndex("CATALOG_ID")
                                    if i != -1:
                                        feat_catid = feat.GetFieldAsString(i)
                                    else:
                                        logger.error("{} field not found in mfp".format("CATALOG_ID"))
                                        break
                                    
                                    if feat_catid in catids:
                                        image_count+=1
                                        #### add feat to stereo imagery
                                        
                                        i = feat.GetFieldIndex("SCENE_ID")
                                        if i != -1:
                                            feat_sceneid = feat.GetFieldAsString(i)
                                        
                                        i = feat.GetFieldIndex("STRIP_ID")
                                        if i != -1:
                                            feat_stripid = feat.GetFieldAsString(i)
                                        
                                        i = feat.GetFieldIndex("SENSOR")
                                        if i != -1:
                                            feat_sensor = feat.GetFieldAsString(i)
                                            
                                        i = feat.GetFieldIndex("ACQ_TIME")
                                        if i != -1:
                                            feat_acqtime = feat.GetFieldAsString(i)
                                        
                                        i = feat.GetFieldIndex("PROD_CODE")
                                        if i != -1:
                                            feat_prodcode = feat.GetFieldAsString(i)
                                        
                                        i = feat.GetFieldIndex("O_FILENAME")
                                        if i != -1:
                                            feat_ofilename = feat.GetFieldAsString(i)
                                        
                                        i = feat.GetFieldIndex("O_FILEPATH")
                                        if i != -1:
                                            feat_ofilepath = feat.GetFieldAsString(i)
                                        
                                        i = feat.GetFieldIndex("O_DRIVE")
                                        if i != -1:
                                            feat_odrive = feat.GetFieldAsString(i)
                                            
                                        i = feat.GetFieldIndex("S_FILEPATH")
                                        if i != -1:
                                            feat_sfilepath = feat.GetFieldAsString(i)
                                            
                                        geom = feat.GetGeometryRef().Clone()
                                        if not geom.IsValid():
                                            logger.info( "Feature {} does not have a valid geometry".format(feat_sceneid))
                                        
                                        new_feat = ogr.Feature(lyr.GetLayerDefn())
                                
                                        new_feat.SetField("SCENE_ID",feat_sceneid)
                                        new_feat.SetField("STRIP_ID",feat_stripid)
                                        new_feat.SetField("CATALOG_ID",feat_catid)
                                        new_feat.SetField("PAIRNAME",catids[feat_catid])
                                        new_feat.SetField("SENSOR",feat_sensor)
                                        new_feat.SetField("ACQ_TIME",feat_acqtime)
                                        new_feat.SetField("PROD_CODE",feat_prodcode)
                                        new_feat.SetField("O_FILENAME",feat_ofilename)
                                        new_feat.SetField("O_FILEPATH",feat_ofilepath)
                                        new_feat.SetField("O_DRIVE",feat_odrive)
                                        new_feat.SetField("S_FILEPATH",feat_sfilepath)
                                        new_feat.SetField("REGION",feature_id)
                
                                        new_feat.SetGeometry(geom)
                                        if lyr.CreateFeature(new_feat) != 0:
                                            logger.debug("ERROR: Could not create feature for image {}".format(feat_sceneid))
                                        else:
                                            logger.debug("Created feature for image {}".format(feat_sceneid))
                                            
                                        new_feat.Destroy()
                                        

                                ds = None
                                
                                logger.info("Number contributing images from mfp: {}".format(image_count))
                            mfp = None
                        
                    
                    #### Write textfiles
                    if not os.path.isdir(dstdir):
                        os.makedirs(dstdir)
                    
                    txt = open(txtpath,'w')
                    catid_txt = open(txtpath_catalogids,'w')
                    
                    for contrib in contribs:
                        txt.write("%s\n" %contrib.pairname)
                        catid_txt.write("{}\n{}\n".format(contrib.catid,contrib.catid2))
    
                    txt.close()
                    catid_txt.close()

                    feature_selection.update(set([contrib.pairname for contrib in contribs]))
    
    return feature_selection


def get_source_names(src_fp):
    """Get the source footprint name and layer name, if provided"""

    if src_fp.lower().endswith(".shp"):
        src_dsp = src_fp
        src_lyr = os.path.splitext(os.path.basename(src_fp))[0]
    elif ".gdb" in src_fp.lower() and not src_fp.lower().endswith(".gdb"):
        src_dsp, src_lyr = re.split(r"(?<=\.gdb)/", src_fp, re.I)
    else:
        msg = "The source {} does not appear to be a shapefile or File GDB".format(src_fp)
        raise RuntimeError(msg)

    return (src_dsp, src_lyr)

if __name__ == '__main__':
    main()