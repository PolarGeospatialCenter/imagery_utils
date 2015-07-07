import os, string, sys, logging, argparse, numpy
from datetime import datetime

import gdal, ogr,osr, gdalconst

from lib.mosaic import *
from lib import ortho_utils


### Create Logger
logger = logging.getLogger("logger")

EPSG_WGS84 = 4326

def main():
    
    #### Set Up Arguments 
    parent_parser = buildMosaicParentArgumentParser()
    parser = argparse.ArgumentParser(
	parents=[parent_parser],
	description="query PGC index for images contributing to a mosaic"
	)
    
    parser.add_argument("index", help="PGC index shapefile")
    parser.add_argument("tile_csv", help="tile schema csv")
    parser.add_argument("dstdir", help="textfile output directory")
    #pos_arg_keys = ["index","tile_csv","dstdir"]
    
    parser.add_argument("--log",
                      help="output log file (default is queryFP.log in the output folder)")
    parser.add_argument("--ttile",
                      help="target tile (default is to compute all valid tiles. multiple tiles should be delimited by a comma [ex: 23_24,23_25])")
    parser.add_argument("--overwrite", action="store_true", default=False,
                      help="overwrite any existing files")
    parser.add_argument("--stretch", choices=ortho_utils.stretches, default="rf",
                      help="stretch abbreviation used in image processing (default=rf)")
    parser.add_argument("--build_shp", action='store_true', default=False,
                      help="build shapefile of intersecting images (only invoked if --no_sort is not used)")
    parser.add_argument("--online_only", action='store_true', default=False,
                      help="limit search to those records where status = online and image is found on the file system")
    
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    
    shp = os.path.abspath(args.index)
    csvpath = os.path.abspath(args.tile_csv)
    dstdir = os.path.abspath(args.dstdir)
    
    #### Validate Required Arguments
    if not os.path.isfile(shp):
        parser.error("Arg1 is not a valid file path: %s" %shp)
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
    
    logger.debug("Exclude list: %s" %str(exclude_list))
    
    #### Parse csv, validate tile ID and get tilegeom
    tiles = {}
    csv = open(csvpath,'r')
    for line in csv:
        tile = line.rstrip().split(",")
        if len(tile) != 9:
            logger.warning("funny csv line: %s" %line.strip('\n'))
        else:
            name = tile[2]
            if name != "name":
                ### Tile csv schema: row, column, name, status, xmin, xmax, ymin, ymax, epsg code
                t = TileParams(float(tile[4]),float(tile[5]),float(tile[6]),float(tile[7]),int(tile[0]),int(tile[1]),tile[2])
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
                logger.info("Target tile is not in the tile csv: %s" %ttile)
                
            else:
                t = tiles[ttile]
                if t.status == "0":
                    logger.error("Tile status indicates it should not be created: %s, %s" %(ttile,t.status))
                else:
                    HandleTile(t,shp,dstdir,csvpath,args,exclude_list)
    
    else:
        keys = tiles.keys()
        keys.sort()
        for tile in keys:
            t = tiles[tile]
            if t.status == "1":
                HandleTile(t,shp,dstdir,csvpath,args,exclude_list)   
        
        
def HandleTile(t,shp,dstdir,csvpath,args,exclude_list):
    
    
    otxtpath = os.path.join(dstdir,"%s_%s_orig.txt" %(os.path.basename(csvpath)[:-4],t.name))
    mtxtpath = os.path.join(dstdir,"%s_%s_ortho.txt" %(os.path.basename(csvpath)[:-4],t.name))
    
    if os.path.isfile(otxtpath) and os.path.isfile(mtxtpath) and args.overwrite is False:
        logger.info("Tile %s processing files already exist" %t.name)
    else:
        logger.debug("Tile %s" %(t.name))
    
        t_srs = osr.SpatialReference()
        t_srs.ImportFromEPSG(t.epsg)
        
        #### Open Shp
        shpd, shpn = os.path.split(shp)
        shpbn, shpe = os.path.splitext(shpn)
        
        ds = ogr.Open(shp)
        if ds is None:
            logger.warning("Open failed")
            
        else:
            lyr = ds.GetLayerByName( shpbn )
            
            #### attribute filter for online images
            if args.online_only:
                lyr.SetAttributeFilter('STATUS = "online"')
            
            s_srs = lyr.GetSpatialRef()
            #logger.debug(str(s_srs))
            logger.debug(str(t.geom))
        
            if not t_srs.IsSame(s_srs):
                ict = osr.CoordinateTransformation(t_srs, s_srs)
                ct = osr.CoordinateTransformation(s_srs, t_srs)
            
            lyr.ResetReading()
            feat = lyr.GetNextFeature()
            
            imginfo_list1 = []
            
            while feat:
                
                iinfo = ImageInfo(feat,"RECORD",srs=s_srs)
                
                if iinfo.geom is not None and iinfo.geom.GetGeometryType() == ogr.wkbPolygon:
                    if not t_srs.IsSame(s_srs):
                        iinfo.geom.Transform(ct)
                    
                    if iinfo.geom.Intersect(t.geom):
                        
                        if iinfo.scene_id in exclude_list:
                            logger.debug("Scene in exclude list, excluding: %s" %iinfo.srcfp)
                            
                        elif args.online_only and not os.path.isfile(iinfo.srcfp):
                            logger.warning("Scene does not exist, excluding: {0}".format(iinfo.srcfp))
                            
                        else:
                            logger.debug( "Intersect %s, %s: %s" %(iinfo.scene_id, iinfo.srcfp, str(iinfo.geom)))
                            imginfo_list1.append(iinfo)                                
                            
                feat = lyr.GetNextFeature()
            
            ds = None
        
            logger.info("Number of intersects in tile %s: %i" %(t.name,len(imginfo_list1)))
            
            if len(imginfo_list1) > 0:
                if args.nosort is False:
                
                    #### Get mosaic parameters
                    logger.debug("Getting mosaic parameters")
                    params = getMosaicParameters(imginfo_list1[0],args)
                    
                    #### Remove images that do not match ref
                    logger.debug("Setting image pattern filter")
                    imginfo_list2 = filterMatchingImages(imginfo_list1,params)
                    logger.info("Number of images matching filter: %i" %(len(imginfo_list2)))
                    
                    #### Sort by quality
                    logger.debug("Sorting images by quality")
                    imginfo_list3 = []
                    for iinfo in imginfo_list2:
                        
                        iinfo.getScore(params)
                        if iinfo.score > 0:
                            imginfo_list3.append(iinfo)
                    
                    imginfo_list3.sort(key=lambda x: x.score)
                    
                    if args.build_shp:
                        
                        #######################################################
                        #### Create Shp
                        
                        shp = os.path.join(dstdir,"%s_%s_imagery.shp" %(os.path.basename(csvpath)[:-4],t.name))
                   
                        logger.debug("Creating shapefile of geoms: %s" %shp)
                    
                        fields = [("IMAGENAME", ogr.OFTString, 100),
                            ("SCORE", ogr.OFTReal, 0)]
                        
                        OGR_DRIVER = "ESRI Shapefile"
                        
                        ogrDriver = ogr.GetDriverByName(OGR_DRIVER)
                        if ogrDriver is None:
                            logger.debug("OGR: Driver %s is not available" % OGR_DRIVER)
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
                            logger.debug("ERROR: Failed to create layer: %s" % shpbn)
                            sys.exit(-1)
                        
                        for fld, fdef, flen in fields:
                            field_defn = ogr.FieldDefn(fld, fdef)
                            if fdef == ogr.OFTString:
                                field_defn.SetWidth(flen)
                            if lyr.CreateField(field_defn) != 0:
                                logger.debug("ERROR: Failed to create field: %s" % fld)
                        
                        for iinfo in imginfo_list3:
                        
                            logger.debug("Image: %s" %(iinfo.srcfn))
                            
                            feat = ogr.Feature(lyr.GetLayerDefn())
                            
                            feat.SetField("IMAGENAME",iinfo.srcfn)
                            feat.SetField("SCORE",iinfo.score)
    
                            feat.SetGeometry(iinfo.geom)
                            if lyr.CreateFeature(feat) != 0:
                                logger.debug("ERROR: Could not create feature for image %s" % iinfo.srcfn)
                            else:
                                logger.debug("Created feature for image: %s" %iinfo.srcfn)
                                
                            feat.Destroy()
                    
                    
                    ####  Overlay geoms and remove non-contributors
                    logger.debug("Overlaying images to determine contributors")
                    contribs = []
                    
                    for i in xrange(0,len(imginfo_list3)):
                        iinfo = imginfo_list3[i]
                        basegeom = iinfo.geom
    
                        for j in range(i+1,len(imginfo_list3)):
                            iinfo2 = imginfo_list3[j]
                            geom2 = iinfo2.geom
                            
                            if basegeom.Intersects(geom2):
                                basegeom = basegeom.Difference(geom2)
                                if basegeom is None or basegeom.IsEmpty():
                                    #logger.debug("Broke after %i comparisons" %j)
                                    break
                                    
                        if basegeom is None:
                            logger.debug("Function Error: %s" %iinfo.srcfp)
                        elif basegeom.IsEmpty():
                            logger.debug("Removing non-contributing image: %s" %iinfo.srcfp)
                        else:
                            basegeom = basegeom.Intersection(t.geom)
                            if basegeom is None:
                                logger.debug("Function Error: %s" %iinfo.srcfp)
                            elif basegeom.IsEmpty():
                                logger.debug("Removing non-contributing image: %s" %iinfo.srcfp)
                            else:
                                contribs.append(iinfo.srcfp)
                                                
                elif args.nosort is True:
                    contribs = image_list
            
                logger.info("Number of contributing images: %i" %(len(contribs)))      
            
                if len(contribs) > 0:
                    
                    #### Write textfiles
                    if not os.path.isdir(dstdir):
                        os.makedirs(dstdir)
                    
                    otxtpath = os.path.join(dstdir,"%s_%s_orig.txt" %(os.path.basename(csvpath)[:-4],t.name))
                    mtxtpath = os.path.join(dstdir,"%s_%s_ortho.txt" %(os.path.basename(csvpath)[:-4],t.name))
                    otxt = open(otxtpath,'w')
                    mtxt = open(mtxtpath,'w')
                    
                    for contrib in contribs:
                        
                        if not os.path.isfile(contrib):
                            logger.warning("Image does not exist: %s" %(contrib))
                            
                        otxt.write("%s\n" %contrib)
                        m_fn = "{0}_u08{1}{2}.tif".format(
                            os.path.splitext(os.path.basename(contrib))[0],
                            args.stretch,
                            t.epsg
                        )
                        
                        mtxt.write(os.path.join(dstdir,'orthos',t.name,m_fn)+"\n")
 
                    otxt.close()




if __name__ == '__main__':
    main()