import os, string, sys, logging, argparse, numpy
from datetime import datetime

import gdal, ogr,osr, gdalconst

from lib.mosaic import *
from lib import ortho_utils


### Create Logger
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)              

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
    parser.add_argument("-d", "--dem", action="store_true", default=False,
                      help="if a DEM is use in image processing")
    
    
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
        logfile = os.path.join(os.path.dirname(dstdir),"queryFP_%s.log" %datetime.today().strftime("%Y%m%d%H%M%S"))
    lfh = logging.FileHandler(logfile)
    #lfh = logging.StreamHandler()
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)
    
    #### Get exclude_list if specified
    if args.exclude is not None:
        if not os.path.isfile(args.exclude):
            parser.error("Value for option --exclude-list is not a valid file")
        
        f = open(args.exclude, 'r')
        exclude_list = set([line.rstrip() for line in f.readlines()])
    else:
        exclude_list = set()
    
    logger.info("Exclude list: %s" %str(exclude_list))
    
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
                print "Target tile is not in the tile csv: %s" %ttile
                
            else:
                t = tiles[ttile]
                if t.status == "0":
                    logger.error("Tile status indicates it should not be created: %s, %s" %(ttile,t.status))
                    print "Tile status indicates it should not be created: %s, %s" %(ttile,t.status)
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
        print ("Tile %s processing files already exist" %t.name)
    else:
        logger.info("Tile %s" %(t.name))
    
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
            #lyr.SetAttributeFilter('STATUS = "online"')
            
            s_srs = lyr.GetSpatialRef()
            #logger.info(str( s_srs))
            logger.info(str(t.geom))
        
            if not t_srs.IsSame(s_srs):
                ict = osr.CoordinateTransformation(t_srs, s_srs)
                ct = osr.CoordinateTransformation(s_srs, t_srs)
            
            lyr.ResetReading()
            feat = lyr.GetNextFeature()
            
            image_list = []
            
            while feat:
                
                i = feat.GetFieldIndex("S_FILEPATH")
                if i == -1:
                    i = feat.GetFieldIndex("O_FILEPATH")
                elif len(feat.GetFieldAsString(i)) == 0:
                    i = feat.GetFieldIndex("O_FILEPATH")
                path = feat.GetFieldAsString(i)
                i = feat.GetFieldIndex("CLOUDCOVER")
                cc = feat.GetFieldAsDouble(i)
                i = feat.GetFieldIndex("SENSOR")
                sensor = feat.GetFieldAsString(i)
                i = feat.GetFieldIndex("SCENE_ID")
                scene_id = feat.GetFieldAsString(i)
                geom = feat.GetGeometryRef()
                
                #print (scene_id)
                if geom is not None and geom.GetGeometryType() == ogr.wkbPolygon:
                    if not t_srs.IsSame(s_srs):
                        geom.Transform(ct)
                    if geom.Intersect(t.geom):
                        #print (scene_id, path)
                        if len(path) > 1:
                            if r"V:/pgc/agic/private" in path:
                                srcfp = path.replace(r"V:/pgc",r'/mnt/agic/storage00')
                            elif r"/pgc/agic/private" in path:
                                srcfp = path.replace(r"/pgc",r'/mnt/agic/storage00')
                            elif r"V:/pgc/data" in path:
                                srcfp = path.replace(r"V:/pgc/data",r'/mnt/pgc/data')
                            elif r"/pgc/data" in path:
                                srcfp = path.replace(r"/pgc/data",r'/mnt/pgc/data')
                            else:
                                srcfp = path
                            
                            if scene_id in exclude_list:
                                logger.info("Scene in exclude list: %s" %srcfp)
                            elif not os.path.isfile(srcfp):
                                logger.warning("Intersecting image not found on file system: %s" %srcfp)
                            else:
                                logger.info( "INTERSECT %s, %s: %s" %(scene_id, srcfp, str(geom)))
                                image_list.append(srcfp)
                        else:
                            logger.warning("path field not found in shp")
                                
                            
                    #else:
                        #logger.info( "No polygon geometry: %s" %path)
                feat = lyr.GetNextFeature()
            
            ds = None
        
            logger.info("Number of intersects in tile %s: %i" %(t.name,len(image_list)))
            print "Number of intersects in tile %s: %i" %(t.name,len(image_list))
            
           
            
            if len(image_list) > 0:
                if args.nosort is False:
                
                    #### gather image info list
                    logger.info("Gathering image info")
                    imginfo_list1 = [ImageInfo(image,"raw",logger) for image in image_list]
                    
                     #### Get mosaic parameters
                    logger.info("Getting mosaic parameters")
                    params = getMosaicParameters(imginfo_list1[0],args)
                    
                    #### Remove images that do not match ref
                    logger.info("Setting image pattern filter")
                    imginfo_list2 = filterMatchingImages(imginfo_list1,params,logger)
                    
                    logger.info("Number of images matching filter: %i" %(len(imginfo_list2)))
                    print ("Number of images matching filter: %i" %(len(imginfo_list2)))
                    
                    #### Get RPC projected geom for each image
                    logger.info("Getting RPC geom")
                    
                    imginfo_list3 = []
                    for iinfo in imginfo_list2:
                        geom = getRpcGeom(iinfo,args.dem,t_srs)
                        if geom is not None:
                            logger.info("%s geom: %s" %(iinfo.srcfn,str(geom)))
                            iinfo.geom = geom
                            imginfo_list3.append(iinfo)
                        else:
                            logger.warning("Cannot determine geom: %s" %iinfo.srcfp)
                        
                    #### Sort by quality
                    logger.info("Sorting images by quality")
                    
                    imginfo_list4 = []
                    for iinfo in imginfo_list3:
                        
                        iinfo.score,iinfo.factors = iinfo.getScore(params,logger)
                        if iinfo.score > 0:
                            imginfo_list4.append(iinfo)
                    
                    imginfo_list4.sort(key=lambda x: x.score)
                    
                    #######################################################
                    #### Create RPC Shp
                    
                    shp = os.path.join(dstdir,"%s_%s_imagery.shp" %(os.path.basename(csvpath)[:-4],t.name))
               
                    logger.info("Creating shapefile of geoms: %s" %shp)
                
                    fields = [("IMAGENAME", ogr.OFTString, 100),
                        ("SCORE", ogr.OFTReal, 0)]
                    
                    OGR_DRIVER = "ESRI Shapefile"
                    
                    ogrDriver = ogr.GetDriverByName(OGR_DRIVER)
                    if ogrDriver is None:
                        logger.info("OGR: Driver %s is not available" % OGR_DRIVER)
                        sys.exit(-1)
                    
                    if os.path.isfile(shp):
                        ogrDriver.DeleteDataSource(shp)
                    vds = ogrDriver.CreateDataSource(shp)
                    if vds is None:
                        logger.info("Could not create shp")
                        sys.exit(-1)
                    
                    shpd, shpn = os.path.split(shp)
                    shpbn, shpe = os.path.splitext(shpn)
                    
                    
                    lyr = vds.CreateLayer(shpbn, t_srs, ogr.wkbPolygon)
                    if lyr is None:
                        logger.info("ERROR: Failed to create layer: %s" % shpbn)
                        sys.exit(-1)
                    
                    for fld, fdef, flen in fields:
                        field_defn = ogr.FieldDefn(fld, fdef)
                        if fdef == ogr.OFTString:
                            field_defn.SetWidth(flen)
                        if lyr.CreateField(field_defn) != 0:
                            logger.info("ERROR: Failed to create field: %s" % fld)
                    
                    for iinfo in imginfo_list4:
                        geom = iinfo.geom
                        
                        logger.info("Image: %s" %(iinfo.srcfn))
                        
                        feat = ogr.Feature(lyr.GetLayerDefn())
                        
                        feat.SetField("IMAGENAME",iinfo.srcfn)
                        feat.SetField("SCORE",iinfo.score)
                            
                        feat.SetGeometry(geom)
                        
                        if lyr.CreateFeature(feat) != 0:
                            logger.info("ERROR: Could not create feature for image %s" % image)
                        else:
                            logger.info("Created feature for image: %s" %image)
                            
                        feat.Destroy()
                    
                    
                    ####  Overlay geoms and remove non-contributors
                    logger.info("Overlaying images to determine contributors")
                    contribs = []
                    
                    for i in xrange(0,len(imginfo_list4)):
                        iinfo = imginfo_list4[i]
                        basegeom = iinfo.geom
    
                        for j in range(i+1,len(imginfo_list4)):
                            iinfo2 = imginfo_list4[j]
                            geom2 = iinfo2.geom
                            
                            if basegeom.Intersects(geom2):
                                basegeom = basegeom.Difference(geom2)
                                if basegeom is None or basegeom.IsEmpty():
                                    #logger.info("Broke after %i comparisons" %j)
                                    break
                                    
                        if basegeom is None:
                            logger.info("Function Error: %s" %iinfo.srcfp)
                        elif basegeom.IsEmpty():
                            logger.info("Removing non-contributing image: %s" %iinfo.srcfp)
                        else:
                            basegeom = basegeom.Intersection(t.geom)
                            if basegeom is None:
                                logger.info("Function Error: %s" %iinfo.srcfp)
                            elif basegeom.IsEmpty():
                                logger.info("Removing non-contributing image: %s" %iinfo.srcfp)
                            else:
                                contribs.append(iinfo.srcfp)
                                                
                elif args.nosort is True:
                    contribs = image_list
            
                logger.info("Number of contributing images: %i" %(len(contribs)))      
                print "Number of contributing images: %i" %(len(contribs))
            
                if len(contribs) > 0:
                    
                    #### Write textfiles
                    if not os.path.isdir(dstdir):
                        os.makedirs(dstdir)
                    
                    otxtpath = os.path.join(dstdir,"%s_%s_orig.txt" %(os.path.basename(csvpath)[:-4],t.name))
                    mtxtpath = os.path.join(dstdir,"%s_%s_ortho.txt" %(os.path.basename(csvpath)[:-4],t.name))
                    
                    otxt = open(otxtpath,'w')
                    mtxt = open(mtxtpath,'w')
                    
                    for contrib in contribs:
                        
                        if os.path.isfile(contrib):
                        
                            otxt.write("%s\n" %contrib)
                            if "\\" in contrib:
                                fn = contrib[contrib.rfind("\\")+ 1:contrib.rfind(".")]
                            else:
                                fn = contrib[contrib.rfind("/")+ 1:contrib.rfind(".")]
                            
                            if args.dem is not None:
                                mtxt.write(os.path.join(dstdir,"orthos",t.name,"ortho%s_u08%s%i.tif\n"% (fn, args.stretch, t.epsg)))
                            else:
                                mtxt.write(os.path.join(dstdir,"orthos",t.name,"%s_u08%s%i.tif\n"% (fn, args.stretch, t.epsg)))
                            
                        else:
                            logger.warning("Image does not exist: %s" %(contrib))
                            
                    otxt.close()
                    mtxt.close()




if __name__ == '__main__':
    main()