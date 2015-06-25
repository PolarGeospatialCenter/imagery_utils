import os, string, sys, shutil, glob, re, tarfile,argparse, numpy, logging
from datetime import datetime, timedelta
from subprocess import *
from math import *
from xml.etree import cElementTree as ET

from lib.mosaic import *
import gdal, ogr,osr,gdalconst

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


def main():
    
    #########################################################
    ####  Handle args
    #########################################################

    #### Set Up Arguments 
    parent_parser = buildMosaicParentArgumentParser()
    parser = argparse.ArgumentParser(
	parents=[parent_parser],
	description="Create cutline or component shapefile"
	)
    
    parser.add_argument("shp", help="output shapefile name")
    parser.add_argument("src", help="textfile or directory of input rasters (tif only)")
    
    parser.add_argument("--cutline_step", type=int, default=2,
                       help="cutline calculator pixel skip interval (default=2)")
    parser.add_argument("--component_shp", action="store_true", default=False,
                        help="create shp of all component images")
   
    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    
    inpath = os.path.abspath(args.src)
    shp = os.path.abspath(args.shp)
    
    #print (" ".join(sys.argv))
    
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
    logfile = os.path.splitext(shp)[0]+".log"
    lfh = logging.FileHandler(logfile)
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)    

    stm = datetime.today()
    logger.info("Start Time: %s\n" %(stm))
    
    minx = args.extent[0]
    maxx = args.extent[1]
    miny = args.extent[2]
    maxy = args.extent[3]
    poly_wkt = 'POLYGON (( %s %s, %s %s, %s %s, %s %s, %s %s ))' %(minx,miny,minx,maxy,maxx,maxy,maxx,miny,minx,miny)
    extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)
    
    if os.path.isfile(shp):
        logger.info("Cutlines shapefile already exists: %s" %shp)
    else:
    
        intersects = []
        t = open(inpath,'r')
        for line in t.readlines():
            if os.path.isfile(line.rstrip('\n').rstrip('\r')):
                intersects.append(line.rstrip('\n').rstrip('\r'))
            else:
                logger.warning("Imagepath in intersects textfile does not exist: %s" %line.rstrip('\n').rstrip('\r'))
        t.close()
        
        if len(intersects) == 0:
            logger.error("No images found: %s" %inpath)
            sys.exit()
        else:
            logger.info("Number of intersecting images: %i" %len(intersects))
        
        
        #### gather image info list
        logger.info("Gathering image info")
        imginfo_list = [ImageInfo(image,"IMAGE") for image in intersects]
        
        #### Get mosaic parameters
        logger.info("Getting mosaic parameters")
        params = getMosaicParameters(imginfo_list[0],args)
        logger.info("Mosaic extent: %f %f %f %f" %(params.xmin, params.xmax, params.ymin, params.ymax))
        logger.info("Mosaic tilesize: %f %f" %(params.xtilesize, params.ytilesize))
        logger.info("Mosaic resolution: %.10f %.10f" %(params.xres, params.yres))
        logger.info("Mosaic projection: %s" %(params.proj))
        
        logger.info("Getting Exact Image geometry")
        
        imginfo_list2 =[]
        for iinfo in imginfo_list:
            simplify_tolerance = 2.0 * ((params.xres + params.yres) / 2.0) ## 2 * avg(xres, yres), should be 1 for panchromatic mosaics where res = 0.5m
            geom,xs1,ys1 = GetExactTrimmedGeom(iinfo.srcfp,step=args.cutline_step,tolerance=simplify_tolerance)
                
            if geom is None:
                logger.info("%s: geometry could not be determined" %iinfo.srcfn)
            elif geom.IsEmpty():
                logger.info("%s: geometry is empty" %iinfo.srcfn)
            else:
                iinfo.geom = geom
                tm = datetime.today()
                imginfo_list2.append(iinfo)
                centroid = geom.Centroid()
                logger.info("%s: geometry acquired - centroid: %f, %f" %(iinfo.srcfn, centroid.GetX(), centroid.GetY()))
        
        logger.info("Calculating image scores")
        for iinfo in imginfo_list2:
            iinfo.getScore(params)
            logger.info("%s: %s" %(iinfo.srcfn,iinfo.score))
               
        ####  Overlay geoms and remove non-contributors
        if args.component_shp:
            contribs = [(iinfo,iinfo.geom) for iinfo in imginfo_list2]
                
        else:
            
            logger.info("Overlaying images to determine contributors")
            contribs = []
            
            for i in xrange(0,len(imginfo_list2)):
                iinfo = imginfo_list2[i]
                basegeom = iinfo.geom
            
                for j in range(i+1,len(imginfo_list2)):
                    iinfo2 = imginfo_list2[j]
                    geom2 = iinfo2.geom
                    
                    if basegeom.Intersects(geom2):
                        basegeom = basegeom.Difference(geom2)
                        if basegeom is None or basegeom.IsEmpty():
                            break
                            
                if basegeom is None:
                    logger.info("Function Error: %s" %iinfo.srcfn)
                elif basegeom.IsEmpty():
                    logger.info("Removing non-contributing image: %s" %iinfo.srcfn)
                else:
                    basegeom = basegeom.Intersection(extent_geom)
                    if basegeom is None:
                        logger.info("Function Error: %s" %iinfo.srcfn)
                    elif basegeom.IsEmpty():
                        logger.info("Removing non-contributing image: %s" %iinfo.srcfn)
                    else:
                        contribs.append((iinfo,basegeom))
                        tm = datetime.today()    
                        logger.info("Image: %s" %(os.path.basename(image)))
        
        logger.info("Number of contributors: %d" %len(contribs))
        
        #######################################################
        #### Create Shp      
   
        logger.info("Creating shapefile of image boundaries: %s" %shp)
    
        fields = (
            ("IMAGENAME", ogr.OFTString, 100),
            ("SENSOR", ogr.OFTString, 10),
            ("ACQDATE", ogr.OFTString, 10),
            ("CAT_ID", ogr.OFTString, 30),
            ("RESOLUTION", ogr.OFTReal, 0),
            ("OFF_NADIR", ogr.OFTReal, 0),
            ("SUN_ELEV", ogr.OFTReal, 0),
            ("CLOUDCOVER", ogr.OFTReal, 0),
            ("TDI", ogr.OFTReal, 0),
            ("DATE_DIFF", ogr.OFTReal, 0),
            ("SCORE", ogr.OFTReal, 0),
        )

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
        
        rp = osr.SpatialReference()
        rp.ImportFromWkt(params.proj)
        
        lyr = vds.CreateLayer(shpbn, rp, ogr.wkbPolygon)
        if lyr is None:
            logger.info("ERROR: Failed to create layer: %s" % shpbn)
            sys.exit(-1)
        
        for fld, fdef, flen in fields:
            field_defn = ogr.FieldDefn(fld, fdef)
            if fdef == ogr.OFTString:
                field_defn.SetWidth(flen)
            if lyr.CreateField(field_defn) != 0:
                logger.info("ERROR: Failed to create field: %s" % fld)
        
        for iinfo,geom in contribs:
            
            logger.info("Image: %s" %(iinfo.srcfn))
            
            feat = ogr.Feature(lyr.GetLayerDefn())
            
            feat.SetField("IMAGENAME",iinfo.srcfn)
            feat.SetField("SENSOR",iinfo.sensor)
            feat.SetField("ACQDATE",iinfo.acqdate.strftime("%Y-%m-%d"))
            feat.SetField("CAT_ID",iinfo.catid)
            feat.SetField("OFF_NADIR",iinfo.ona)
            feat.SetField("SUN_ELEV",iinfo.sunel)
            feat.SetField("CLOUDCOVER",iinfo.cloudcover)
            feat.SetField("SCORE",iinfo.score)
            
            tdi = iinfo.tdi if iinfo.tdi else 0
            feat.SetField("TDI",tdi)
            
            date_diff = iinfo.date_diff if iinfo.date_diff else -9999
            feat.SetField("DATE_DIFF",date_diff)
            
            res = ((iinfo.xres+iinfo.yres)/2.0) if iinfo.xres else 0
            feat.SetField("RESOLUTION",res)
                
            feat.SetGeometry(geom)
            
            if lyr.CreateFeature(feat) != 0:
                logger.info("ERROR: Could not create feature for image %s" % image)
            else:
                logger.info("Created feature for image: %s" %image)
                
            feat.Destroy()
            
            
    etm = datetime.today()
    td = (etm-stm)
    logger.info("Total Processing Time: %s\n" %(td))


if __name__ == '__main__':
    main()
