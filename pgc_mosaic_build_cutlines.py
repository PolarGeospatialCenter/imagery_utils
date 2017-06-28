import os, string, sys, shutil, glob, re, tarfile,argparse, numpy, logging
from datetime import datetime, timedelta, date
from subprocess import *
from math import *
from xml.etree import cElementTree as ET
import gdal, ogr,osr,gdalconst
from lib import mosaic

gdal.SetConfigOption('GDAL_PAM_ENABLED','NO')

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)


def main():
    
    #########################################################
    ####  Handle args
    #########################################################

    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="Create cutline or component shapefile"
    )
    
    parser.add_argument("shp", help="output shapefile name")
    parser.add_argument("src", help="textfile or directory of input rasters (tif only)")
    
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
    parser.add_argument("--use-exposure", action="store_true", default=False,
                        help="use exposure settings in metadata to inform score")
    parser.add_argument("--max-cc", type=float, default=0.5,
                        help="maximum fractional cloud cover (0.0-1.0, default 0.5)")
    parser.add_argument("--median-remove", action="store_true", default=False,
                        help="subtract the median from each input image before forming the mosaic in order to correct for contrast")
    parser.add_argument("--include-all-ms", action="store_true", default=False,
                        help="include all multispectral imagery, even if the imagery has differing numbers of bands")
    parser.add_argument("--cutline-step", type=int, default=2,
                       help="cutline calculator pixel skip interval (default=2)")
    parser.add_argument("--component-shp", action="store_true", default=False,
                        help="create shp of all component images")
    parser.add_argument("--calc-stats", action="store_true", default=False,
                       help="calculate image stats and record them in the index")
   
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
        imginfo_list = [mosaic.ImageInfo(image,"IMAGE") for image in intersects]
        
        #### Get mosaic parameters
        logger.info("Getting mosaic parameters")
        params = mosaic.getMosaicParameters(imginfo_list[0],args)
        logger.info("Mosaic extent: %f %f %f %f" %(params.xmin, params.xmax, params.ymin, params.ymax))
        logger.info("Mosaic resolution: %.10f %.10f" %(params.xres, params.yres))
        logger.info("Mosaic projection: %s" %(params.proj))
        
        logger.info("Getting Exact Image geometry")
        
        imginfo_list2 =[]
        for iinfo in imginfo_list:
            simplify_tolerance = 2.0 * ((params.xres + params.yres) / 2.0) ## 2 * avg(xres, yres), should be 1 for panchromatic mosaics where res = 0.5m
            geom,xs1,ys1 = mosaic.GetExactTrimmedGeom(iinfo.srcfp,step=args.cutline_step,tolerance=simplify_tolerance)
                
            if geom is None:
                logger.warning("%s: geometry could not be determined, verify image is valid" %iinfo.srcfn)
            elif geom.IsEmpty():
                logger.warning("%s: geometry is empty" %iinfo.srcfn)
            else:
                iinfo.geom = geom
                tm = datetime.today()
                imginfo_list2.append(iinfo)
                centroid = geom.Centroid()
                logger.info("%s: geometry acquired - centroid: %f, %f" %(iinfo.srcfn, centroid.GetX(), centroid.GetY()))
                #print geom
                
        if len(imginfo_list) <> len(imginfo_list2):
            logger.error("Some source images do not have valid geometries.  Cannot proceeed")
        
        else:
            logger.info("Getting image metadata and calculating image scores")
            for iinfo in imginfo_list2:
                iinfo.getScore(params)
                if (params.median_remove is True):
                    iinfo.get_raster_median()
                if args.calc_stats:
                    iinfo.get_raster_stats()
                logger.info("%s: %s" %(iinfo.srcfn,iinfo.score))
                
            # Build componenet index
            if args.component_shp:
                
                contribs = [(iinfo,iinfo.geom) for iinfo in imginfo_list2]
                logger.info("Number of contributors: %d" %len(contribs))
                # for iinfo, geom in contribs:
                #     logger.info("Image: %s" %(iinfo.srcfn))
                
                logger.info("Building component index")
                comp_shp = shp.replace('cutlines.shp','components.shp')
                if len(contribs) > 0:
                    build_shp(contribs, comp_shp, args, params)

                else:
                    logger.error("No contributing images")

            # Build cutlines index                    
            ####  Overlay geoms and remove non-contributors
            logger.info("Overlaying images to determine contribution geom")
            contribs = mosaic.determine_contributors(imginfo_list2,extent_geom)
            
            logger.info("Number of contributors: %d" %len(contribs))       
            # for iinfo, geom in contribs:
            #     logger.info("Image: %s" %(iinfo.srcfn))
            
            logger.info("Building cutlines index")
            if len(contribs) > 0:
                build_shp(contribs, shp, args, params)

            else:
                logger.error("No contributing images")
            
    etm = datetime.today()
    td = (etm-stm)
    logger.info("Total Processing Time: %s\n" %(td))


def build_shp(contribs, shp, args, params):
    logger.info("Creating shapefile of image boundaries: %s" %shp)
    
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
    
    if (params.median_remove is True): 
        fields = fields + (
            ("MEDIAN", ogr.OFTString, 80),
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
        
        feat.SetField("IMAGENAME", iinfo.srcfn)
        feat.SetField("SENSOR", iinfo.sensor)
        feat.SetField("ACQDATE", iinfo.acqdate.strftime("%Y-%m-%d"))
        feat.SetField("CAT_ID", iinfo.catid)
        feat.SetField("OFF_NADIR", iinfo.ona)
        feat.SetField("SUN_ELEV" ,iinfo.sunel)
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
                keys = iinfo.stat_dct.keys()
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

        if (params.median_remove is True):
            median_list = []
            keys = iinfo.median.keys()
            keys.sort()
            for band in keys:
                band_median = iinfo.median[band]
                median_list.append(str(band_median))
            feat.SetField("MEDIAN", ",".join(median_list))
            logger.info("median = {}".format(",".join(median_list)))
            
            
        feat.SetGeometry(geom)
        
        if lyr.CreateFeature(feat) != 0:
            logger.info("ERROR: Could not create feature for image %s" % iinfo.srcfn)
        else:
            logger.info("Created feature for image: %s" %iinfo.srcfn)
            
        feat.Destroy()
    

if __name__ == '__main__':
    main()
