import os, string, sys, shutil, math, glob, re, tarfile, logging, platform, argparse, subprocess
from datetime import datetime, timedelta
from xml.etree import cElementTree as ET
import gdal, ogr, osr, gdalconst
import numpy
from numpy import flatnonzero

from lib import utils

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

MODES = ["ALL","MOSAIC","SHP","TEST"]
EXTS = [".tif",".ntf"]
GTIFF_COMPRESSIONS = ["jpeg95","lzw"]

#class Attribs:
#    def __init__(self,dAttribs):
#        self.cc = dAttribs["cc"]
#        self.sunel = dAttribs["sunel"]
#        self.ona = dAttribs["ona"]
#        self.tdi = dAttribs["tdi"]
#        self.alr = dAttribs["alr"]
#        self.exdur = dAttribs["exdur"]
#        self.datediff = dAttribs["datediff"]
#        self.exfact = dAttribs["exfact"]
#        self.panfact = dAttribs["panfact"]



class ImageInfo:
    def __init__(self,src,frmt,srs=None):
        
        self.frmt = frmt  #image format (IMAGE,RECORD)
        
        if frmt == 'IMAGE':
            self.get_attributes_from_file(os.path.abspath(src))
        elif frmt == 'RECORD':
            self.get_attributes_from_record(src,srs)
        else:
            logger.error("Image format must be RECORD or IMAGE")
           
    #self.xsize = None
    #self.ysize = None
    #self.proj = None
    #self.bands = None
    #self.datatype = None
    #self.datatype_readable = None
    #self.xres = None
    #self.yres = None
    #self.geom = None
    #self.sensor = None
    #self.acqdate = None
    #"cc":None,
    #"sunel":None,
    #"ona":None,
    #"date":None,
    #"tdi":None
     
        
    def get_attributes_from_record(self, feat, srs):
        
        i = feat.GetFieldIndex("S_FILEPATH")
        if i <> -1:
            spath = feat.GetFieldAsString(i)
        else:
            logger.error("S_FILEPATH fields does not exist in record")
            spath = None
            
        i = feat.GetFieldIndex("O_FILEPATH")
        if i <> -1:
            opath = feat.GetFieldAsString(i)
        else:
            logger.error("O_FILEPATH fields does not exist in record")
            opath = None
        
        if spath and len(spath) > 1:
            path = spath
        elif opath and len(opath) > 1:
            path = opath
        else: path = ""
        
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
            
        self.srcfp = srcfp
        self.srcdir, self.srcfn = os.path.split(srcfp)
        
        i = feat.GetFieldIndex("COLUMNS")
        if i != -1:
            self.xsize = feat.GetFieldAsDouble(i)
        i = feat.GetFieldIndex("ROWS")
        if i != -1:
            self.ysize = feat.GetFieldAsDouble(i)
        i = feat.GetFieldIndex("BANDS")
        if i != -1:
            self.bands = feat.GetFieldAsDouble(i)
        
        self.proj = srs.ExportToWkt()
        self.xres = None
        self.yres = None
        self.datatype = None
        
        self.sataz = None
        self.satel = None
        self.sunaz = None
        
        i = feat.GetFieldIndex("SUN_ELEV")
        if i != -1:
            self.sunel = feat.GetFieldAsDouble(i)
        i = feat.GetFieldIndex("OFF_NADIR")
        if i != -1:
            self.ona = feat.GetFieldAsDouble(i)
        i = feat.GetFieldIndex("CLOUDCOVER")
        if i != -1:
            self.cloudcover = feat.GetFieldAsDouble(i)
        i = feat.GetFieldIndex("SENSOR")
        if i != -1:
            self.sensor = feat.GetFieldAsString(i)
        i = feat.GetFieldIndex("SCENE_ID")
        if i != -1:
            self.scene_id = feat.GetFieldAsString(i)
        i = feat.GetFieldIndex("CATALOG_ID")
        if i != -1:
            self.catid = feat.GetFieldAsString(i)
        
        i = feat.GetFieldIndex("TDI")
        if i != -1:
            tdi_str = feat.GetFieldAsString(i)
            tdi_list = tdi_str.split('|')
            self.tdi = None
            for item in tdi_list:
                try:
                    if 'pan' in item:
                        self.tdi = int(item[4:])
                    if 'green' in item:
                        self.tdi = int(item[6:])
                    if 'BAND_P' in item:
                        self.tdi = int(item[7:])
                    if 'BAND_G' in item:
                        self.tdi = int(item[7:])
                except ValueError, e:
                    logger.error("cannot parse TDI field for {}: {}".format(self.scene_id,tdi_str))
        
        i = feat.GetFieldIndex("ACQ_TIME")
        if i != -1:
            date_str = feat.GetFieldAsString(i)
            self.acqdate = datetime.strptime(date_str[:19],"%Y-%m-%dT%H:%M:%S")
        
        geom = feat.GetGeometryRef()
        self.geom = geom.Clone()
        
    
    def get_attributes_from_file(self, srcfp):
        self.srcfp = srcfp
        self.srcdir, self.srcfn = os.path.split(srcfp)
        
        ds = gdal.Open(self.srcfp)
        if ds is not None:
            self.xsize = ds.RasterXSize
            self.ysize = ds.RasterYSize
            self.proj = ds.GetProjectionRef() if ds.GetProjectionRef() != '' else ds.GetGCPProjection()
            self.bands = ds.RasterCount
            self.nodatavalue = [ds.GetRasterBand(b).GetNoDataValue() for b in range(1,self.bands+1)]
            self.datatype = ds.GetRasterBand(1).DataType
            self.datatype_readable = gdal.GetDataTypeName(self.datatype)

            gtf = ds.GetGeoTransform()
            num_gcps = ds.GetGCPCount()
            
            if num_gcps == 0:
                
                self.xres = abs(gtf[1])
                self.yres = abs(gtf[5])
                ulx = gtf[0] + 0 * gtf[1] + 0 * gtf[2]
                uly = gtf[3] + 0 * gtf[4] + 0 * gtf[5]
                urx = gtf[0] + self.xsize * gtf[1] + 0 * gtf[2]
                ury = gtf[3] + self.xsize * gtf[4] + 0 * gtf[5]
                llx = gtf[0] + 0 * gtf[1] + self.ysize * gtf[2]
                lly = gtf[3] + 0 * gtf[4] + self.ysize * gtf[5]
                lrx = gtf[0] + self.xsize * gtf[1] + self.ysize* gtf[2]
                lry = gtf[3] + self.xsize * gtf[4] + self.ysize * gtf[5]
                
            
            elif num_gcps == 4:
                
                gcps = ds.GetGCPs()
                gcp_dict = {}
                id_dict = {"UpperLeft":1,
                           "1":1,
                           "UpperRight":2,
                           "2":2,
                           "LowerLeft":4,
                           "4":4,
                           "LowerRight":3,
                           "3":3}
        
                for gcp in gcps:
                    gcp_dict[id_dict[gcp.Id]] = [float(gcp.GCPPixel), float(gcp.GCPLine), float(gcp.GCPX), float(gcp.GCPY), float(gcp.GCPZ)]
        
                ulx = gcp_dict[1][2]
                uly = gcp_dict[1][3]
                urx = gcp_dict[2][2]
                ury = gcp_dict[2][3]
                llx = gcp_dict[4][2]
                lly = gcp_dict[4][3]
                lrx = gcp_dict[3][2]
                lry = gcp_dict[3][3]
        
                self.xres = abs(math.sqrt((ulx - urx)**2 + (uly - ury)**2)/ self.xsize)
                self.yres = abs(math.sqrt((ulx - llx)**2 + (uly - lly)**2)/ self.ysize)
                
            poly_wkt = 'POLYGON (( %.12f %.12f, %.12f %.12f, %.12f %.12f, %.12f %.12f, %.12f %.12f ))' %(ulx,uly,urx,ury,lrx,lry,llx,lly,ulx,uly)
            self.geom = ogr.CreateGeometryFromWkt(poly_wkt)
            self.xs = [ulx,urx,lrx,llx]
            self.ys = [uly,ury,lry,lly]

        else:
            logger.warning("Cannot open image: %s" %self.srcfp)
            self.xsize = None
            self.ysize = None
            self.proj = None
            self.bands = None
            self.datatype = None
            self.datatype_readable = None
            self.xres = None
            self.yres = None

        ds = None
        
        #### Set unknown attribs to None for now
        self.sataz = None
        self.satel = None
        self.sunaz = None
        self.sunel = None
        self.ona = None
        self.cloudcover = None
        self.sensor = None
        self.scene_id = None
        self.catid = None
        self.tdi = None
        self.acqdate = None
 
   
    def get_attributes_from_xml(self):
 
        
        dAttribs = {
            "cc":None,
            "sunel":None,
            "sunaz":None,
            "satel":None,
            "sataz":None,
            "ona":None,
            "date":None,
            "tdi":None,
            "catid":None,
            "sensor":None
        }
    
        dTags = {
            ## DG tags
            "CATID":"catid",
            "SATID":"sensor",
            "CLOUDCOVER":"cc",
            "MEANSUNEL":"sunel",
            "MEANSUNAZ":"sunaz",
            "MEANSATEL":"satel",
            "MEANSATAZ":"sataz",
            "MEANOFFNADIRVIEWANGLE":"ona",
            "FIRSTLINETIME":"date",
            "TDILEVEL":"tdi",
            
            ## GE tags
            "archiveId":"catid",
            "satelliteName":"sensor",
            "percentCloudCover":"cc",
            "firstLineAzimuthAngle":"sataz",
            "firstLineSunAzimuthAngle":"sunaz",
            "firstLineSunElevationAngle":"sunel",
            "firstLineElevationAngle":"satel",
            "firstLineAcquisitionDateTime":"date",
            "tdiMode":"tdi",
            
            ## IK tags
            "Source_Image_ID":"catid",
            "Sensor":"sensor",
            "Percent_Component_Cloud_Cover":"cc",
            "Nominal_Collection_Azimuth":"sataz",
            "Nominal_Collection_Elevation":"satel",
            "Sun_Angle_Elevation":"sunel",
            "Sun_Angle_Azimuth":"sunaz",
            "Acquisition_Date_Time":"date",
            "Pachchromatic_TDI_Mode":"tdi",
            
        }
            
        paths = (
            os.path.splitext(self.srcfp)[0]+'.xml',
            os.path.splitext(self.srcfp)[0]+'.XML',
            os.path.splitext(self.srcfp)[0]+'.txt',
            os.path.splitext(self.srcfp)[0]+'.pvl',
        )
        
        metapath = None
        for path in paths:
            if os.path.isfile(path):
                metapath = path
                break
        
        if not metapath:
            logger.debug("No metadata found for %s" %self.srcfp)
        
        else:
            metad = None
            
            #### if xml format
            if os.path.splitext(metapath)[1].lower() == '.xml':
                try:
                    metad = ET.parse(metapath)
                except ET.ParseError, err:
                    logger.debug("ERROR parsing metadata: %s, %s" %(err,metapath))
            
            else:
                try:
                    metad = getGEMetadataAsXml(metapath)
                except Exception, err:
                    logger.debug("ERROR parsing metadata: %s, %s" %(err,metapath))
                #### Write IK01 code 
        
            if metad is not None:
                
                for tag in dTags:
                    taglist = metad.findall(".//%s"%tag)
                    vallist = []
                    for elem in taglist:
                        
                        text = elem.text
                    
                        if text is not None:
                            try:
                                if tag in [
                                    "Acquisition_Date_Time",
                                    "FIRSTLINETIME",
                                    "firstLineAcquisitionDateTime",
                                    
                                    "CATID",
                                    "archiveId",
                                    
                                    "SATID",
                                ]:
                                    val = text
                                elif tag in ["Source_Image_ID"]:
                                    val = elem.attrib['id']
                                elif tag in ["percentCloudCover", "Percent_Component_Cloud_Cover"]:
                                    val = float(text)/100
                                elif tag in ["Sun_Angle_Azimuth","Sun_Angle_Elevation","Nominal_Collection_Azimuth","Nominal_Collection_Elevation"]:
                                    val = text.strip(" degrees")
                                elif tag == "satelliteName":
                                    val = "GE01"
                                elif tag == "Sensor":
                                    val = "IK01"
                                else:
                                    val = float(text)
                                    
                                vallist.append(val)
                                
                            except Exception, e:
                                logger.debug("Error reading metadata values: %s, %s" %(metapath,e))
                                
                    if dTags[tag] == 'tdi' and len(taglist) > 1:    
                        #### use pan or green band TDI for exposure calculation
                        if len(vallist) == 4:
                            dAttribs['tdi'] = vallist[1]
                        elif len(vallist) == 5 and self.bands == 1: #pan image
                            dAttribs['tdi'] = vallist[4]
                        elif len(vallist) == 5 and self.bands in [3,4]: #multi image
                            dAttribs['tdi'] = vallist[1]
                        elif len(vallist) == 8:
                            dAttribs['tdi'] = vallist[3]
                        else:
                            logger.debug("Unexpected number of TDI values and band count ( TDI: expected 1, 4, 5, or 8 - found %d ; Band cound, expected 1, 4, or 8 - found %d) %s" %(len(vallist), self.bands, metapath))
                    
                    elif dTags[tag] == 'sensor' and len(taglist) > 1:
                        val = vallist[0]
                        dAttribs[dTags[tag]] = val
                    
                    elif len(taglist) == 1:
                        val = vallist[0]
                        dAttribs[dTags[tag]] = val
                        
                    elif len(taglist) > 1:
                        logger.debug("Unexpected number of {} values ({}), {}".format(tag,len(taglist),metapath))
                
                self.sataz = float(dAttribs["sataz"])
                self.satel = float(dAttribs["satel"])
                self.sunaz = float(dAttribs["sunaz"])               
                self.sunel = float(dAttribs["sunel"])
                try:
                    self.ona = dAttribs["ona"] if dAttribs["ona"] else 90 - self.satel
                except TypeError, e:
                    pass
                self.cloudcover = dAttribs["cc"]
                self.sensor = dAttribs["sensor"]
                self.catid = dAttribs["catid"]
                self.tdi = dAttribs["tdi"]
                
                if dAttribs["date"]:
                    try:
                        self.acqdate = datetime.strptime(dAttribs["date"],"%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError, e:
                        try:
                            self.acqdate = datetime.strptime(dAttribs["date"],"%Y-%m-%d %H:%M GMT")
                        except ValueError, e:
                            logger.error("Cannot parse date string {} from {}".format(dAttribs['date'],metapath))


    def getScore(self,params):
        
        score = 0
       
        if not self.catid:
            self.get_attributes_from_xml()
        
        required_attribs = [
            self.sunel,
            self.ona,
            self.cloudcover,
            self.sensor,
        ]
        
        #### Test if all required values were found in metadata search
        status = [val is None for val in required_attribs]
        
        if sum(status) != 0:
            logger.error("Cannot determine score for image {0}:\n  Sun elev\t{1}\n  Off nadir\t{2}\n  Cloudcover\t{3}\n  Sensor\t{4}".format(self.srcfn,self.sunel,self.ona,self.cloudcover,self.sensor))
            score = -1
        
        else:
            
            #### Assign panfactor if pan images are to be included in a multispectral mosaic   
            if self.bands == 1 and params.force_pan_to_multi is True:
                self.panfactor = 0.5
            else:
                self.panfactor = 1
                    
            #### Test if TDI is needed, get exposure factor
            if params.useExposure is True:
                if self.tdi is None:
                    logger.error("Cannot get tdi for image to determine exposure settings: {0}".format(self.srcfp))
                    self.exposure_factor = None
                else:
                    exfact = self.tdi * self.sunel
                    self.exposure_factor = exfact
                    
                    pan_exposure_thresholds = {
                        "WV01":1400,
                        "WV02":1400,
                        "WV03":1400,
                        "QB02":500,
                        #"GE01":,
                    }
                    
                    multi_exposure_thresholds = {
                        "WV02":400,
                        "WV03":400,
                        "GE01":170,
                        "QB02":25,
                    }
                    
                    #### Remove images with high exposure settings (tdi_pan (or tdi_grn) * sunel)
                    if params.bands == 1:
                        if self.sensor in pan_exposure_thresholds:
                            if exfact > pan_exposure_thresholds[self.sensor]:
                                logger.debug("Image overexposed: %s --> %i" %(self.srcfp,exfact))
                                score = -1
                    
                    else:
                        if self.sensor in multi_exposure_thresholds:
                            if exfact > multi_exposure_thresholds[self.sensor]:
                                logger.debug("Image overexposed: %s --> %i" %(self.srcfp,exfact))
                                score = -1
            
            #### Test if acqdate if needed, get date difference
            if params.m != 0:
                if self.acqdate is None:
                    logger.error("Cannot get acqdate for image to determine date-based score: {0}".format(self.srcfn))
                    self.date_diff = -9999
                    
                else:
                    #### Find nearest year for target day
                    tdeltas = []
                    for y in range(self.acqdate.year-1,self.acqdate.year+2):
                        tdeltas.append(abs((datetime(y,params.m,params.d) - self.acqdate).days))
                    
                    self.date_diff = min(tdeltas)
            
            
                #### Assign weights
                ccwt = 30
                sunelwt = 10
                onawt = 5
                datediffwt = 55
                
            else:
                self.date_diff = -9999
                ccwt = 48
                sunelwt = 28
                onawt = 24
                datediffwt = 0

            if params.y != 0:
                if self.acqdate is None:
                    logger.error("Cannot get acqdate for image to determine date-based score: {0}".format(self.srcfn))
                    self.year_diff = -9999

                else:
                    #### find year difference
                    ydeltas = []
                    for yr in params.y:
                        ## if perfect match, no need to check others
                        if yr == self.acqdate.year:
                            ydeltas = [0]
                            continue
                        else:
                            ydeltas.append(abs(yr - self.acqdate.year))

                    if len(ydeltas) > 1:
                        self.year_diff = min(ydeltas)
                    else:
                        self.year_diff = ydeltas[0]

                #### Assign weight
                yeardiffwt = 55
            else:
                self.year_diff = -9999
                yeardiffwt = 0
                
            #### Handle nonesense or nodata cloud cover values
            if self.cloudcover < 0 or self.cloudcover > 1:
                self.cloudcover = params.max_cc
            
            if self.cloudcover > params.max_cc:
                logger.debug("Image too cloudy (>%s): %s --> %f" %(params.max_cc,self.srcfp,self.cloudcover))
                score = -1
            
            #### Handle ridiculously low sun el values, these images will result is spurious TOA values
            if self.sunel < 2:
                logger.debug("Sun elevation too low (<2 degrees): %s --> %f" %(self.srcfp,self.sunel))
                score = -1
                        
            if not score == -1:
                rawscore = ccwt * (1-self.cloudcover) + sunelwt * (self.sunel/90) + onawt * ((90-self.ona)/90.0) + \
                           datediffwt * ((183 - self.date_diff)/183.0) + yeardiffwt * (1.0 / (self.year_diff + 1))
                score = rawscore * self.panfactor  
        
        self.score = score
        return self.score


    def get_raster_stats(self,get_stats=True,get_median=True):
        
        self.stat_dct = {}
        self.datapixelcount_dct = {}
        self.median = {}
        ds = gdal.Open(self.srcfp)
        if ds:
   
            # get raster dimensions
            nx = ds.RasterXSize
            ny = ds.RasterYSize
 
            #### get stats and store in dictionaries
            for bandnum in range(1,self.bands+1):

                # read band
                band = ds.GetRasterBand(bandnum)

                # get nodata value (default to zero)
                band_nodata = band.GetNoDataValue()
                if band_nodata is None:
                    logger.info("Defaulting band {} nodata value to zero".format(bandnum))
                    band_nodata = 0.0

                # read band as a numpy array
                logger.debug("Reading band {} ".format(bandnum))
                band_array = band.ReadAsArray(0,0,nx,ny)

                # generate mask for nodata
                logger.debug("Calculating band {} no data mask".format(bandnum))
                band_nodata_mask = (band_array == band_nodata)
                band_valid = band_array[~band_nodata_mask]
                self.datapixelcount_dct[bandnum] = band_valid.size
                
                ## initialize arrays
                stats = numpy.array([band_nodata,band_nodata,band_nodata,band_nodata],numpy.float64)
                band_median = numpy.float64(band_nodata)
                if band_valid.size == 0: 
                    logger.warning("Band {} contains no valid data".format(bandnum))
                
                ## calc stats  
                else:
                    if get_stats:
                        logger.debug("Calculating band {} min".format(bandnum))
                        band_min = numpy.amin(band_valid)
                        logger.debug("Calculating band {} max".format(bandnum))
                        band_max = numpy.amax(band_valid)
                        logger.debug("Calculating band {} mean".format(bandnum))
                        band_mean = numpy.mean(band_valid,dtype=numpy.float64)
                        logger.debug("Calculating band {} stdev".format(bandnum))
                        band_std = numpy.std(band_valid,dtype=numpy.float64)
                        stats = numpy.array([band_min,band_max,band_mean,band_std],numpy.float64)
                        logger.info("band {} min {}, max {}, mean {}, stdev {}".format(
                            bandnum,band_min,band_max,band_mean,band_std
                        ))
                               
                    if get_median:
                        logger.debug("Calculating band {} median".format(bandnum))
                        band_median = numpy.float64(numpy.median(band_valid))
                        logger.info("band {} median {}".format(bandnum,band_median))                
                
                self.median[bandnum] = band_median
                self.stat_dct[bandnum] = stats
                
                band_valid = None
                band_nodata_mask = None
                band_array = None

            ds = None

        else:
            logger.warning("Cannot open image: %s" %self.srcfp)
  
            
    def set_raster_median(self, median):
        self.median = median
        
        
class DemInfo:
    def __init__(self,src,frmt,srs=None):
        
        self.frmt = frmt  #image format (IMAGE,RECORD)
        self.pairname = None
        self.geom = None
        self.sensor = None
        self.acqdate = None
        self.sunel = None
        self.cloudcover = None
        self.density = None
        self.dem_id = None
        
        if frmt == 'IMAGE':
            self.get_attributes_from_file(src)
        elif frmt == 'RECORD':
            self.get_attributes_from_record(src,srs)
        else:
            logger.error("Image format must be RECORD or IMAGE")
        
        
    def get_attributes_from_record(self, feat, srs):
                
        self.proj = srs.ExportToWkt()
       
        # Fields from DG archive index 
        i = feat.GetFieldIndex("AVSUNELEV")
        if i != -1:
            self.sunel = feat.GetFieldAsDouble(i)
        i = feat.GetFieldIndex("CLOUDCOVER")
        if i != -1:
            self.cloudcover = feat.GetFieldAsDouble(i)/100.0
        i = feat.GetFieldIndex("PLATFORM")
        if i != -1:
            self.sensor = feat.GetFieldAsString(i)
        i = feat.GetFieldIndex("PAIRNAME")
        if i != -1:
            self.pairname = feat.GetFieldAsString(i)
        i = feat.GetFieldIndex("CATALOGID")
        if i != -1:
            self.catid = feat.GetFieldAsString(i)
        i = feat.GetFieldIndex("STEREOPAIR")
        if i != -1:
            self.catid2 = feat.GetFieldAsString(i)
        i = feat.GetFieldIndex("SENSOR")
        if i != -1:
            self.sensor = feat.GetFieldAsString(i)
        
        i = feat.GetFieldIndex("ACQDATE")
        if i != -1:
            date_str = feat.GetFieldAsString(i)
            self.acqdate = datetime.strptime(date_str[:19],"%Y-%m-%d")

        # Fields from SETSM indices
        i = feat.GetFieldIndex("DENSITY")
        if i != -1:
            self.density = feat.GetFieldAsDouble(i)
        i = feat.GetFieldIndex("DEM_ID")
        if i != -1:
            self.dem_id = feat.GetFieldAsString(i)
        
        geom = feat.GetGeometryRef()
        self.geom = geom.Clone()
        

    def getScore(self, target_date=None):
        
        score = 0
      
        required_attribs1 = [
            self.sunel,
            self.cloudcover,
            self.sensor,
        ]

        required_attribs2 = [
            self.density,
            self.dem_id,
            self.sensor,
        ]
        
        #### Test if all required values were found in metadata search
        status1 = [val is None for val in required_attribs1]
        status2 = [val is None for val in required_attribs2]

        if sum(status1) != 0 and sum(status2) != 0:
            logger.error("Cannot determine score for image {0}:\n  Sun elev\t{1}\n  Cloudcover\t{2}\n  Sensor\t{3}\n  Density\t{4}".format(self.pairname,self.sunel,self.cloudcover,self.sensor,self.density)) 
            score = -1
            
        elif self.sensor == 'QB02':
            score = -1
        
        else:
            
            #### Test if acqdate if needed, get date difference
            if target_date:
                if self.acqdate is None:
                    logger.error("Cannot get acqdate for image to determine date-based score: {0}".format(self.srcfn))
                    self.date_diff = -9999
                    
                else:
                    #### Find nearest year for target day
                    tdeltas = []
                    target_month, target_day = target_date[0]
                    for y in range(self.acqdate.year-1,self.acqdate.year+2):
                        tdeltas.append(abs((datetime(y,target_month,target_day) - self.acqdate).days))
                    
                    self.date_diff = min(tdeltas)
            
            
                #### Assign weights
                ccwt = 75
                sunelwt = 5
                datediffwt = 20
                densitywt = 80
                
            else:
                ccwt = 90
                sunelwt = 10
                datediffwt = 0
                densitywt = 100
                self.date_diff = -9999
                
                
            #### Handle nonesense or nodata cloud cover values
            if self.cloudcover:
                if self.cloudcover < 0 or self.cloudcover > 1:
                    self.cloudcover = 0.5
            
                if self.cloudcover > 0.2:
                    logger.debug("Stereopair too cloudy (>20 percent): %s --> %f" %(self.pairname,self.cloudcover))
                    score = -1
            
            #### Handle ridiculously low sun el values
            if self.sunel and self.sunel < 1:
                logger.debug("Sun elevation too low (<1 degrees): %s --> %f" %(self.pairname,self.sunel))
                score = -1
                        
            if not score == -1:
                # determine score method
                if sum(status1) == 0:
                    score = ccwt * (1-self.cloudcover) + sunelwt * (self.sunel/90) + datediffwt * ((183 - self.date_diff)/183.0)
                elif sum(status2) == 0:
                    score = densitywt * self.density + datediffwt * ((183 - self.date_diff)/183.0)
        
        self.score = score
        return self.score

        
class DGInfo:
    def __init__(self,src,frmt,srs=None):
        
        self.frmt = frmt  #image format (IMAGE,RECORD)
        self.pairname = None
        self.geom = None
        self.sensor = None
        self.acqdate = None
        self.sunel = None
        self.cloudcover = None
        self.density = None
        self.dem_id = None
        
        if frmt == 'RECORD':
            self.get_attributes_from_record(src,srs)
        else:
            logger.error("Image format must be RECORD")
        
        
    def get_attributes_from_record(self, feat, srs):
                
        self.proj = srs.ExportToWkt()
       
        # Fields from DG archive index 
        i = feat.GetFieldIndex("AVSUNELEV")
        if i != -1:
            self.sunel = feat.GetFieldAsDouble(i)
        i = feat.GetFieldIndex("CLOUDCOVER")
        if i != -1:
            self.cloudcover = feat.GetFieldAsDouble(i)/100.0
        i = feat.GetFieldIndex("PLATFORM")
        if i != -1:
            self.sensor = feat.GetFieldAsString(i)
        i = feat.GetFieldIndex("CATALOGID")
        if i != -1:
            self.catid = feat.GetFieldAsString(i)
        i = feat.GetFieldIndex("SENSOR")
        if i != -1:
            self.sensor = feat.GetFieldAsString(i)
        
        i = feat.GetFieldIndex("ACQDATE")
        if i != -1:
            date_str = feat.GetFieldAsString(i)
            self.acqdate = datetime.strptime(date_str[:19],"%Y-%m-%d")
        
        geom = feat.GetGeometryRef()
        self.geom = geom.Clone()
        

    def getScore(self, target_date=None):
        
        score = 0
      
        required_attribs1 = [
            self.sunel,
            self.cloudcover,
            self.sensor,
        ]
        
        #### Test if all required values were found in metadata search
        status1 = [val is None for val in required_attribs1]

        if sum(status1) != 0:
            logger.error("Cannot determine score for image {0}:\n  Sun elev\t{1}\n  Cloudcover\t{2}\n  Sensor\t{3}".format(self.pairname,self.sunel,self.cloudcover,self.sensor)) 
            score = -1
        
        else:
            
            #### Test if acqdate if needed, get date difference
            if target_date:
                if self.acqdate is None:
                    logger.error("Cannot get acqdate for image to determine date-based score: {0}".format(self.srcfn))
                    self.date_diff = -9999
                    
                else:
                    #### Find nearest year for target day
                    tdeltas = []
                    target_month, target_day = target_date[0]
                    for y in range(self.acqdate.year-1,self.acqdate.year+2):
                        tdeltas.append(abs((datetime(y,target_month,target_day) - self.acqdate).days))
                    
                    self.date_diff = min(tdeltas)
            
            
                #### Assign weights
                ccwt = 75
                sunelwt = 5
                datediffwt = 20
                
            else:
                ccwt = 90
                sunelwt = 10
                datediffwt = 0
                self.date_diff = -9999
                
                
            #### Handle nonesense or nodata cloud cover values
            if self.cloudcover:
                if self.cloudcover < 0 or self.cloudcover > 1:
                    self.cloudcover = 0.5
            
                if self.cloudcover > 0.2:
                    logger.debug("Catid too cloudy (>20 percent): %s --> %f" %(self.pairname,self.cloudcover))
                    score = -1
            
            #### Handle ridiculously low sun el values
            if self.sunel and self.sunel < 1:
                logger.debug("Sun elevation too low (<1 degrees): %s --> %f" %(self.pairname,self.sunel))
                score = -1
                        
            if not score == -1:
                # determine score method
                if sum(status1) == 0:
                    score = ccwt * (1-self.cloudcover) + sunelwt * (self.sunel/90) + datediffwt * ((183 - self.date_diff)/183.0)
        
        self.score = score
        return self.score

        
class MosaicParams:
    pass


class TileParams:
    def __init__(self,x,x2,y,y2,j,i,name):
        self.xmin = x
        self.xmax = x2
        self.ymin = y
        self.ymax = y2
        self.i = i
        self.j = j
        self.name = name
        poly_wkt = 'POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))' %(x,y,x,y2,x2,y2,x2,y,x,y)
        self.geom = ogr.CreateGeometryFromWkt(poly_wkt)
        

def determine_contributors(imginfo_list, tile_geom, contribution_threshold):
        
    # set highest scoring image as seed geom
    imginfo_list.reverse() # highest score first
    union_geom = ogr.Geometry(ogr.wkbPolygon)
    contribs = []
    area_threshold_images = []
    
    # add lower scoring images in turn, if they add new area
    for iinfo in imginfo_list:
        diff = iinfo.geom.Difference(union_geom)
        if diff is None:
            logger.info("Function Error: {}".format(iinfo.srcfp))
        elif diff.IsEmpty():
            logger.debug("Non-contributing image: {}".format(iinfo.srcfp))
        else:
            ## test if contributing area is within tile extent
            if not diff.Intersects(tile_geom):
                logger.debug("Non-contributing image: {}".format(iinfo.srcfp))
            else:
                contrib_geom = diff.Intersection(tile_geom)
                
                ## Filter based on contribution area
                if contrib_geom.Area() >= contribution_threshold:                
                    union_geom = union_geom.Union(iinfo.geom)
                    contribs.append((iinfo,contrib_geom))
                else:
                    logger.debug("Image below minimum area threshold: {}".format(iinfo.srcfp))
                    area_threshold_images.append(iinfo)
                    
    # after first round, check if any of the images below the min area threshold fill a gap
    if len(area_threshold_images) > 0:
        for iinfo in area_threshold_images:
            diff = iinfo.geom.Difference(union_geom)
            if diff is None:
                logger.info("Function Error: {}".format(iinfo.srcfp))
            elif not diff.IsEmpty():
                ## test if contributing area is within tile extent
                if diff.Intersects(tile_geom):
                    contrib_geom = diff.Intersection(tile_geom)
                    union_geom = union_geom.Union(iinfo.geom)
                    contribs.append((iinfo,contrib_geom))
                    logger.debug("Adding image with contribution area below threshold to fill a gap: {}".format(iinfo.srcfp))
    
    # reverse list so highest score is last
    contribs.reverse()
    return contribs
    

def filterMatchingImages(imginfo_list,params):
    imginfo_list2 = []
   
    for iinfo in imginfo_list:
        #print iinfo.srcfp, iinfo.proj
        isSame = True
        p = osr.SpatialReference()
        p.ImportFromWkt(iinfo.proj)
        rp = osr.SpatialReference()
        rp.ImportFromWkt(params.proj)
        if p.IsSame(rp) is False:
            isSame = False
            logger.debug("Image projection differs from mosaic params: %s" %iinfo.srcfp)
        if iinfo.bands != params.bands and not (params.force_pan_to_multi is True and iinfo.bands == 1) and not (params.include_all_ms is True): 
            isSame = False
            logger.debug("Image band count differs from mosaic params: %s" %iinfo.srcfp)
        if iinfo.datatype != params.datatype:
            isSame = False
            logger.debug("Image datatype differs from mosaic params: %s" %iinfo.srcfp)
            
        if isSame is True:
            imginfo_list2.append(iinfo)

    return imginfo_list2


def filter_images_by_geometry(imginfo_list, params):
    imginfo_list2 = []
    for iinfo in imginfo_list:
        if iinfo.geom is not None:
            if params.extent_geom.Intersect(iinfo.geom):
                imginfo_list2.append(iinfo)
            else:
                logger.debug("Image does not intersect mosaic extent: %s" %iinfo.srcfn)
        else: # remove from list if no geom
            logger.debug("Null geometry for image: %s" %iinfo.srcfn)
    return imginfo_list2


def getMosaicParameters(iinfo,options):
    
    params = MosaicParams()
    
    try:
        if options.resolution is not None:
            params.xres = options.resolution[0]
            params.yres = options.resolution[1]
        else:
            params.xres = iinfo.xres
            params.yres = iinfo.yres
    except AttributeError:
        params.xres = iinfo.xres
        params.yres = iinfo.yres
       
    params.bands = options.bands if options.bands is not None else iinfo.bands
    params.proj = iinfo.proj
    params.datatype = iinfo.datatype
    params.useExposure = options.use_exposure
    
    if options.tday is not None:
        params.m = int(options.tday.split("-")[0])
        params.d = int(options.tday.split("-")[1])   
    else:
        params.m = 0
        params.d = 0
    
    if options.tyear is not None:
        ## build out list of year(s)
        if len(str(options.tyear)) == 4:
            params.y = [options.tyear]
        else:
            yrs = options.tyear.split('-')
            params.y = range(int(yrs[0]), int(yrs[1]) + 1)
    else:
        params.y = 0

    if options.extent is not None: # else set after geoms are collected
        params.xmin = options.extent[0]
        params.ymin = options.extent[2]
        params.xmax = options.extent[1]
        params.ymax = options.extent[3]
        poly_wkt = 'POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))' %(params.xmin,params.ymin,params.xmin,params.ymax,params.xmax,params.ymax,params.xmax,params.ymin,params.xmin,params.ymin)
        params.extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)
    
    try:
        if options.tilesize is not None:
            params.xtilesize = options.tilesize[0]
            params.ytilesize = options.tilesize[1]
            
    except AttributeError:
        if params.xres is not None:
            params.xtilesize = params.xres * 40000
            params.ytilesize = params.yres * 40000
        else:
            params.xtilesize = None
            params.ytilesize = None
        
    if options.max_cc is not None:
    	params.max_cc = options.max_cc
    else:
    	params.max_cc = 0.5
    
    params.force_pan_to_multi = True if params.bands > 1 and options.force_pan_to_multi else False # determine if force pan to multi is applicable and true

    params.include_all_ms = options.include_all_ms
    
    try:
        params.median_remove = options.median_remove
    except AttributeError:
        params.median_remove = None

    return params

def GetExactTrimmedGeom(image, step=4, tolerance=1):
    
    geom2 = None
    geom = None
    xs,ys = [],[]
    ds = gdal.Open(image)
    if ds is not None:
        if ds.RasterCount > 0:
            
            inband = ds.GetRasterBand(1)
            nd = inband.GetNoDataValue()
            if nd is None:
                nd = 0
            
            #print ("Image NoData Value: %d" %nd )
            gtf = ds.GetGeoTransform()
            pixelst = []
            pixelsb = []
            pts = []
            
            #### For every other line, find first and last data pixel
            lines = xrange(0, inband.YSize, step)
            xsize = inband.XSize
            
            try:
                lines_flatnonzero = [flatnonzero(inband.ReadAsArray(0,l,xsize,1) != nd) for l in lines]    
            except AttributeError, e:
                logger.error("Error reading image block.  Check image for corrupt data.")
            
            else:
                #print lines_flatnonzero
                i = 0
                for nz in lines_flatnonzero:
                    nzmin = nz[0] if nz.size > 0 else 0
                    nzmax = nz[-1] if nz.size > 0 else 0
                    if nz.size > 0:
                        pixelst.append((nzmax+1,i))
                        pixelsb.append((nzmin,i))           
                    i += step
                pixelsb.reverse()
                pixels = pixelst + pixelsb
                
                #print "Pixel Array length:", len(pixels)
                
                for px in pixels:
                    x,y = pl2xy(gtf,inband,px[0],px[1])
                    xs.append(x)
                    ys.append(y)
                    pts.append((x,y))
                    #print px[0],px[1],x,y
                
                #### create geometry
                poly_vts = []
                for pt in pts:
                    poly_vts.append("%.16f %.16f" %(pt[0],pt[1]))
                if len(pts) > 0:
                    poly_vts.append("%.16f %.16f" %(pts[0][0],pts[0][1]))
                
                if len(poly_vts) > 0:
                    poly_wkt = 'POLYGON (( %s ))' %(string.join(poly_vts,", "))
                    #print poly_wkt
                    
                    geom = ogr.CreateGeometryFromWkt(poly_wkt)
                    #print geom
                    #### Simplify geom
                    #logger.debug("Simplification tolerance: %.10f" %tolerance)
                    if geom is not None:
                        geom2  = geom.Simplify(tolerance)
        ds = None

    return geom2,xs,ys 

    
def findVertices(xoff, yoff, xsize, ysize, band, nd):
    line = band.ReadAsArray(xoff,yoff,xsize,ysize,xsize,ysize)
    if line is not None:
        nz = numpy.flatnonzero(line != nd)
    
        nzbool = nz.size > 0
        nzmin = nz[0] if nz.size > 0 else 0
        nzmax = nz[-1] if nz.size > 0 else 0
        
        return(nzbool, nzmin, nzmax)
    
    else:
        return (False,0,0)
    
    
def pl2xy(gtf,band,p,l):
    
    cols = band.XSize
    rows = band.YSize
    
    cellSizeX = gtf[1]
    cellSizeY = -1 * gtf[5]
  
    minx = gtf[0]
    maxy = gtf[3]
    
    # calc locations of pixels
    x = cellSizeX * p + minx
    y = maxy - cellSizeY * l - cellSizeY * 0.5
    
    return x,y
 
 
def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step


def buffernum(num,buf):
    sNum = str(num)
    while len(sNum)<buf:
        sNum = "0%s" %sNum
    return sNum
   
                    
def copyall(srcfile,dstdir):
    for fpi in glob.glob("%s.*" %os.path.splitext(srcfile)[0]):
        fpo = os.path.join(dstdir,os.path.basename(fpi))
        shutil.copy2(fpi,fpo)


