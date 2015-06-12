import os, string, sys, shutil, glob, re, tarfile, logging, argparse
from datetime import *
from subprocess import *
from math import *
from xml.etree import cElementTree as ET

import gdal, ogr,osr, gdalconst
import numpy

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG) 

MODES = ["ALL","MOSAIC","SHP","TEST"]
EXTS = [".tif"]
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


def buildMosaicParentArgumentParser():
    
    #### Set Up Arguments 
    parser = argparse.ArgumentParser(add_help=False)
    
    ####Optional Arguments
    
    parser.add_argument("-r", "--resolution", nargs=2, type=float,
                        help="output pixel resolution -- xres yres (default is same as first input file)")
    parser.add_argument("-e", "--extent", nargs=4, type=float,
                        help="extent of output mosaic -- xmin xmax ymin ymax (default is union of all inputs)")
    parser.add_argument("-t", "--tilesize", nargs=2, type=float,
                        help="tile size in coordinate system units -- xsize ysize (default is 40,000 times output resolution)")
    parser.add_argument("--force_pan_to_multi", action="store_true", dest="force_pan_to_multi", default=False,
                        help="if output is multiband, force script to also use 1 band images")
    parser.add_argument("-b", "--bands", type=int,
                        help="number of output bands( default is number of bands in the first image)")
    parser.add_argument("--tday",
                        help="month and day of the year to use as target for image suitability ranking -- 04-05")
    parser.add_argument("--nosort", action="store_true", default=False,
                        help="do not sort images by metadata. script uses the order of the input textfile or directory (first image is first drawn).  Not recommended if input is a directory; order will be random")
    parser.add_argument("--use_exposure", action="store_true", default=False,
                        help="use exposure settings in metadata to inform score")
    parser.add_argument("--exclude",
                        help="file of SCENE_IDs to exclude")

    return parser


class ImageInfo:
    def __init__(self,srcfp,frmt,logger):
                
        self.srcfp = srcfp
        self.srcdir, self.srcfn = os.path.split(srcfp)
        self.frmt = frmt  #image format (raw, warped)
        self.geom = None
        self.sensor = None
        for s in ['WV01','WV02','QB02','GE01','IK01']:
            if s in self.srcfn:
                self.sensor = s
        
        d, catid, sensor = getInfoFromName(self.srcfn)
        
        if d is not None:
            self.acqdate = d.strftime("%Y-%m-%d")
        else:
            self.acqdate = ""
        
        ds = gdal.Open(self.srcfp)
        if ds is not None:
            self.xsize = ds.RasterXSize
            self.ysize = ds.RasterYSize
            self.proj = ds.GetProjectionRef() if ds.GetProjectionRef() != '' else ds.GetGCPProjection()
            self.bands = ds.RasterCount
            self.datatype = ds.GetRasterBand(1).DataType
            self.datatype_readable = gdal.GetDataTypeName(self.datatype)

            if self.frmt == "warped":
                gtf = ds.GetGeoTransform()
                self.xres = abs(gtf[1])
                self.yres = abs(gtf[5])
            else:
                self.xres = None
                self.yres = None
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
    
    
    def getScore(self,params,logger):
        
        score = 0
        metad = None
        dAttribs = None
        
        if self.frmt == "warped":
            
            metapath = os.path.splitext(self.srcfp)[0]+'.xml'
            if os.path.isfile(metapath):
                try:
                    metad = ET.parse(metapath)
                except ET.ParseError, err:
                    logger.warning("ERROR parsing metadata: %s, %s" %(err,metapath))
            else:
                logger.warning("No metadata xml exists for %s" %self.srcfp)
            
            
        elif self.frmt == "raw":
            
            metapath_xml = os.path.splitext(self.srcfp)[0]+'.xml'
            metapath_txt = os.path.splitext(self.srcfp)[0]+'.txt'
            if os.path.isfile(metapath_xml):
                metapath = metapath_xml
                try:
                    metad = ET.parse(metapath)
                except ET.ParseError, err:
                    logger.warning("ERROR parsing metadata: %s, %s" %(err,metapath))
            elif os.path.isfile(metapath_txt):
                metapath = metapath_txt
                try:
                    metad = getGEMetadataAsXml(metapath)
                except ET.ParseError, err:
                    logger.warning("ERROR parsing metadata: %s, %s" %(err,metapath))
            else:    
                logger.warning("No metadata xml/txt exists for %s" %self.srcfp)
                    
            
            #### Write IK01 code        
            #if self.sensor in ['IK01']:
                    
        dAttribs = {
            "cc":None,
            "sunel":None,
            "ona":None,
            "date":None,
            "tdi":None
        }
    
        dTags = {
            "CLOUDCOVER":"cc",
            "MEANSUNEL":"sunel",
            "MEANOFFNADIRVIEWANGLE":"ona",
            "FIRSTLINETIME":"date",
            "TDILEVEL":"tdi",
            "percentCloudCover":"cc",
            "firstLineSunElevationAngle":"sunel",
            "firstLineElevationAngle":"ona",
            "firstLineAcquisitionDateTime":"date",
            "tdiMode":"tdi"
        }
        
        if metad is not None:
            
            for tag in dTags:
                taglist = metad.findall(".//%s"%tag)
                vallist = []
                for elem in taglist:
                    
                    text = elem.text
                
                    if text is not None:
                        try:
                            if tag == "firstLineElevationAngle":
                                val = 90 - float(text)
                            elif tag == "FIRSTLINETIME" or tag == "firstLineAcquisitionDateTime":
                                val = text
                            elif tag == "percentCloudCover":
                                val = float(text)/100
                            else:
                                val = float(text)
                                
                            vallist.append(val)
                            
                        except Exception, e:
                            logger.warning("Error reading metadata values: %s, %s" %(metapath,e))
                            
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
                        logger.warning("Unexpected number of TDI values and band count ( TDI: expected 1, 4, 5, or 8 - found %d ; Band cound, expected 1, 4, or 8 - found %d) %s" %(len(vallist), self.bands, metapath))
                        
                elif len(taglist) == 1:
                    val = vallist[0]
                    dAttribs[dTags[tag]] = val
                    
                elif len(taglist) <> 0:
                    logger.warning("Unexpected number of %s values, %s" %(tag,metapath))
            
            #### Test if all required values were found in metadata search
            status = [val is None for val in dAttribs.values()]
            
            if sum(status) != 0:
                logger.warning("Cannot determine score for image %s: %s" %(self.srcfp,str(dAttribs)))
                score = -1
            
            #### Assign panfactor if pan images are to be included in a multispectral mosaic   
            else:
                if self.bands == 1 and params.force_pan_to_multi is True:
                    panfactor = 0.5
                else:
                    panfactor = 1
                
                dAttribs["panfact"] = panfactor
                
                
                #### Parse target day and assign weights
                if params.m == 0:
                    date_diff = -9999
                    ccwt = 48
                    sunelwt = 28
                    onawt = 24
                    datediffwt = 0
                    
                else:
                    cd = datetime.strptime(dAttribs["date"],"%Y-%m-%dT%H:%M:%S.%fZ")
                    
                    #### Find nearest year for target day
                    tdeltas = []
                    for y in range(cd.year-1,cd.year+2):
                        tdeltas.append(abs((datetime(y,params.m,params.d) - cd).days))
                    
                    date_diff = min(tdeltas)
                    ccwt = 30
                    sunelwt = 10
                    onawt = 5
                    datediffwt = 55
                
                dAttribs["datediff"] = date_diff
                
                
                #### Remove images with high exposure settings (tdi_pan (or tdi_grn) * sunel)
                exfact = float(dAttribs["tdi"]) * float(dAttribs["sunel"])
                dAttribs["exfact"] = exfact
                
                if params.useExposure is True:
                    
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
                    
                    if params.bands == 1:
                        if self.sensor in pan_exposure_thresholds:
                            if exfact > pan_exposure_thresholds[self.sensor]:
                                logger.warning("Image overexposed: %s --> %i" %(self.srcfp,exfact))
                                score = -1
                    
                    else:
                        if self.sensor in multi_exposure_thresholds:
                            if exfact > multi_exposure_thresholds[self.sensor]:
                                logger.warning("Image overexposed: %s --> %i" %(self.srcfp,exfact))
                                score = -1
                        
                #### Handle nonesense or nodata cloud cover values
                if float(dAttribs["cc"]) < 0 or float(dAttribs["cc"]) > 1:
                    dAttribs["cc"] = 0.5
                
                if float(dAttribs["cc"]) > 0.5:
                    logger.warning("Image too cloudy: %s --> %f" %(self.srcfp,float(dAttribs["cc"])))
                    score = -1
                        
                #try:
                #
                #except TypeError, err:
                #    logger.warning("Error calculating score for image %s: %e" %(self.srcfp,err))
                #    score = -1
                
                if not score == -1:
                    rawscore = ccwt * (1-float(dAttribs["cc"])) + sunelwt * (float(dAttribs["sunel"])/90) + onawt * ((90-float(dAttribs["ona"]))/90.0) + datediffwt * ((183 - date_diff)/183.0)
                    score = rawscore * panfactor  
            
              
        return score, dAttribs
    
        
class MosaicParams:
    pass


class TileParams:
    def __init__(self,x,x2,y,y2,j,i,name):
        self.minx = x
        self.maxx = x2
        self.miny = y
        self.maxy = y2
        self.i = i
        self.j = j
        self.name = name
        poly_wkt = 'POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))' %(x,y,x,y2,x2,y2,x2,y,x,y)
        self.geom = ogr.CreateGeometryFromWkt(poly_wkt)
        

def filterMatchingImages(imginfo_list,params,logger):
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
        if iinfo.bands != params.bands and not (params.force_pan_to_multi is True and iinfo.bands == 1):
            isSame = False
        if iinfo.datatype != params.datatype:
            isSame = False
            
        if isSame is True:
            imginfo_list2.append(iinfo)
        else:
            logger.warning("Image does not match filter: %s" %iinfo.srcfp)

    return imginfo_list2


def getMosaicParameters(iinfo,options):
    
    params = MosaicParams()
    
    if options.resolution is not None:
        params.xres = options.resolution[0]
        params.yres = options.resolution[1]
    else:
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
    
    if options.extent is not None: # else set after geoms are collected
        params.xmin = options.extent[0]
        params.ymin = options.extent[2]
        params.xmax = options.extent[1]
        params.ymax = options.extent[3]
        
    if options.tilesize is not None:
        params.xtilesize = options.tilesize[0]
        params.ytilesize = options.tilesize[1]
    elif params.xres is not None:
        params.xtilesize = params.xres * 40000
        params.ytilesize = params.yres * 40000
    else:
        params.xtilesize = None
        params.ytilesize = None
    
    params.force_pan_to_multi = True if params.bands > 1 and options.force_pan_to_multi else False # determine if force pan to multi is applicable and true
    
    return params


def GetExactTrimmedGeom(image, step=2, tolerance=1):
    
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
            npflatnonzero = numpy.flatnonzero
            bandReadAsArray = inband.ReadAsArray
            
            lines_flatnonzero = [npflatnonzero(bandReadAsArray(0,l,xsize,1) != nd) for l in lines]
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
            
            #print len(pixels)
            
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
                #logger.info("Simplification tolerance: %.10f" %tolerance)
                if geom is not None:
                    geom2  = geom.Simplify(tolerance)
            
            
        ds = None

    return geom2,xs,ys 


def getGeom(image):
    
    geom = None
    xs,ys = [],[]
    
    ds = gdal.Open(image)
    if ds is not None:
    
        xsize = ds.RasterXSize
        ysize = ds.RasterYSize
        gtf = ds.GetGeoTransform()
        
        #### create geometry
        minx = gtf[0]
        maxx = minx + xsize * gtf[1]
        maxy = gtf[3]
        miny = maxy + ysize * gtf[5]
        poly_wkt = 'POLYGON (( '+str(minx)+' '+str(miny)+', '+str(minx)+' '+str(maxy)+', '+str(maxx)+' '+str(maxy)+', '+str(maxx)+' '+str(miny)+', '+str(minx)+' '+str(miny)+' ))'
        geom = ogr.CreateGeometryFromWkt(poly_wkt)
        
        xs = [minx,maxx]
        ys = [miny,maxy]
    ds = None
        
    return geom,xs,ys


def getRpcGeom(iinfo,dem,t_srs):
    geom = None
    image = iinfo.srcfp
    
    #### Create coordiante system transformation
    img_srs = osr.SpatialReference(iinfo.proj)
    imgct = osr.CoordinateTransformation(img_srs, t_srs)
    
    ds = gdal.Open(image)
    if ds is not None:
    
        #### Build list of image coordinates on the perimeter (every 1/100 of the image size)
        xsize = int(ds.RasterXSize)
        ysize = int(ds.RasterYSize)
        xstep = int(floor(xsize/100))
        ystep = int(floor(ysize/100))
        #print xsize, ysize
        #print xstep, ystep
        #perimeter = []
        #for x in xrange(0,xsize+1,xstep):
        #    #print x,0
        #    perimeter.append((x,0))
        #for y in xrange(0,ysize+1,ystep):
        #    #print xsize+1,y
        #    perimeter.append((xsize+1,y))
        #for x in xrange(xsize+1,0,-1*xstep):
        #    #print x,ysize+1
        #    perimeter.append((x,ysize+1))
        #for y in xrange(ysize+1,0,-1*ystep):
        #    #print 0,y
        #    perimeter.append((0,y))
        
        perimeter = [(0,0), (0,ysize), (xsize,ysize), (xsize,0)]
        
        #### Get RPC transformer
        #if dem is not None:
        #    to = ['METHOD=RPC','RPC_DEM=%s' %dem]
        #else:
        #    m = ds.GetMetadata("RPC")
        #    if "HEIGHT_OFF" in m:
        #        h = m["HEIGHT_OFF"]
        #    else:
        #        h = 0
        #    to = ["METHOD=RPC", "RPC_HEIGHT=%s" %h]
        #to = ['METHOD=GCP_POLYNOMIAL']
        to = []
        tf = gdal.Transformer(ds, None, to)

        #### Transform points
        pts = []
        for s_coords in perimeter:
            #print s_coords
            rc, t_coords = tf.TransformPoint(0,s_coords[0],s_coords[1])
            #inv_rc, inv_coords = tf.TransformPoint(1,t_coords[0],t_coords[1],t_coords[2])
            #if abs(s_coords[0] - inv_coords[0]) > 100 or abs(s_coords[1] - inv_coords[1]) > 100:
            #print iinfo.srcfn, s_coords, t_coords
            #print t_coords
            
            
            pt = ogr.Geometry(ogr.wkbPoint)
            pt.SetPoint_2D(0,t_coords[0],t_coords[1])
            pt.Transform(imgct)
            pts.append("%f %f" %(pt.GetX(),pt.GetY()))
        
        if len(pts) > 1:
            pts.append(pts[0])
        poly_wkt = 'POLYGON (( %s ))' %(string.join(pts,", "))
        geom = ogr.CreateGeometryFromWkt(poly_wkt)
        
    ds = None
        
    return geom
    

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
   
    
def deleteTempFiles(names):
    print ('Deleting Temp Files')
    for name in names:
        if name is not None:
            deleteList = glob.glob(os.path.splitext(name)[0]+'.*')
            for f in deleteList:
                try:
                    os.remove(f)
                    print ('Deleted '+os.path.basename(f))
                except:
                    print ('Could not remove '+os.path.basename(f))
   
                    
def copyall(srcfile,dstdir):
    for fpi in glob.glob("%s.*" %os.path.splitext(srcfile)[0]):
        fpo = os.path.join(dstdir,os.path.basename(fpi))
        shutil.copy2(fpi,fpo)
    
    
def ExecCmd(cmd):
    print (cmd)
    p = Popen(cmd,shell=True,stderr=PIPE,stdout=PIPE)
    (so,se) = p.communicate()
    rc = p.wait()
    print (rc)
    print (se)
    print (so)
    

def getGEMetadataAsXml(metafile):
	if os.path.isfile(metafile):
		try:
			metaf = open(metafile, "r")
		except IOError, err:
			LogMsg("Could not open metadata file %s because %s" % (metafile, err))
			raise
	else:
		LogMsg("Metadata file %s not found" % metafile)
		return None

	# Patterns to extract tag/value pairs and BEGIN/END group tags
	gepat1 = re.compile(r'(?P<tag>\w+) = "?(?P<data>.*?)"?;', re.I)
	gepat2 = re.compile(r"(?P<tag>\w+) = ", re.I)

	# These tags use the following tag/value as an attribute of the group rather than
	# a standalone node
	group_tags = {"aoiGeoCoordinate":"coordinateNumber",
				  "aoiMapCoordinate":"coordinateNumber",
				  "bandSpecificInformation":"bandNumber"}

	# Start processing
	root = ET.Element("root")
	parent = None
	current = root
	node_stack = []
	mlstr = False  # multi-line string flag

	for line in metaf:
		# mlstr will be true when working on a multi-line string
		if mlstr:
			if not line.strip() == ");":
				data += line.strip()
			else:
				data += line.strip()
				child = ET.SubElement(current, tag)
				child.text = data
				mlstr = False

		# Handle tag/value pairs and groups
		mat1 = gepat1.search(line)
		if mat1:
			tag = mat1.group("tag").strip()
			data = mat1.group("data").strip()

			if tag == "BEGIN_GROUP":
				if data is None or data == "":
					child = ET.SubElement(current, "group")
				else:
					child = ET.SubElement(current, data)
				if parent:
					node_stack.append(parent)
				parent = current
				current = child
			elif tag == "END_GROUP":
				current = parent if parent else root
				parent = node_stack.pop() if node_stack else None
			else:
				if current.tag in group_tags and tag == group_tags[current.tag]:
					current.set(tag, data)
				else:
					child = ET.SubElement(current, tag)
					child.text = data
		else:
			mat2 = gepat2.search(line)
			if mat2:
				tag = mat2.group("tag").strip()
				data = ""
				mlstr = True

	metaf.close()
	#print ET.ElementTree(root)
	return ET.ElementTree(root)


def getInfoFromName(filename):
    
    DG = re.compile("(?P<snsr>[A-Z]{2}[0-9]{2})_(?P<ts>[0-9]{2}[A-Z]{3}[0-9]{9})-\w+-(?P<catid>\w{16})")
    # orthoWV02_12JAN062217482-P1BS-1030010010D21A00_u08rf3031.tif
    # WV02_12JAN062217482-P1BS-1030010010D21A00_u08rf3031.tif
    # orthoWV01_11DEC300148296-P1BS_R1C1-1020010018BE2400_u08rf3031.tif
    
    
    GE = re.compile("(?P<snsr>[A-Z]{2}[0-9]{2})_(?P<ts>[0-9]{6})[PMBGRNpmbgrn][0-9]{10}[ABab][0-9]{12}[MSms]_[0-9]{9}")
    # orthoGE01_111214P0011193504A222000100662M_000757427_u08rf3031.tif
    # GE01_111214P0011193504A222000100662M_000757427_u08rf3031.tif
    
    IK = re.compile("(?P<snsr>[A-Z]{2}[0-9]{2})_(?P<catid>(?P<ts>[0-9]{8})[0-9]{20})_(pan|msi|blu|grn|red|nir|bgrn)_[0-9]{4}[SNsm]")
    # IK01_2007072321341760000011613218_pan_6837N_u08mr3338.tif
    # orthoIK01_2007072321341760000011613218_pan_6837N_u08mr3338.tif
    
    RegExs = [DG,GE,IK]

    d, ts, catid, sensor = None, None, None, None

    for regex in RegExs:
        m = regex.search(filename)
        if m is not None:
            gd = m.groupdict()
            if 'snsr' in gd:
                sensor = gd['snsr']
            if 'catid' in gd:
                catid = gd['catid']
            if 'ts' in gd:
                ts = gd['ts']
                #print ts
                
                if len(ts) > 8:
                    try:
                        d = datetime.strptime('20'+ts[:7],"%Y%b%d")
                    except Exception, e:
                        print "ERROR: %s, Cannot parse timestamp from filename %s" %(e,filename)
                    
                    #12JAN062217482
        
                elif len(ts) == 6:
                    try:
                        d = datetime.strptime('20'+ts,"%Y%m%d")
                    except Exception, e:
                        print "ERROR: %s, Cannot parse timestamp from filename %s" %(e,filename)
                
                elif len(ts) == 8:
                    try:
                        d = datetime.strptime(ts,"%Y%m%d")
                    except Exception, e:
                        print "ERROR: %s, Cannot parse timestamp from filename %s" %(e,filename)
                
                if d == None:
                    print "ERROR: Cannot parse timestamp from filename %s" %filename
                    
            else:
                print "ERROR: Unable to extract date from filename %s" %filename
                
    if sensor is None:
         print("ERROR: Unable to match sensor pattern from filename %s" %filename)
    
    return d, catid, sensor

