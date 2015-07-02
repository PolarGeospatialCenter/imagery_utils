import os, string, sys, shutil, math, glob, re, tarfile, logging, shlex, platform, argparse, signal
from datetime import datetime, timedelta

from subprocess import *
from xml.dom import minidom
from xml.etree import cElementTree as ET

import gdal, ogr,osr, gdalconst

DGbandList = ['BAND_P','BAND_C','BAND_B','BAND_G','BAND_Y','BAND_R','BAND_RE','BAND_N','BAND_N2','BAND_S1','BAND_S2','BAND_S3','BAND_S4','BAND_S5','BAND_S6','BAND_S7','BAND_S8']
formats = {'GTiff':'.tif','JP2OpenJPEG':'.jp2','ENVI':'.envi','HFA':'.img'}
outtypes = ['Byte','UInt16','Float32']
stretches = ["ns","rf","mr","rd"]
resamples = ["near","bilinear","cubic","cubicspline","lanczos"]
gtiff_compressions = ["jpeg95","lzw"]
exts = ['.ntf','.tif']

WGS84 = 4326

formatVRT = "VRT"
VRTdriver = gdal.GetDriverByName( formatVRT )
ikMsiBands = ['blu','grn','red','nir']
satList = ['WV01','QB02','WV02','GE01','IK01']

PGC_DG_FILE = re.compile(r"""
                         (?P<pgcpfx>                        # PGC prefix
                            (?P<sensor>[a-z]{2}\d{2})_      # Sensor code
                            (?P<tstamp>\d{14})_             # Acquisition time (yyyymmddHHMMSS)
                            (?P<catid>[a-f0-9]{16})         # Catalog ID
                         )_
                         (?P<oname>                         # Original DG name
                            (?P<ts>\d{2}[a-z]{3}\d{8})-     # Acquisition time (yymmmddHHMMSS)
                            (?P<prod>[a-z0-9]{4})_?         # DG product code
                            (?P<tile>R\d+C\d+)?-            # Tile code (mosaics, optional)
                            (?P<oid>                        # DG Order ID
                                (?P<onum>\d{12}_\d{2})_     # DG Order number
                                (?P<pnum>P\d{3})            # Part number
                            )
                            (?P<tail>[a-z0-9_-]+(?=\.))?    # Descriptor (optional)
                         )
                         (?P<ext>\.[a-z0-9][a-z0-9.]*)      # File name extension
                         """, re.I | re.X)
PGC_IK_FILE = re.compile(r"""
                         (?P<pgcpfx>                        # PGC prefix
                            (?P<sensor>[a-z]{2}\d{2})_      # Sensor code
                            (?P<tstamp>\d{14})_             # Acquisition time (yyyymmddHHMMSS)
                            (?P<catid>\d{28})               # Catalog ID
                         )_
                         (?P<oname>
                            po_(?P<po>\d{5,7})_             # PO number
                            (?P<band>[a-z]+(?=_))?_?        # Band description
                            (?P<cmp>\d{7}(?=[_.]))?         # Component number
                            (?P<tail>[a-z0-9_-]+(?=\.))?    # Descriptor (optional)
                         )
                         (?P<ext>\.[a-z0-9][a-z0-9.]*)      # File name extension
                         """, re.I | re.X)
#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

class ImageInfo:
    pass


class SpatialRef(object):

    def __init__(self,epsg):
	srs = osr.SpatialReference()
	try:
	    epsgcode = int(epsg)

	except ValueError, e:
	    raise RuntimeError("EPSG value must be an integer: %s" %epsg)
	else:

	    err = srs.ImportFromEPSG(epsgcode)
	    if err == 7:
		raise RuntimeError("Invalid EPSG code: %d" %epsgcode)
	    else:
		proj4_string = srs.ExportToProj4()

		proj4_patterns = {
		    "+ellps=GRS80 +towgs84=0,0,0,0,0,0,0":"+datum=NAD83",
		    "+ellps=WGS84 +towgs84=0,0,0,0,0,0,0":"+datum=WGS84",
		}

		for pattern, replacement in proj4_patterns.iteritems():
		    if proj4_string.find(pattern) <> -1:
			proj4_string = proj4_string.replace(pattern,replacement)

		self.srs = srs
		self.proj4 = proj4_string
		self.epsg = epsgcode


def buildParentArgumentParser():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(add_help=False)

    #### Positional Arguments
    parser.add_argument("src", help="source image, text file, or directory")
    parser.add_argument("dst", help="destination directory")
    pos_arg_keys = ["src","dst"]


    ####Optional Arguments
    parser.add_argument("-f", "--format", choices=formats.keys(), default="GTiff",
                      help="output to the given format (default=GTiff)")
    parser.add_argument("--gtiff_compression", choices=gtiff_compressions, default="lzw",
                      help="GTiff compression type (default=lzw)")
    parser.add_argument("-p", "--epsg", required=True, type=int,
                      help="epsg projection code for output files")
    parser.add_argument("-d", "--dem",
                      help="the DEM to use for orthorectification (elevation values should be relative to the wgs84 ellipoid")
    parser.add_argument("-t", "--outtype", choices=outtypes, default="Byte",
                      help="output data type (default=Byte)")
    parser.add_argument("-r", "--resolution",
                      help="output pixel resolution in units of the projection")
    parser.add_argument("-c", "--stretch", choices=stretches, default="rf",
                      help="stretch type [ns: nostretch, rf: reflectance (default), mr: modified reflectance, rd: absolute radiance]")
    parser.add_argument("--resample", choices=resamples, default="near",
                      help="resampling strategy - mimicks gdalwarp options")
    parser.add_argument("--rgb", action="store_true", default=False,
                      help="output multispectral images as 3 band RGB")
    parser.add_argument("--bgrn", action="store_true", default=False,
                      help="output multispectral images as 4 band BGRN (reduce 8 band to 4)")
    parser.add_argument("-s", "--save-temps", action="store_true", default=False,
                      help="save temp files")
    parser.add_argument("--wd",
                      help="local working directory for cluster jobs (default is dst dir)")
    parser.add_argument("--skip_warp", action='store_true', default=False,
                      help="skip warping step")
    parser.add_argument("--no_pyramids", action='store_true', default=False, help='suppress calculation of output image pyramids and stats')
    parser.add_argument("--ortho_height", type=long, help='constant elevation to use for orthorectification (value should be in meters above the wgs84 ellipoid)')


    return parser, pos_arg_keys


def processImage(srcfp,dstfp,opt):

    err = 0

    #### Instantiate ImageInfo object
    info = ImageInfo()
    info.srcfp = srcfp
    info.srcdir,info.srcfn = os.path.split(srcfp)
    info.dstfp = dstfp
    info.dstdir,info.dstfn = os.path.split(dstfp)

    starttime = datetime.today()
    LogMsg('Image: %s' %(info.srcfn))

    #### Get working dir
    if opt.wd is not None:
        wd = opt.wd
    else:
        wd = info.dstdir
    if not os.path.isdir(wd):
        try:
            os.makedirs(wd)
        except OSError:
            pass
    LogMsg("Working Dir: %s" %wd)

    #### Derive names
    info.localsrc = os.path.join(wd,info.srcfn)
    info.localdst = os.path.join(wd,info.dstfn)
    info.rawvrt = os.path.splitext(info.localsrc)[0] + "_raw.vrt"
    info.warpfile = os.path.splitext(info.localsrc)[0] + "_warp.tif"
    info.vrtfile = os.path.splitext(info.localsrc)[0] + "_vrt.vrt"

    #### Verify EPSG
    try:
        spatial_ref = SpatialRef(opt.epsg)
    except RuntimeError, e:
        logger.error("Invalid EPSG code: {0)".format(opt.epsg))
	err = 1
    else:
	opt.spatial_ref = spatial_ref
        
    #### Verify that dem and ortho_height are not both specified
    if opt.dem is not None and opt.ortho_height is not None:
        logger.error("--dem and --ortho_height options are mutually exclusive.  Please choose only one.")
        err = 1
        
    
    #### Check if image is level 2A and tiled, raise error
    p = re.compile("-(?P<prod>\w{4})?(_(?P<tile>\w+))?-\w+?(?P<ext>\.\w+)")
    m = p.search(info.srcfn)
    if m:
        gd = m.groupdict()
        if gd['prod'][3] == 'M':
            logger.error("Cannot process mosaic product")
            err = 1
        if gd['prod'][1] == '3':
            logger.error("Cannot process 3* products")
            err = 1
        if gd['prod'][1:3] == '2A' and gd['tile'] is not None and gd['ext'] == '.tif':
            logger.error("Cannot process 2A tiled Geotiffs")
            err = 1
    
    #### Find metadata file
    if not err == 1:
        metafile = GetDGMetadataPath(info.srcfp)
        if metafile is None:
            metafile = GetIKMetadataPath(info.srcfp)
        if metafile is None:
            metafile = GetGEMetadataPath(info.srcfp)
        if metafile is None:
            logger.error("Cannot find metadata for image: {0}".format(info.srcfp))
            err = 1
        else:
            info.metapath = metafile

    if not err == 1:
        #### Check If Image is IKONOS msi that does not exist, if so, stack to dstdir, else, copy srcfn to dstdir
        if "IK01" in info.srcfn and "msi" in info.srcfn and not os.path.isfile(info.srcfp):
            LogMsg("Converting IKONOS band images to composite image")
            members = [os.path.join(info.srcdir,info.srcfn.replace("msi",b)) for b in ikMsiBands]
            status = [os.path.isfile(member) for member in members]
            if sum(status) != 4:
                logger.error("1 or more IKONOS multispectral member images are missing %s" %", ".join(members))
                err = 1
            elif not os.path.isfile(info.localsrc):
                rc = stackIkBands(info.localsrc, members)
                #if not os.path.isfile(os.path.join(wd,os.path.basename(info.metapath))):
                #    shutil.copy(info.metapath, os.path.join(wd,os.path.basename(info.metapath)))

        else:
            if os.path.isfile(info.srcfp):
                LogMsg("Copying image to working directory")
                copy_list = glob.glob("%s.*" %os.path.splitext(info.srcfp)[0])
                #copy_list.append(info.metapath)
                for fpi in copy_list:
                    fpo = os.path.join(wd,os.path.basename(fpi))
                    if not os.path.isfile(fpo):
                        shutil.copy2(fpi,fpo)
               
            else:
                LogMsg("Source images does not exist: %s" %info.srcfp)
                err = 1

    #### Get Image Stats
    if not err == 1:
        info, rc = GetImageStats(opt,info)
        if rc == 1:
            err = 1
            LogMsg("ERROR in stats calculation")

    #### Check that DEM overlaps image
    if not err == 1:
        if opt.dem:
            overlap = overlap_check(info.geometry_wkt,opt.spatial_ref,opt.dem)
            if overlap is False:
                err = 1
    
    #### Warp Image
    if not err == 1 and not os.path.isfile(info.warpfile):
        rc = WarpImage(opt,info)
        if rc == 1:
            err = 1
            LogMsg("ERROR in image warping")

    #### Calculate Output File
    if not err == 1 and os.path.isfile(info.warpfile):
        rc = calcStats(opt,info)
        if rc == 1:
            err = 1
            LogMsg("ERROR in image calculation")
    
    ####  Write Output Metadata
    if not err == 1:
        rc = WriteOutputMetadata(opt,info)
        if rc == 1:
            err = 1
            LogMsg("ERROR in writing metadata file")
    
    #### Copy image to final location if working dir is used
    if opt.wd is not None:
        if not err == 1:
            LogMsg("Copying to destination directory")
            for fpi in glob.glob("%s.*" %os.path.splitext(info.localdst)[0]):
                fpo = os.path.join(info.dstdir,os.path.basename(fpi))
                if not os.path.isfile(fpo):
                    shutil.copy2(fpi,fpo)
        if not opt.save_temps:
            deleteTempFiles([info.localdst])

    #### Check If Done, Delete Temp Files
    done = os.path.isfile(info.dstfp)
    if done is False:
        err = 1
        LogMsg("ERROR: final image not present")

    if err == 1:
        LogMsg("Processing failed: %s" %info.srcfn)
        if not opt.save_temps:
            deleteTempFiles([dstfp,info.rawvrt,info.warpfile,info.vrtfile,info.localsrc])
    
    elif not opt.save_temps:
        deleteTempFiles([info.rawvrt,info.warpfile,info.vrtfile,info.localsrc])
        
    #### Calculate Total Time
    endtime = datetime.today()
    td = (endtime-starttime)
    LogMsg("Total Processing Time: %s\n" %(td))

    return err


def stackIkBands(dstfp, members):

    rc = 0

    band_dict = {1:gdalconst.GCI_BlueBand,2:gdalconst.GCI_GreenBand,3:gdalconst.GCI_RedBand,4:gdalconst.GCI_Undefined}
    remove_keys = ("NITF_FHDR","NITF_IREP","NITF_OSTAID","NITF_IC","NITF_ICORDS","NITF_IGEOLO") #"NITF_FHDR"
    meta_dict = {"NITF_IREP":"MULTI"}

    srcfp = members[0]
    srcdir,srcfn = os.path.split(srcfp)
    dstdir,dstfn = os.path.split(dstfp)
    vrt = os.path.splitext(srcfp)[0]+ "_merge.vrt"

    #### Gather metadata from original blue image and save as strings for merge command
    LogMsg("Stacking IKONOS MSI bands")
    src_ds = gdal.Open(srcfp,gdalconst.GA_ReadOnly)
    if src_ds is not None:


        #### Get basic metadata
        m = src_ds.GetMetadata()
        if src_ds.GetGCPCount() > 1:
            proj = src_ds.GetGCPProjection()
        else:
            proj = src_ds.GetProjectionRef()
        s_srs = osr.SpatialReference(proj)
        s_srs_proj4 = s_srs.ExportToProj4()

        #### Remove keys we want to leave or set ourselves
        for k in remove_keys:
            if k in m:
                del m[k]
        #### Make the dictionary into a list and append the ones we set ourselves
        m_list = []
        keys = m.keys()
        keys.sort()
        for k in keys:
            if not '"' in m[k]:
                m_list.append('-co "%s=%s"' %(k.replace("NITF_",""),m[k]))
        for k in meta_dict.keys():
            if not '"' in meta_dict[k]:
                m_list.append('-co "%s=%s"' %(k.replace("NITF_",""),meta_dict[k]))

        #### Get the TRE metadata
        tres = src_ds.GetMetadata("TRE")
        #### Make the dictionary into a list
        tre_list = []
        for k in tres.keys():
            if not '"' in tres[k]:
                tre_list.append('-co "TRE=%s=%s"' %(k,src_ds.GetMetadataItem(k,"TRE")))


        #### Close the source dataset
        src_ds = None

        #print "Merging bands"
        cmd = 'gdalbuildvrt -separate "%s" "%s"' %(vrt,'" "'.join(members))

        #
        (err,so,se) = ExecCmd(cmd)
        if err == 1:
            rc = 1

        cmd = 'gdal_translate -a_srs "%s" -of NITF -co "IC=NC" %s %s "%s" "%s"' %(s_srs_proj4,string.join(m_list, " "), string.join(tre_list, " "), vrt, dstfp )

        (err,so,se) = ExecCmd(cmd)
        if err == 1:
            rc = 1

        #print "Writing metadata to output"
        dst_ds = gdal.Open(dstfp,gdalconst.GA_ReadOnly)
        if dst_ds is not None:
            #### check that ds has correct number of bands
            if not dst_ds.RasterCount == len(band_dict):
                logger.error("Missing MSI band in stacked dataset.  Band count: %i, Required band count: %i" %(dst_ds.RasterCount,len(band_dict)))
                rc = 1

            else:
                #### Set Color Interpretation
                for key in band_dict.keys():
                    rb = dst_ds.GetRasterBand(key)
                    rb.SetColorInterpretation(band_dict[key])

        #### Close Image
        dst_ds = None

        #### also copy blue and rgb aux files
        for fpi in glob.glob(os.path.join(srcdir,"%s.*" %os.path.splitext(srcfn)[0])):
            fpo = os.path.join(dstdir,os.path.basename(fpi).replace("blu","msi"))
            if not os.path.isfile(fpo) and not os.path.basename(fpi) == srcfn:
                shutil.copy2(fpi,fpo)
        for fpi in glob.glob(os.path.join(srcdir,"%s.*" %os.path.splitext(srcfn)[0].replace("blu","rgb"))):
            fpo = os.path.join(dstdir,os.path.basename(fpi).replace("rgb","msi"))
            if not os.path.isfile(fpo) and not os.path.basename(fpi) == srcfn:
                shutil.copy2(fpi,fpo)
        for fpi in glob.glob(os.path.join(srcdir,"%s.txt" %os.path.splitext(srcfn)[0].replace("blu","pan"))):
            fpo = os.path.join(dstdir,os.path.basename(fpi).replace("pan","msi"))
            if not os.path.isfile(fpo):
                shutil.copy2(fpi,fpo)

    else:
        rc = 1
    try:
        os.remove(vrt)
    except Exception, e:
        logger.warning("Cannot remove file: %s, %s" %(vrt,e))
    return rc


def calcStats(opt,info):

    LogMsg("Calculating image with stats")

    rc = 0

    #### Get Well-known Text String of Projection from EPSG Code
    p = opt.spatial_ref.srs
    prj = p.ExportToWkt()

    imax = 2047.0

    if info.stretch == 'ns':
        if opt.outtype == "Byte":
            omax = 255.0
        elif opt.outtype == "UInt16":
            omax = 2047.0
        elif opt.outtype == "Float32":
            omax = 2047.0
    else:
        if opt.outtype == "Byte":
            omax = 200.0
        elif opt.outtype == "UInt16":
            omax = 2000.0
        elif opt.outtype == "Float32":
            omax = 1.0

    #### Stretch
    if info.stretch != "ns":
        CFlist = GetCalibrationFactors(info)
        if len(CFlist) == 0:
            LogMsg("Cannot get image calibration factors from metadata")
            return 1
    
    wds = gdal.Open(info.warpfile,gdalconst.GA_ReadOnly)
    if wds is not None:

        xsize = wds.RasterXSize
        ysize = wds.RasterYSize

        vds = VRTdriver.CreateCopy(info.vrtfile,wds,0)
        if vds is not None:

            for band in range(1,vds.RasterCount+1):

                if info.stretch == "ns":
                    LUT = "0:0,%f:%f" %(imax,omax)
                elif info.stretch == "rf":
                    LUT = "0:0,%f:%f" %(imax,omax*imax*CFlist[band-1])
                elif info.stretch == "rd":
                    LUT = "0:0,%f:%f" %(imax,imax*CFlist[band-1])
                elif info.stretch == "mr":
                    iLUT = [0, 0.125, 0.25, 0.375, 0.625, 1]
                    oLUT = [0, 0.375, 0.625, 0.75, 0.875, 1]
                    lLUT = map(lambda x: "%f:%f"%(iLUT[x]/CFlist[band-1],oLUT[x]*omax), range(len(iLUT)))
                    LUT = ",".join(lLUT)

                if info.stretch != "ns":
                    logger.debug("Band Calibration Factors: %i %f" %(band, CFlist[band-1]))
                logger.debug("Band stretch parameters: %i %s" %(band, LUT))

                ComplexSourceXML = ('<ComplexSource>'
                                    '   <SourceFilename relativeToVRT="0">%s</SourceFilename>'
                                    '   <SourceBand>%s</SourceBand>'
                                    '   <ScaleOffset>0</ScaleOffset>'
                                    '   <ScaleRatio>1</ScaleRatio>'
                                    '   <LUT>%s</LUT>'
                                    '   <NODATA>0</NODATA>'
                                    '   <SrcRect xOff="0" yOff="0" xSize="%d" ySize="%d"/>'
                                    '   <DstRect xOff="0" yOff="0" xSize="%d" ySize="%d"/>'
                                    '</ComplexSource>)' %(info.warpfile,band,LUT,xsize,ysize,xsize,ysize))

                vds.GetRasterBand(band).SetMetadataItem("source_0", ComplexSourceXML, "new_vrt_sources")
                if vds.GetRasterBand(band).GetColorInterpretation() == gdalconst.GCI_AlphaBand:
                    vds.GetRasterBand(band).SetColorInterpretation(gdalconst.GCI_Undefined)
        else:
            LogMsg("Cannot create virtual dataset: %s" %info.vrtfile)

    else:
        LogMsg("Cannot open dataset: %s" %wds_fp)

    vds = None
    wds = None

    if opt.format == 'GTiff':
        if opt.gtiff_compression == 'lzw':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=IF_SAFER" '
        elif opt.gtiff_compression == 'jpeg95':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "compress=jpeg" -co "jpeg_quality=95" -co "BIGTIFF=IF_SAFER" '

    elif opt.format == 'HFA':
        co = '-co "COMPRESSED=YES" -co "STATISTICS=YES" '

    elif opt.format == 'JP2OpenJPEG':   #### add rgb constraint if openjpeg (3 bands only, also test if 16 bit possible)?
        co = '-co "QUALITY=25" '

    else:
        co = ''

    pf = platform.platform()
    if pf.startswith("Linux"):
        config_options = '--config GDAL_CACHEMAX 2048'
    else:
        config_options = ''

    if opt.no_pyramids:
        base_cmd = 'gdal_translate'
    else:
        base_cmd = 'gdal_translate -stats'



    cmd = ('%s %s -ot %s -a_srs "%s" %s%s-of %s "%s" "%s"' %(
        base_cmd,
        config_options,
        opt.outtype,
        opt.spatial_ref.proj4,
        info.rgb_bands,
        co,
        opt.format,
        info.vrtfile,
        info.localdst
        ))

    (err,so,se) = ExecCmd(cmd)
    if err == 1:
        rc = 1

    #### Calculate Pyramids
    if not opt.no_pyramids:
        if opt.format in ["GTiff"]:
            if os.path.isfile(info.localdst):
                cmd = ('gdaladdo "%s" 2 4 8 16' %(info.localdst))
                (err,so,se) = ExecCmd(cmd)
                if err == 1:
                    rc = 1

    #### Write .prj File
    if os.path.isfile(info.localdst):
        txtpath = os.path.splitext(info.localdst)[0] + '.prj'
        txt = open(txtpath,'w')
        txt.write(prj)
        txt.close()

    return rc


def GetImageStats(opt, info):

    #### Add code to read info from IKONOS blu image

    rc = 0
    info.extent = ""
    info.centerlong = ""

    vendor, sat = getSensor(info.srcfn)

    if vendor is None:
        rc = 1

    info.sat = sat
    info.vendor = vendor

    info.stretch = opt.stretch

    if info.vendor == 'GeoEye' and info.sat == 'IK01' and "_msi_" in info.srcfn:
        src_image_name = info.srcfn.replace("_msi_","_blu_")
        src_image = os.path.join(info.srcdir,src_image_name)
        info.bands = 4
    else:
        src_image = info.localsrc
        info.bands = None

    ds = gdal.Open(info.localsrc,gdalconst.GA_ReadOnly)
    if ds is not None:

        ####  Get extent from GCPs
        num_gcps = ds.GetGCPCount()
        if info.bands is None:
            info.bands = ds.RasterCount

        if num_gcps == 4:
            gcps = ds.GetGCPs()
            proj = ds.GetGCPProjection()

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

            xsize = gcp_dict[1][0] - gcp_dict[2][0]
            ysize = gcp_dict[1][1] - gcp_dict[4][1]

        else:
            xsize = ds.RasterXSize
            ysize = ds.RasterYSize
            proj = ds.GetProjectionRef()
            gtf = ds.GetGeoTransform()


            ulx = gtf[0] + 0 * gtf[1] + 0 * gtf[2]
            uly = gtf[3] + 0 * gtf[4] + 0 * gtf[5]
            urx = gtf[0] + xsize * gtf[1] + 0 * gtf[2]
            ury = gtf[3] + xsize * gtf[4] + 0 * gtf[5]
            llx = gtf[0] + 0 * gtf[1] + ysize * gtf[2]
            lly = gtf[3] + 0 * gtf[4] + ysize * gtf[5]
            lrx = gtf[0] + xsize * gtf[1] + ysize* gtf[2]
            lry = gtf[3] + xsize * gtf[4] + ysize * gtf[5]
            
            #print xsize, ysize
            #print gtf
            #print proj
            #print ulx, uly
            #print urx, ury
            #print llx, lly
            #print lrx,lry

        ds = None

        ####  Create geometry objects
        ul = "POINT ( %.12f %.12f )" %(ulx, uly)
        ur = "POINT ( %.12f %.12f )" %(urx, ury)
        ll = "POINT ( %.12f %.12f )" %(llx, lly)
        lr = "POINT ( %.12f %.12f )" %(lrx, lry)
        poly_wkt = 'POLYGON (( %.12f %.12f, %.12f %.12f, %.12f %.12f, %.12f %.12f, %.12f %.12f ))' %(ulx,uly,urx,ury,lrx,lry,llx,lly,ulx,uly)

        ul_geom = ogr.CreateGeometryFromWkt(ul)
        ur_geom = ogr.CreateGeometryFromWkt(ur)
        ll_geom = ogr.CreateGeometryFromWkt(ll)
        lr_geom = ogr.CreateGeometryFromWkt(lr)
        extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)

        #### Create srs objects
        s_srs = osr.SpatialReference(proj)
        t_srs = opt.spatial_ref.srs
        g_srs = osr.SpatialReference()
        g_srs.ImportFromEPSG(WGS84)
        sg_ct = osr.CoordinateTransformation(s_srs,g_srs)
        gt_ct = osr.CoordinateTransformation(g_srs,t_srs)
        tg_ct = osr.CoordinateTransformation(t_srs,g_srs)

        #### Transform geometries to geographic
        if not s_srs.IsSame(g_srs):
            ul_geom.Transform(sg_ct)
            ur_geom.Transform(sg_ct)
            ll_geom.Transform(sg_ct)
            lr_geom.Transform(sg_ct)
            extent_geom.Transform(sg_ct)
        LogMsg("Geographic extent: %s" %str(extent_geom))

        #### Get Lat and Lon coords in arrays
        lons = []
        lats = []
        for pt in ul_geom, ur_geom, ll_geom, lr_geom:
            lons.append(pt.GetX())
            lats.append(pt.GetY())

        #### Transform geoms to target srs
        if not g_srs.IsSame(t_srs):
            ul_geom.Transform(gt_ct)
            ur_geom.Transform(gt_ct)
            ll_geom.Transform(gt_ct)
            lr_geom.Transform(gt_ct)
            extent_geom.Transform(gt_ct)
        LogMsg("Projected extent: %s" %str(extent_geom))

        info.geometry_wkt = extent_geom.ExportToWkt()
        #### Get centroid and back project to geographic coords (this is neccesary for images that cross 180)
        centroid = extent_geom.Centroid()
        centroid.Transform(tg_ct)

        #### Get X and Y coords in arrays
        Xs = []
        Ys = []
        for pt in ul_geom, ur_geom, ll_geom, lr_geom:
            Xs.append(pt.GetX())
            Ys.append(pt.GetY())

        #print lons
        LogMsg("Centroid: %s" %str(centroid))

        if max(lons) - min(lons) > 180:

            if centroid.GetX() < 0:
                info.centerlong = '--config CENTER_LONG -180 '
            else:
                info.centerlong = '--config CENTER_LONG 180 '

        info.extent = "-te %.12f %.12f %.12f %.12f " %(min(Xs),min(Ys),max(Xs),max(Ys))

        rasterxsize_m = abs(math.sqrt((ul_geom.GetX() - ur_geom.GetX())**2 + (ul_geom.GetY() - ur_geom.GetY())**2))
        rasterysize_m = abs(math.sqrt((ul_geom.GetX() - ll_geom.GetX())**2 + (ul_geom.GetY() - ll_geom.GetY())**2))

        resx = abs(math.sqrt((ul_geom.GetX() - ur_geom.GetX())**2 + (ul_geom.GetY() - ur_geom.GetY())**2)/ xsize)
        resy = abs(math.sqrt((ul_geom.GetX() - ll_geom.GetX())**2 + (ul_geom.GetY() - ll_geom.GetY())**2)/ ysize)


        ####  Make a string for Pixel Size Specification
        if opt.resolution is not None:
            info.res = "-tr %s %s " %(opt.resolution,opt.resolution)
        else:
            info.res = "-tr %.12f %.12f " %(resx,resy)
        LogMsg("Original image size: %f x %f, res: %.12f x %.12f" %(rasterxsize_m, rasterysize_m, resx, resy))


        #### Set RGB bands
        info.rgb_bands = ""

        if opt.rgb is True:
            if info.bands == 4:
                info.rgb_bands = "-b 3 -b 2 -b 1 "
            elif info.bands == 8:
                info.rgb_bands = "-b 5 -b 3 -b 2 "

        if opt.bgrn is True:
            if info.bands == 8:
                info.rgb_bands = "-b 2 -b 3 -b 5 -b 7 "


    else:
        LogMsg("Cannot open dataset: %s" %info.localsrc)
        rc = 1

    return info, rc


def GetDGMetadataPath(srcfp):
    """
    Returns the filepath of the XML, if it can be found. Returns
    None if no valid filepath could be found.
    """
    if os.path.isfile(os.path.splitext(srcfp)[0]+'.xml'):
        metapath = os.path.splitext(srcfp)[0]+'.xml'
    elif os.path.isfile(os.path.splitext(srcfp)[0]+'.XML'):
        metapath = os.path.splitext(srcfp)[0]+'.XML'
    else:
        # Tiled DG images may have a metadata file at the strip level
        metapath = None
        filename = os.path.basename(srcfp)
        match = re.match(PGC_DG_FILE, filename)
        if match:
            try:
                # Build the expected strip-level metadata filepath using
                # parts of the source image filepath
                metapath = os.path.dirname(srcfp)
                metapath = os.path.join(metapath, match.group('pgcpfx'))
                metapath += "_{}".format(match.group('ts'))
                metapath += "-{}".format(match.group('prod'))
                metapath += "-{}".format(match.group('oid'))

                if os.path.isfile(metapath + '.xml'):
                    metapath += ".xml"
                elif os.path.isfile(metapath + ".XML"):
                    metapath += ".XML"
            # If any of the groups we use to build the metapath aren't there,
            # a name error will be thrown, which means we won't be able to find
            # the metapath.
            except NameError:
                metapath = None
    if metapath and os.path.isfile(metapath):
        return metapath
    else:
        return None


def GetIKMetadataPath(srcfp):
    """
    Same as GetDGMetadataPath, but for Ikonos.
    """
    # Most of the time, the metadata file will be the same filename
    # except for the extension or with the band name replaced with rgb.
    # However, some IK metadata will be for
    # an entire strip, and will have a different filename, which we
    # will look for if we need to.
    metapath = os.path.splitext(srcfp)[0]+'.txt'
    
    if not os.path.isfile(metapath):
        for b in ikMsiBands:
            mp = metapath.replace(b,'rgb')
            if os.path.isfile(mp):
                metapath = mp
                break
            
    if not os.path.isfile(metapath):
        source_filename = os.path.basename(srcfp)
        match = re.match(PGC_IK_FILE, source_filename)
        if match:
            try:
                # Build the expected strip-level metadata filepath using
                # parts of the source image filepath
                metapath = os.path.dirname(srcfp)
                metapath = os.path.join(metapath, match.group('pgcpfx'))
                metapath += "_po_{}".format(match.group('po'))

                if os.path.isfile(metapath + '_metadata.txt'):
                    metapath += "_metadata.txt"
            # If any of the groups we use to build the metapath aren't there,
            # a name error will be thrown, which means we won't be able to find
            # the metapath.
            except NameError:
                metapath = None
    if metapath and os.path.isfile(metapath):
        return metapath
    else:
        return None
    
    
def GetGEMetadataPath(srcfp):
    """
    Same as GetDGMetadataPath, but for GE01.
    """
    # Most of the time, the metadata file will be the same filename
    # except for the extension. However, some IK metadata will be for
    # an entire strip, and will have a different filename, which we
    # will look for if we need to.
    metapath = os.path.splitext(srcfp)[0]+'.txt'
    if not os.path.isfile(metapath):
        metapath = os.path.splitext(srcfp)[0]+'.pvl'
    if os.path.isfile(metapath):
        return metapath
    else:
        return None


def WriteOutputMetadata(opt,info):

    ####  Ortho metadata name
    omd = os.path.splitext(info.localdst)[0] + ".xml"

    ####  Get xml/pvl metadata
    ####  If DG
    if info.vendor == 'DigitalGlobe':
        metapath = info.metapath
        
        try:
            metad = ET.parse(metapath)
        except ET.ParseError:
            LogMsg("Invalid xml formatting in metadata file: %s" %metapath)
            return 1
        else:
            imd = metad.find("IMD")

    ####  If GE
    elif info.vendor == 'GeoEye' and info.sat == "GE01":
        
        metad = getGEMetadataAsXml(info.metapath)
        imd = ET.Element("IMD")
        include_tags = ["sensorInfo","inputImageInfo","correctionParams","bandSpecificInformation"]

        elem = metad.find("productInfo")
        if elem is not None:
            rpc = elem.find("rationalFunctions")
            elem.remove(rpc)
            imd.append(elem)

        for tag in include_tags:
            elems = metad.findall(tag)
            imd.extend(elems)

        
    elif info.sat in ['IK01']:
        imd = None
        # TODO: write code for IK metadata

    ####  Determine custom MD
    dMD = {}
    tm = datetime.today()
    dMD["PROCESS_DATE"] = tm.strftime("%d-%b-%Y %H:%M:%S")
    if opt.dem:
        dMD["ORTHO_DEM"] = os.path.basename(opt.dem)
    elif opt.ortho_height is not None:
        dMD["ORTHO_HEIGHT"] = str(opt.ortho_height)
    else:
        h = get_rpc_height(info)
        dMD["ORTHO_HEIGHT"] = str(h)
    dMD["RESAMPLEMETHOD"] = opt.resample
    dMD["STRETCH"] = opt.stretch
    dMD["BITDEPTH"] = opt.outtype
    dMD["FORMAT"] = opt.format
    dMD["COMPRESSION"] = opt.gtiff_compression
    #dMD["BANDNUMBER"]
    #dMD["BANDMAP"]
    dMD["EPSG_CODE"] = str(opt.epsg)

    pgcmd = ET.Element("PGC_IMD")
    for tag in dMD:
        child = ET.SubElement(pgcmd,tag)
        child.text = dMD[tag]

    ####  Write output

    root = ET.Element("IMD")

    root.append(pgcmd)

    ref = ET.SubElement(root,"SOURCE_IMD")
    child = ET.SubElement(ref,"SOURCE_IMAGE")
    child.text = os.path.basename(info.localsrc)
    child = ET.SubElement(ref,"VENDOR")
    child.text = info.vendor

    if imd is not None:
        ref.append(imd)

    ET.ElementTree(root).write(omd)
    return 0


def WarpImage(opt,info):

    rc = 0

    pf = platform.platform()
    if pf.startswith("Linux"):
        config_options = '-wm 2000 --config GDAL_CACHEMAX 2048 --config GDAL_NUM_THREADS 1'
    else:
        config_options = '--config GDAL_NUM_THREADS 1'

    if not os.path.isfile(info.warpfile):

        LogMsg("Warping Image")
        
        if not opt.skip_warp:
        
            #### If Image is TIF, extract RPB
            if os.path.splitext(info.localsrc)[1].lower() == ".tif":
                if info.vendor == "DigitalGlobe":
                    rpb_p = os.path.splitext(info.localsrc)[0] + ".RPB"
    
                elif info.vendor == "GeoEye" and info.sat == "GE01":
                    rpb_p = os.path.splitext(info.localsrc)[0] + "_rpc.txt"
    
                else:
                    rpb_p = None
                    logger.error("Cannot extract rpc's for Ikonos. Image cannot be terrain corrected with a DEM or avg elevation.")
                    rc = 1
    
                if rpb_p:
                    if not os.path.isfile(rpb_p):
                        err = ExtractRPB(info.localsrc,rpb_p)
                        if err == 1:
                            rc = 1
                    if not os.path.isfile(rpb_p):
                        logger.error("No RPC information found. Image cannot be terrain corrected with a DEM or avg elevation.")
                        rc = 1
                        
                        
        #### convert to VRT and modify 4th band
        cmd = 'gdal_translate -of VRT "{0}" "{1}"'.format(info.localsrc,info.rawvrt)
        (err,so,se) = ExecCmd(cmd)
        if err == 1:
            rc = 1
        
        if os.path.isfile(info.rawvrt) and info.bands > 3:
            vds = gdal.Open(info.rawvrt,gdalconst.GA_Update)
            if vds.GetRasterBand(4).GetColorInterpretation() == 6:
                vds.GetRasterBand(4).SetColorInterpretation(gdalconst.GCI_Undefined)
            vds = None

        nodata_list = ["0"] * info.bands
        
        
        if not opt.skip_warp:

            if rc <> 1:
                ####  Set RPC_DEM or RPC_HEIGHT transformation option
                if opt.dem != None:
                    LogMsg('DEM: %s' %(os.path.basename(opt.dem)))
                    to = "RPC_DEM=%s" %opt.dem
    
                elif opt.ortho_height is not None:
                    LogMsg("Elevation: {0} meters".format(opt.ortho_height))
                    to = "RPC_HEIGHT=%f" %opt.ortho_height
                    
                else:
                    #### Get Constant Elevation From XML
                    h = get_rpc_height(info)
                    LogMsg("Average elevation: %f meters" %(h))
                    to = "RPC_HEIGHT=%f" %h
                    ds = None
                    
                
                #### GDALWARP Command
                cmd = 'gdalwarp %s -srcnodata "%s" -of GTiff -ot UInt16 %s%s%s-co "TILED=YES" -co "BIGTIFF=IF_SAFER" -t_srs "%s" -r %s -et 0.01 -rpc -to "%s" "%s" "%s"' %(
                    config_options,
                    " ".join(nodata_list),
                    info.centerlong,
                    info.extent,
                    info.res,
                    opt.spatial_ref.proj4,
                    opt.resample,
                    to,
                    info.rawvrt,
                    info.warpfile
                    )           
                
                (err,so,se) = ExecCmd(cmd)
                #print err
                if err == 1:
                    rc = 1
                
                
        else:
            #### GDALWARP Command
            cmd = 'gdalwarp %s -srcnodata "%s" -of GTiff -ot UInt16 %s-co "TILED=YES" -co "BIGTIFF=IF_SAFER" -t_srs "%s" -r %s "%s" "%s"' %(
                config_options,
                " ".join(nodata_list),
                info.res,
                opt.spatial_ref.proj4,
                opt.resample,
                info.rawvrt,
                info.warpfile
                )           
            
            (err,so,se) = ExecCmd(cmd)
            #print err
            if err == 1:
                rc = 1
                
        return rc


def get_rpc_height(info):
    ds = gdal.Open(info.localsrc,gdalconst.GA_ReadOnly)
    if ds is not None:
        m = ds.GetMetadata("RPC")
        m2 = ds.GetMetadata("RPB")
        if "HEIGHT_OFF" in m:
            h1 = m["HEIGHT_OFF"]
            h = float(''.join([c for c in h1 if c in '1234567890.+-']))
        elif "HEIGHTOFFSET" in m:
            h1 = m["HEIGHTOFFSET"]
            h = float(''.join([c for c in h1 if c in '1234567890.+-']))
        elif "HEIGHT_OFF" in m2:
            h1 = m["HEIGHT_OFF"]
            h = float(''.join([c for c in h1 if c in '1234567890.+-']))
        elif "HEIGHTOFFSET" in m2:
            h1 = m["HEIGHTOFFSET"]
            h = float(''.join([c for c in h1 if c in '1234567890.+-']))
        else:
            h = 0
            LogMsg("Cannot determine avg elevation. Using 0.")
    else:
        h = 0
        LogMsg("Cannot determine avg elevation. Using 0.")
    return h


def GetCalibrationFactors(info):

    calibDict = {}
    CFlist = []

    if info.vendor == "DigitalGlobe":

        xmlpath = info.metapath
        calibDict = getDGXmlData(xmlpath,info.stretch)
        bandList = DGbandList

    elif info.vendor == "GeoEye" and info.sat == "GE01":

        metapath = info.metapath
        calibDict = GetGEcalibDict(metapath,info.stretch)
        if info.bands == 1:
            bandList = [5]
        elif info.bands == 4:
            bandList = range(1,5,1)

    elif info.vendor == "GeoEye" and info.sat == "IK01":
        metapath = info.metapath
        calibDict = GetIKcalibDict(metapath,info.stretch)
        if info.bands == 1:
            bandList = [4]
        elif info.bands == 4:
            bandList = range(0,4,1)

    else:
        LogMsg( "Vendor or sensor not recognized: %s, %s" %(info.vendor, info.sat))

    #LogMsg("Calibration factors: %s"%calibDict)
    if len(calibDict) > 0:

        for band in bandList:
            if band in calibDict:
                CFlist.append(calibDict[band])

    LogMsg("Calibration factor list: %s"%CFlist)
    return CFlist


def overlap_check(geometry_wkt, spatial_ref, demPath):


    imageSpatialReference = spatial_ref.srs
    imageGeometry = ogr.CreateGeometryFromWkt(geometry_wkt)
    dem = gdal.Open(demPath, gdalconst.GA_ReadOnly)

    if dem is not None:
            xsize = dem.RasterXSize
            ysize = dem.RasterYSize
            demProjection = dem.GetProjectionRef()
            if demProjection:

                gtf = dem.GetGeoTransform()

                minx = gtf[0]
                maxx = minx + xsize * gtf[1]
                maxy = gtf[3]
                miny = maxy + ysize * gtf[5]

                dem_geometry_wkt = 'POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))' %(minx,miny,minx,maxy,maxx,maxy,maxx,miny,minx,miny)
                demGeometry = ogr.CreateGeometryFromWkt(dem_geometry_wkt)
		LogMsg("DEM extent: %s" %demGeometry)
                demSpatialReference = osr.SpatialReference(demProjection)

                coordinateTransformer = osr.CoordinateTransformation(imageSpatialReference, demSpatialReference)
		if not imageSpatialReference.IsSame(demSpatialReference):
		    #LogMsg("Image Spatial Refernce: %s" %imageSpatialReference)
		    #LogMsg("DEM Spatial ReferenceL %s" %demSpatialReference)
		    #LogMsg("Image Geometry before transformation: %s" %imageGeometry)
		    LogMsg("Transforming image geometry to dem spatial reference")
                    imageGeometry.Transform(coordinateTransformer)
		    #LogMsg("Image Geometry after transformation: %s" %imageGeometry)

                dem = None
                overlap = imageGeometry.Within(demGeometry)

                if overlap is False:
                    LogMsg("ERROR - Image is not contained within DEM extent")

            else:
                LogMsg("ERROR - DEM has no spatial reference information: %s" %demPath)
                overlap = False

    else:
        LogMsg("ERROR - Cannot open DEM to determine extent: %s" %demPath)
        overlap = False


    return overlap


def ExtractRPB(item,rpb_p):
    rc = 0
    tar_p = os.path.splitext(item)[0]+".tar"
    LogMsg(tar_p)
    if os.path.isfile(tar_p):
        try:
            tar = tarfile.open(tar_p, 'r')
            tarlist = tar.getnames()
            for t in tarlist:
                if '.rpb' in string.lower(t) or '_rpc' in string.lower(t): #or '.til' in string.lower(t):
                    tf = tar.extractfile(t)
                    fp = os.path.splitext(rpb_p)[0] + os.path.splitext(t)[1]
                    fpfh = open(fp,"w")
                    tfstr = tf.read()
                    #print repr(tfstr)
                    fpfh.write(tfstr)
                    fpfh.close()
                    tf.close()
                    status = 0
        except Exception,e:
            logger.error("Cannot open Tar file: %s" %tar_p)
            rc = 1
    else:
        LogMsg("Tar file does not exist: %s" %tar_p)
        rc = 1

    if rc == 1:
        LogMsg("Cannot extract RPC file.  Orthorectification will fail.")
    return rc


def calcEarthSunDist(t):
    year = t.year
    month = t.month
    day = t.day
    hr = t.hour
    minute = t.minute
    sec = t.second
    ut = hr + (minute/60) + (sec/3600)
    #print ut

    if month <= 2:
        year = year - 1
        month = month + 12

    a = int(year/100)
    b = 2 - a + int(a/4)
    jd = int(365.25*(year+4716)) + int(30.6001*(month+1)) + day + (ut/24) + b - 1524.5
    #print jd

    g = 357.529 + 0.98560028 * (jd-2451545.0)
    d = 1.00014 - 0.01671 * math.cos(math.radians(g)) - 0.00014 * math.cos(math.radians(2*g))
    #print d

    return d


def getDGXmlData(xmlpath,stretch):
    calibDict = {}
    abscalfact_dict = {}
    try:
        xmldoc = minidom.parse(xmlpath)
    except Exception, e:
        logger.error("Cannot parse metadata file: {0}".format(xmlpath))
        return None
    else:

        if len(xmldoc.getElementsByTagName('IMD')) >=1:
    
            nodeIMD = xmldoc.getElementsByTagName('IMD')[0]
            EsunDict = {  # Spectral Irradiance in W/m2/um
                'QB02_BAND_P':1381.79,
                'QB02_BAND_B':1924.59,
                'QB02_BAND_G':1843.08,
                'QB02_BAND_R':1574.77,
                'QB02_BAND_N':1113.71,
                
                'WV01_BAND_P':1487.54715,
                
                'WV02_BAND_P':1580.8140,
                'WV02_BAND_C':1758.2229,
                'WV02_BAND_B':1974.2416,
                'WV02_BAND_G':1856.4104,
                'WV02_BAND_Y':1738.4791,
                'WV02_BAND_R':1559.4555,
                'WV02_BAND_RE':1342.0695,
                'WV02_BAND_N':1069.7302,
                'WV02_BAND_N2':861.2866,
    
                'WV03_BAND_P':1588.54256,
                'WV03_BAND_C':1803.910899,
                'WV03_BAND_B':1982.448496,
                'WV03_BAND_G':1857.123219,
                'WV03_BAND_Y':1746.59472,
                'WV03_BAND_R':1556.972971,
                'WV03_BAND_RE':1340.682185,
                'WV03_BAND_N':1072.526674,
                'WV03_BAND_N2':871.105797,
                'WV03_BAND_S1':494.4049774,
                'WV03_BAND_S2':261.6434525,
                'WV03_BAND_S3':230.4614177,
                'WV03_BAND_S4':196.7908515,
                'WV03_BAND_S5':80.35901853,
                'WV03_BAND_S6':74.81263622,
                'WV03_BAND_S7':69.01250464,
                'WV03_BAND_S8':59.79459729,
    
                'GE01_BAND_P':1617,
                'GE01_BAND_B':1960,
                'GE01_BAND_G':1853,
                'GE01_BAND_R':1505,
                'GE01_BAND_N':1039,
    
                'IK01_BAND_P':1375.8,
                'IK01_BAND_B':1930.9,
                'IK01_BAND_G':1854.8,
                'IK01_BAND_R':1556.5,
                'IK01_BAND_N':1156.9
                }
    
            # get acquisition IMAGE tags
            nodeIMAGE = nodeIMD.getElementsByTagName('IMAGE')
    
            sat = nodeIMAGE[0].getElementsByTagName('SATID')[0].firstChild.data
            t = nodeIMAGE[0].getElementsByTagName('FIRSTLINETIME')[0].firstChild.data
    
            if len(nodeIMAGE[0].getElementsByTagName('MEANSUNEL')) >= 1:
                sunEl = float(nodeIMAGE[0].getElementsByTagName('MEANSUNEL')[0].firstChild.data)
            elif len(nodeIMAGE[0].getElementsByTagName('SUNEL')) >= 1:
                sunEl = float(nodeIMAGE[0].getElementsByTagName('SUNEL')[0].firstChild.data)
            else:
                return None
    
            
            sunAngle = 90 - sunEl
            des = calcEarthSunDist(datetime.strptime(t,"%Y-%m-%dT%H:%M:%S.%fZ"))
            
            # get BAND tags
            for band in DGbandList:
                nodeBAND = nodeIMD.getElementsByTagName(band)
                #print nodeBAND
                if not len(nodeBAND) == 0:
                    
                    temp = nodeBAND[0].getElementsByTagName('ABSCALFACTOR')
                    if not len(temp) == 0:
                        abscal = float(temp[0].firstChild.data)
                        
                    else:
                        return None
                        
                    temp = nodeBAND[0].getElementsByTagName('EFFECTIVEBANDWIDTH')
                    if not len(temp) == 0:
                        effbandw = float(temp[0].firstChild.data)
                    else:
                        return None
                    
                    abscalfact_dict[band] = (abscal,effbandw)
            
            #### Determine if unit shift factor should be applied
            
            ## 1) If BAND_B abscalfact < 0.004, then units are in W/cm2/nm and should be multiplied 
            ##  by 10 in order to get units of W/m2/um
            ## 1) If BAND_P abscalfact < 0.01, then units are in W/cm2/nm and should be multiplied 
            ##  by 10 in order to get units of W/m2/um
            
            units_factor = 1
            if sat == 'GE01':
                if 'BAND_B' in abscalfact_dict:
                    if abscalfact_dict['BAND_B'][0] < 0.004:
                        units_factor = 10
                if 'BAND_P' in abscalfact_dict:
                    if abscalfact_dict['BAND_P'][0] < 0.01:
                        units_factor = 10
            
            for band in abscalfact_dict:
                satband = sat+'_'+band
                if satband not in EsunDict:
                    LogMsg("Cannot find sensor and band in Esun lookup table: %s.  Try using --stretch ns." %satband)
                    return None
                else:
                    Esun = EsunDict[satband]
                
                abscal,effbandw = abscalfact_dict[band]
                
                #print abscal,des,Esun,math.cos(math.radians(sunAngle)),effbandw
                
                radfact = units_factor * (abscal/effbandw)
                reflfact = units_factor * ((abscal * des**2 * math.pi) / (Esun * math.cos(math.radians(sunAngle)) * effbandw))
                
                LogMsg("{0}: absCalFact {1}, Earth-Sun distance {2}, Esun {3}, sun angle {4}, sun elev {5}, effBandwidth {6}, units factor {9}, reflectance factor {7}, radience factor {8}".format(satband, abscal, des, Esun, sunAngle, sunEl, effbandw, reflfact, radfact, units_factor))
                
                if stretch == "rd":
                    calibDict[band] = radfact
                else:
                    calibDict[band] = reflfact

    return calibDict


def GetIKcalibDict(metafile,stretch):
    fp_mode = "renamed"
    metadict = getIKMetadata(fp_mode,metafile)
    #print metadict

    calibDict = {}
    EsunDict = [1930.9, 1854.8, 1556.5, 1156.9, 1375.8] # B,G,R,N,Pan(TDI13)
    bwList = [71.3, 88.6, 65.8, 95.4, 403] # B,G,R,N,Pan(TDI13)
    calCoefs1 = [633, 649, 840, 746, 161] # B,G,R,N,Pan(TDI13) - Pre 2/22/01
    calCoefs2 = [728, 727, 949, 843, 161] # B,G,R,N,Pan(TDI13) = Post 2/22/01


    for band in range(0,5,1):
        sunElStr = metadict["Sun_Angle_Elevation"]
        sunAngle = float(sunElStr.strip(" degrees"))
        datestr = metadict["Acquisition_Date_Time"] # 2011-12-09 18:43 GMT
        d = datetime.strptime(datestr,"%Y-%m-%d %H:%M GMT")
        des = calcEarthSunDist(d)

        breakdate = datetime(2001,2,22)
        if d < breakdate:
            calCoef = calCoefs1[band]
        else:
            calCoef = calCoefs2[band]

        bw = bwList[band]
        Esun = EsunDict[band]
        
        #print sunAngle, des, gain, Esun
        radfact = 10000.0 / (calCoef * bw )
        reflfact = (10000.0 * des**2 * math.pi) / (calCoef * bw * Esun * math.cos(math.radians(sunAngle)))
        
        LogMsg("{0}: calibration coef {1}, Earth-Sun distance {2}, Esun {3}, sun angle {4}, bandwidth {5}, reflectance factor {6}, radience factor {7}".format(band, calCoef, des, Esun, sunAngle, bw, reflfact, radfact))
        
        if stretch == "rd":
            calibDict[band] = radfact
        else:
            calibDict[band] = reflfact

    return calibDict


def getIKMetadata(fp_mode, metafile):
    ik2fp = [
            ("File_Format", "OUTPUT_FMT"),
            ("Product_Order_Number", "ORDER_ID"),
            ("Bits_per_Pixel_per_Band", "BITS_PIXEL"),
            ("Source_Image_ID", "CAT_ID"),
            ("Acquisition_Date_Time", "ACQ_TIME"),
            ("Scan_Direction", "SCAN_DIR"),
            ("Country_Code", "COUNTRY"),
            ("Percent_Component_Cloud_Cover", "CLOUDCOVER"),
            ("Sensor_Name", "SENSOR"),
            ("Sun_Angle_Elevation", "SUN_ELEV")
    	]

    metad = getIKMetadataAsXml(metafile)
    if metad is not None:
        metadict = {}
        search_keys = dict(ik2fp)

    else:
        LogMsg("Unable to parse metadata from %s" % metafile)
        return None

    metad_map = dict((c, p) for p in metad.getiterator() for c in p)  # Child/parent mapping
    attribs = ["Source_Image_ID", "Component_ID"]  # nodes we need the attributes of

    # We must identify the exact Source_Image_ID and Component_ID for this image
    # before loading the dictionary

    # In raw mode we match the image file name to a Component_ID, this yields a Product_Image_ID,
    # which in turn links to a Source_Image_ID; we accomplish this by examining all the
    # Component_File_Name nodes for an image file name match, on a hit, the parent node of
    # the CFN node will be the Component_ID node we are interested in

    if fp_mode == "renamed":

        # In renamed mode, we find the Source Image ID (from the file name) and then find
        # the matching Component ID

        # For new style(pgctools3) filename, we use a regex to get the source image ID.
        # If we can't match the filename to the new style name regex, we assume
        # that we have old style names, and we can get the source image ID directly from
        # the filename, as shown below.
        match = re.search(PGC_IK_FILE, metafile.lower())
        if match:
            siid = match.group('catid')
        else:
            siid = os.path.basename(metafile.lower())[5:33]
        siid_nodes = metad.findall(r".//Source_Image_ID")
        if siid_nodes is None:
            LogMsg( "Could not find any Source Image ID fields in metadata %s" % metafile)
            return None

        siid_node = None
        for node in siid_nodes:
            if node.attrib["id"] == siid:
                siid_node = node
                break
        
        if siid_node is None:
            LogMsg( "Could not locate SIID: %s in metadata %s" % (siid, metafile))
            return None


    # Now assemble the dict
    for node in siid_node.getiterator():
        if node.tag in search_keys:
            if node.tag == "Source_Image_ID":
                metadict[node.tag] = node.attrib["id"]
            else:
                metadict[node.tag] = node.text

        
    return metadict


def getIKMetadataAsXml(metafile):
	"""
	Given the text of an IKONOS metadata file, returns all the key/pair values as a
	searchable XML tree
	"""
	if not metafile:
		return ET.Element("root")  # No metadata provided, return an empty tree

	# If metafile is a file, open it and read from it, otherwise assume a list of strings
	if os.path.isfile(metafile) and os.path.getsize(metafile) > 0:
		try:
			metaf = open(metafile, "r")
		except IOError, err:
			LogMsg( "Could not open metadata file %s because %s" % (metafile, err))
			raise
	else:
		metaf = metafile

	# Patterns to identify tag/value pairs and group tags
	ikpat1 = re.compile(r"(?P<tag>.+?): (?P<data>.+)?", re.I)
	ikpat2 = re.compile(r"(?P<tag>[a-zA-Z ()]+)", re.I)

	# Lists of tags known to be at a certain depth of the tree, to be used as
	# attributes rather than nodes or ignored altogether
	tags_1L = ["Product_Order_Metadata", "Source_Image_Metadata", "Product_Space_Metadata",
			   "Product_Component_Metadata"]
	tags_2L = ["Source_Image_ID", "Component_ID"]
	tags_coords = ["Latitude", "Longitude", "Map_X_(Easting)", "Map_Y_(Northing)",
				   "UL_Map_X_(Easting)", "UL_Map_Y_(Northing)"]
	ignores = ["Company Information", "Address", "GeoEye", "12076 Grant Street",
			  "Thornton, Colorado 80241", "U.S.A.", "Contact Information",
			  "On the Web: http://www.geoeye.com", "Customer Service Phone (U.S.A.): 1.800.232.9037",
			  "Customer Service Phone (World Wide): 1.703.480.5670",
			  "Customer Service Fax (World Wide): 1.703.450.9570", "Customer Service Email: info@geoeye.com",
			  "Customer Service Center hours of operation:", "Monday - Friday, 8:00 - 20:00 Eastern Standard Time"
			  ]

	# Start processing
	root = ET.Element("root")
	parent = None
	current = root
	node_stack = []

	for line in metaf:
		item = line.strip()
		if item in ignores:
			continue  # Skip this stuff

		# Can't have spaces or slashes in node tags
		item = item.replace(" ", "_").replace("/", "_")

		# If we've found a top-level group name, handle it here
		if item in tags_1L:
			child = ET.SubElement(root, item)
			node_stack = []  # top-level nodes are children of root so reset
			parent = root
			current = child

		# Everything else
		else:
			mat1 = ikpat1.search(line)
			mat2 = ikpat2.search(line) if not mat1 else None

			# Tag/value pair
			if mat1:
				tag = mat1.group("tag").strip().replace(" ", "_").replace("/", "_")
				if mat1.group("data"):
					data = mat1.group("data").strip()
				else:
					data = ""

				# Second-level groups define major blocks
				if tag in tags_2L:
					# We may have been working on a different second-level tag, so
					# reset the stack and pointers as needed
					while current.tag not in tags_1L and current.tag != "root":
						current = parent
						parent = node_stack.pop()

					# Now add the new child node
					child = ET.SubElement(current, tag)
					child.set("id", data)  # Currently, all 2L tags are IDs
					node_stack.append(parent)
					parent = current
					current = child

				# Handle 'Coordinate' tags as a special case
				elif tag == "Coordinate":
					# If we were working on a Coordinate, back up a level
					if current.tag == "Coordinate":
						child = ET.SubElement(parent, tag)
						child.set("id", data)
						current = child
					else:
						child = ET.SubElement(current, tag)
						child.set("id", data)
						node_stack.append(parent)
						parent = current
						current = child

				# Vanilla tag/value pair
				else:
					# Adjust depth if we just finished a Coordinate block
					if tag not in tags_coords and current.tag == "Coordinate":
						while current.tag not in tags_2L and current.tag not in tags_1L and current.tag != "root":
							current = parent
							parent = node_stack.pop()

					# Add a standard node
					child = ET.SubElement(current, tag)
					child.text = data

			# Handle new group names
			elif mat2:
				tag = mat2.group("tag").strip()

				# Except for Coordinates there aren't really any 4th level tags we care about, so we always
				# back up until current points at a second or top-level node
				while current.tag not in tags_2L and current.tag not in tags_1L and current.tag != "root":
					current = parent
					parent = node_stack.pop()

				# Now add the new group node
				child = ET.SubElement(current, tag)
				node_stack.append(parent)
				parent = current
				current = child

	return ET.ElementTree(root)


def GetGEcalibDict(metafile,stretch):
    fp_mode = "renamed"
    metadict = getGEMetadata(fp_mode,metafile)
    #print metadict

    calibDict = {}
    EsunDict = [196.0, 185.3, 150.5, 103.9, 161.7]


    for band in metadict["gain"].keys():
        sunAngle = float(metadict["firstLineSunElevationAngle"])
        datestr = metadict["originalFirstLineAcquisitionDateTime"] # 2009-11-01T01:49:33.685421Z
        des = calcEarthSunDist(datetime.strptime(datestr,"%Y-%m-%dT%H:%M:%S.%fZ"))
        gain = float(metadict["gain"][band])
        Esun = EsunDict[band-1]

        #print sunAngle, des, gain, Esun
        radfact = gain
        reflfact = (gain * des**2 * math.pi) / (Esun * math.cos(math.radians(sunAngle)))

        if stretch == "rd":
            calibDict[band] = radfact
        else:
            calibDict[band] = reflfact

    return calibDict


def getGEMetadata(fp_mode, metafile):
	metadict = {}
	metad = getGEMetadataAsXml(metafile)
	if metad is not None:

            search_keys = ["originalFirstLineAcquisitionDateTime", "firstLineSunElevationAngle"]
            for key in search_keys:
                node = metad.find(".//%s" % key)
                if node is not None:
                    metadict[key] = node.text

            band_keys = ["gain", "offset"]
            for key in band_keys:
                nodes = metad.findall(".//bandSpecificInformation")

                vals = {}
                for node in nodes:
                    try:
                        band = int(node.attrib["bandNumber"])
                    except Exception, e:
                        LogMsg("Unable to retrieve band number in GE metadata")
                    else:
                        node = node.find(".//%s" % key)
                        if node is not None:
                            vals[band] = node.text
                metadict[key] = vals
	else:
            LogMsg("Unable to get metadata from %s" % metafile)

	return metadict


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


def XmlToJ2w(jp2p):

    xmlp = jp2p+".aux.xml"
    xml = open(xmlp,'r')
    for line in xml:
        if "<GeoTransform>" in line:
            gt = line[line.find("<GeoTransform>")+len("<GeoTransform>"):line.find("</GeoTransform>")]
            gtl = gt.split(",")
            wldl = [float(gtl[1]),float(gtl[2]),float(gtl[4]),float(gtl[5]),float(gtl[0])+float(gtl[1])*0.5,float(gtl[3])+float(gtl[5])*0.5]
    xml.close()

    j2wp = xmlp[:xmlp.find(".")] + ".j2w"
    j2w = open(j2wp,"w")
    for param in wldl:
        #print param
        j2w.write("%f\n"%param)
    j2w.close()


def deleteTempFiles(names):

    #LogMsg('Deleting Temp Files')
    for name in names:
        deleteList = glob.glob(os.path.splitext(name)[0]+'.*')
        for f in deleteList:
            if not "log" in os.path.basename(f):
                try:
                    os.remove(f)
                except Exception, e:
                    LogMsg('Could not remove %s: %s' %(os.path.basename(f),e))


def LogMsg(msg):
    logger.info(msg)


def ExecCmd(cmd):
    logger.info(cmd)

    p = Popen(cmd,stdout=PIPE,stderr=PIPE,shell=True)
    (so,se) = p.communicate()
    rc = p.wait()
    err = 0

    if rc != 0:
        logger.error("Error found - Return Code = %s:  %s" %(rc,cmd))
        err = 1
    else:
        logger.debug("Return Code = %s:  %s" %(rc,cmd))

    logger.debug("STDOUT:  "+so)
    logger.debug("STDERR:  "+se)
    return (err,so,se)


def ExecCmd_mp(job):
    job_name, cmd = job
    logger.info('Running job: {0}'.format(job_name))
    logger.debug('Cmd: {0}'.format(cmd))
    p = Popen(cmd,shell=True,stderr=PIPE,
              stdout=PIPE,preexec_fn=os.setsid)
    try:
        (so,se) = p.communicate()
    except KeyboardInterrupt:
        os.killpg(p.pid, signal.SIGTERM)
    
    else:
        logger.debug(so)
        logger.debug(se)
    

def getBitdepth(outtype):
    if outtype == "Byte":
        bitdepth = 'u08'
    elif outtype == "UInt16":
        bitdepth = "u16"
    elif outtype == "Float32":
        bitdepth = "f32"

    return bitdepth


def getSensor(srcfn):

    ### Regex signatures to identify file vendor, mode, kind, and create the name_dict
    RAW_DG = "(?P<ts>\d\d[a-z]{3}\d{8})-(?P<prod>\w{4})?(?P<tile>\w+)?-(?P<oid>\d{12}_\d\d)_(?P<pnum>p\d{3})"

    RENAMED_DG = "(?P<snsr>\w\w\d\d)_(?P<ts>\d\d[a-z]{3}\d{9})-(?P<prod>\w{4})?(?P<tile>\w+)?-(?P<catid>[a-z0-9]+)"

    RENAMED_DG2 = "(?P<snsr>\w\w\d\d)_(?P<ts>\d{14})_(?P<catid>[a-z0-9]{16})"

    RAW_GE = "(?P<snsr>\d[a-z])(?P<ts>\d{6})(?P<band>[a-z])(?P<said>\d{9})(?P<prod>\d[a-z])(?P<pid>\d{3})(?P<siid>\d{8})(?P<ver>\d)(?P<mono>[a-z0-9])_(?P<pnum>\d{8,9})"

    RENAMED_GE = "(?P<snsr>\w\w\d\d)_(?P<ts>\d{6})(?P<band>\w)(?P<said>\d{9})(?P<prod>\d\w)(?P<pid>\d{3})(?P<siid>\d{8})(?P<ver>\d)(?P<mono>\w)_(?P<pnum>\d{8,9})"

    RAW_IK = "po_(?P<po>\d{5,7})_(?P<band>[a-z]+)_(?P<cmp>\d+)"

    RENAMED_IK = "(?P<snsr>[a-z]{2}\d\d)_(?P<ts>\d{12})(?P<siid>\d+)_(?P<band>[a-z]+)_(?P<lat>\d{4}[ns])"

    sat = None
    vendor = None

    DG_patterns = [RAW_DG, RENAMED_DG, RENAMED_DG2]
    GE_patterns = [RAW_GE, RENAMED_GE]
    IK_patterns = [RAW_IK, RENAMED_IK]

    for pattern in DG_patterns:
        p = re.compile(pattern)
        m = p.search(srcfn.lower())
        if m is not None:
            vendor = "DigitalGlobe"
            gd = m.groupdict()
            if 'snsr' in gd:
                sat = gd['snsr']

    for pattern in GE_patterns:
        p = re.compile(pattern)
        m = p.search(srcfn.lower())
        if m is not None:
            vendor = "GeoEye"
            sat = "GE01"

    for pattern in IK_patterns:
        p = re.compile(pattern)
        m = p.search(srcfn.lower())
        if m is not None:
            vendor = "GeoEye"
            sat = "IK01"

    return vendor, sat


def FindImages(inpath,exts):

    image_list = []
    for root,dirs,files in os.walk(inpath):
        for f in  files:
            if os.path.splitext(f)[1].lower() in exts:
                image_path = os.path.join(root,f)
                image_path = string.replace(image_path,'\\','/')
                image_list.append(image_path)
    return image_list
