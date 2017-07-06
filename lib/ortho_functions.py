import os, string, sys, shutil, math, glob, re, tarfile, logging, platform, argparse
from datetime import datetime, timedelta

from lib import utils, taskhandler
from xml.dom import minidom
from xml.etree import cElementTree as ET

import gdal, ogr, osr, gdalconst

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

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
                            )
                         ?(?P<tail>[a-z0-9_-]+(?=\.))?      # Descriptor (optional)
                         (?P<ext>\.[a-z0-9][a-z0-9.]*)      # File name extension
                         """, re.I | re.X)

DG_FILE = re.compile(r"""
                        (?P<oname>\d{2}[a-z]{3}\d{8}-[a-z0-9_]{4,9}-\d{12}_\d{2}_P\d{3})
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

class ImageInfo:
    pass

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
    parser.add_argument("--gtiff-compression", choices=gtiff_compressions, default="lzw",
                      help="GTiff compression type (default=lzw)")
    parser.add_argument("-p", "--epsg", required=True, type=int,
                      help="epsg projection code for output files")
    parser.add_argument("-d", "--dem",
                      help="the DEM to use for orthorectification (elevation values should be relative to the wgs84 ellipoid")
    parser.add_argument("-t", "--outtype", choices=outtypes, default="Byte",
                      help="output data type (default=Byte)")
    parser.add_argument("-r", "--resolution", type=float,
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
    parser.add_argument("--skip-warp", action='store_true', default=False,
                      help="skip warping step")
    parser.add_argument("--skip-dem-overlap-check", action='store_true', default=False,
                      help="skip verification of image-DEM overlap")
    parser.add_argument("--no-pyramids", action='store_true', default=False, help='suppress calculation of output image pyramids and stats')
    parser.add_argument("--ortho-height", type=long, help='constant elevation to use for orthorectification (value should be in meters above the wgs84 ellipoid)')


    return parser, pos_arg_keys


def process_image(srcfp,dstfp,args,target_extent_geom=None):
    
    err = 0

    #### Instantiate ImageInfo object
    info = ImageInfo()
    info.srcfp = srcfp
    info.srcdir,info.srcfn = os.path.split(srcfp)
    info.dstfp = dstfp
    info.dstdir,info.dstfn = os.path.split(dstfp)

    starttime = datetime.today()
    logger.info('Image: %s' %(info.srcfn))

    #### Get working dir
    if args.wd is not None:
        wd = args.wd
    else:
        wd = info.dstdir
    if not os.path.isdir(wd):
        try:
            os.makedirs(wd)
        except OSError:
            pass
    logger.info("Working Dir: %s" %wd)

    #### Derive names
    info.localsrc = os.path.join(wd,info.srcfn)
    info.localdst = os.path.join(wd,info.dstfn)
    info.rawvrt = os.path.splitext(info.localsrc)[0] + "_raw.vrt"
    info.warpfile = os.path.splitext(info.localsrc)[0] + "_warp.tif"
    info.vrtfile = os.path.splitext(info.localsrc)[0] + "_vrt.vrt"

    #### Verify EPSG
    try:
        spatial_ref = utils.SpatialRef(args.epsg)
    except RuntimeError, e:
        logger.error("Invalid EPSG code: {0)".format(args.epsg))
        err = 1
    else:
        args.spatial_ref = spatial_ref

    #### Verify that dem and ortho_height are not both specified
    if args.dem is not None and args.ortho_height is not None:
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
        if (gd['prod'][1:3] == '2A' and gd['tile'] is not None and gd['ext'] == '.tif') and not args.skip_warp:
            logger.error("Cannot process 2A tiled Geotiffs")
            err = 1

    #### Find metadata file
    if not err == 1:
        metafile = GetDGMetadataPath(info.srcfp)
        if metafile is None:
            metafile = ExtractDGMetadataFile(info.srcfp,wd)
        if metafile is None:
            metafile = GetIKMetadataPath(info.srcfp)
        if metafile is None:
            metafile = GetGEMetadataPath(info.srcfp)
        if metafile is None:
            logger.error("Cannot find metadata for image: {0}".format(info.srcfp))
            err = 1
        else:
            info.metapath = metafile

    #### Check If Image is IKONOS msi that does not exist, if so, stack to dstdir, else, copy srcfn to dstdir
    if not err == 1:
        if "IK01" in info.srcfn and "msi" in info.srcfn and not os.path.isfile(info.srcfp):
            logger.info("Converting IKONOS band images to composite image")
            members = [os.path.join(info.srcdir,info.srcfn.replace("msi",b)) for b in ikMsiBands]
            status = [os.path.isfile(member) for member in members]
            if sum(status) != 4:
                logger.error("1 or more IKONOS multispectral member images are missing %s" %", ".join(members))
                err = 1
            elif not os.path.isfile(info.localsrc):
                rc = stackIkBands(info.localsrc, members)
                #if not os.path.isfile(os.path.join(wd,os.path.basename(info.metapath))):
                #    shutil.copy(info.metapath, os.path.join(wd,os.path.basename(info.metapath)))
                if rc == 1:
                    logger.error("Error building merged Ikonos image: %s" %info.srcfp)
                    err = 1

        else:
            if os.path.isfile(info.srcfp):
                logger.info("Copying image to working directory")
                copy_list = glob.glob("%s.*" %os.path.splitext(info.srcfp)[0])
                #copy_list.append(info.metapath)
                for fpi in copy_list:
                    fpo = os.path.join(wd,os.path.basename(fpi))
                    if not os.path.isfile(fpo):
                        shutil.copy2(fpi,fpo)

            else:
                logger.warning("Source image does not exist: %s" %info.srcfp)
                err = 1


    #### Get Image Stats
    if not err == 1:
        info, rc = GetImageStats(args,info,target_extent_geom)
        if rc == 1:
            err = 1
            logger.errpr("Error in stats calculation")

    #### Check that DEM overlaps image
    if not err == 1:
        if args.dem and not args.skip_dem_overlap_check:
            overlap = overlap_check(info.geometry_wkt, args.spatial_ref, args.dem)
            if overlap is False:
                err = 1
    
    if not os.path.isfile(info.dstfp):
        #### Warp Image
        if not err == 1 and not os.path.isfile(info.warpfile):
            rc = WarpImage(args,info)
            if rc == 1:
                err = 1
                logger.error("Error in image warping")
    
        #### Calculate Output File
        if not err == 1 and os.path.isfile(info.warpfile):
            rc = calcStats(args,info)
            if rc == 1:
                err = 1
                logger.error("Error in image calculation")

    ####  Write Output Metadata
    if not err == 1:
        rc = WriteOutputMetadata(args,info)
        if rc == 1:
            err = 1
            logger.error("Error in writing metadata file")

    #### Copy image to final location if working dir is used
    if args.wd is not None:
        if not err == 1:
            logger.info("Copying to destination directory")
            for fpi in glob.glob("%s.*" %os.path.splitext(info.localdst)[0]):
                fpo = os.path.join(info.dstdir,os.path.basename(fpi))
                if not os.path.isfile(fpo):
                    shutil.copy2(fpi,fpo)
        if not args.save_temps:
            utils.delete_temp_files([info.localdst])

    #### Check If Done, Delete Temp Files
    done = os.path.isfile(info.dstfp)
    if done is False:
        err = 1
        logger.error("Final image not present")

    if err == 1:
        logger.error("Processing failed: %s" %info.srcfn)
        if not args.save_temps:
            utils.delete_temp_files([dstfp,info.rawvrt,info.warpfile,info.vrtfile,info.localsrc])

    elif not args.save_temps:
        utils.delete_temp_files([info.rawvrt,info.warpfile,info.vrtfile,info.localsrc])

    #### Calculate Total Time
    endtime = datetime.today()
    td = (endtime-starttime)
    logger.info("Total Processing Time: %s\n" %(td))

    return err


def stackIkBands(dstfp, members):

    rc = 0

    band_dict = {1:gdalconst.GCI_BlueBand,2:gdalconst.GCI_GreenBand,3:gdalconst.GCI_RedBand,4:gdalconst.GCI_Undefined}
    remove_keys = ("NITF_FHDR","NITF_IREP","NITF_OSTAID","NITF_IC","NITF_ICORDS","NITF_IGEOLO") #"NITF_FHDR"
    meta_dict = {"NITF_IREP":"MULTI"}

    srcfp = members[0]
    srcdir,srcfn = os.path.split(srcfp)
    dstdir,dstfn = os.path.split(dstfp)
    vrt = os.path.splitext(dstfp)[0]+ "_merge.vrt"

    #### Gather metadata from original blue image and save as strings for merge command
    logger.info("Stacking IKONOS MSI bands")
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
        (err,so,se) = taskhandler.exec_cmd(cmd)
        if err == 1:
            rc = 1

        cmd = 'gdal_translate -a_srs "%s" -of NITF -co "IC=NC" %s %s "%s" "%s"' %(s_srs_proj4,string.join(m_list, " "), string.join(tre_list, " "), vrt, dstfp )

        (err,so,se) = taskhandler.exec_cmd(cmd)
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


def calcStats(args,info):

    logger.info("Calculating image with stats")
    rc = 0

    #### Get Well-known Text String of Projection from EPSG Code
    p = args.spatial_ref.srs
    prj = p.ExportToWkt()

    imax = 2047.0

    if info.stretch == 'ns':
        if args.outtype == "Byte":
            omax = 255.0
        elif args.outtype == "UInt16":
            omax = 2047.0
        elif args.outtype == "Float32":
            omax = 2047.0
    else:
        if args.outtype == "Byte":
            omax = 200.0
        elif args.outtype == "UInt16":
            omax = 2000.0
        elif args.outtype == "Float32":
            omax = 1.0

    #### Stretch
    if info.stretch != "ns":
        CFlist = GetCalibrationFactors(info)
        if len(CFlist) == 0:
            logger.error("Cannot get image calibration factors from metadata")
            return 1

        if len(CFlist) < info.bands:
            logger.error("Metadata image calibration factors have fewer bands than the image")
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
                    # iLUT = [0, 0.125, 0.25, 0.375, 0.625, 1]
                    # oLUT = [0, 0.375, 0.625, 0.75, 0.875, 1]
                    iLUT = [0, 0.125, 0.25, 0.375, 1.0]
                    oLUT = [0, 0.675, 0.85, 0.9675, 1.2]
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
                                    '   <SrcRect xOff="0" yOff="0" xSize="%d" ySize="%d"/>'
                                    '   <DstRect xOff="0" yOff="0" xSize="%d" ySize="%d"/>'
                                    '</ComplexSource>)' %(info.warpfile,band,LUT,xsize,ysize,xsize,ysize))

                vds.GetRasterBand(band).SetMetadataItem("source_0", ComplexSourceXML, "new_vrt_sources")
                vds.GetRasterBand(band).SetNoDataValue(0)
                if vds.GetRasterBand(band).GetColorInterpretation() == gdalconst.GCI_AlphaBand:
                    vds.GetRasterBand(band).SetColorInterpretation(gdalconst.GCI_Undefined)
        else:
            logger.error("Cannot create virtual dataset: %s" %info.vrtfile)

    else:
        logger.error("Cannot open dataset: %s" %wds_fp)

    vds = None
    wds = None

    if args.format == 'GTiff':
        if args.gtiff_compression == 'lzw':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=IF_SAFER" '
        elif args.gtiff_compression == 'jpeg95':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "compress=jpeg" -co "jpeg_quality=95" -co "BIGTIFF=IF_SAFER" '

    elif args.format == 'HFA':
        co = '-co "COMPRESSED=YES" -co "STATISTICS=YES" '

    elif args.format == 'JP2OpenJPEG':   #### add rgb constraint if openjpeg (3 bands only, also test if 16 bit possible)?
        co = '-co "QUALITY=25" '

    else:
        co = ''

    pf = platform.platform()
    if pf.startswith("Linux"):
        config_options = '--config GDAL_CACHEMAX 2048'
    else:
        config_options = ''

    if args.no_pyramids:
        base_cmd = 'gdal_translate'
    else:
        base_cmd = 'gdal_translate -stats'

    cmd = ('%s %s -ot %s -a_srs "%s" %s%s-of %s "%s" "%s"' %(
        base_cmd,
        config_options,
        args.outtype,
        args.spatial_ref.proj4,
        info.rgb_bands,
        co,
        args.format,
        info.vrtfile,
        info.localdst
        ))

    (err,so,se) = taskhandler.exec_cmd(cmd)
    if err == 1:
        rc = 1

    #### Calculate Pyramids
    if not args.no_pyramids:
        if args.format in ["GTiff"]:
            if os.path.isfile(info.localdst):
                cmd = ('gdaladdo "%s" 2 4 8 16' %(info.localdst))
                (err,so,se) = taskhandler.exec_cmd(cmd)
                if err == 1:
                    rc = 1

    #### Write .prj File
    if os.path.isfile(info.localdst):
        txtpath = os.path.splitext(info.localdst)[0] + '.prj'
        txt = open(txtpath,'w')
        txt.write(prj)
        txt.close()

    return rc


def GetImageStats(args, info, target_extent_geom=None):

    #### Add code to read info from IKONOS blu image
    rc = 0
    info.extent = ""
    info.centerlong = ""
    vendor, sat = utils.get_sensor(info.srcfn)

    if vendor is None:
        rc = 1

    info.sat = sat
    info.vendor = vendor

    info.stretch = args.stretch

    if info.vendor == 'GeoEye' and info.sat == 'IK01' and "_msi_" in info.srcfn:
        src_image_name = info.srcfn.replace("_msi_","_blu_")
        src_image = os.path.join(info.srcdir,src_image_name)
        info.bands = 4
    else:
        src_image = info.localsrc
        info.bands = None

    ds = gdal.Open(src_image,gdalconst.GA_ReadOnly)
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
            print gtf


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
        t_srs = args.spatial_ref.srs
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
        logger.info("Geographic extent: %s" %str(extent_geom))

        #### Get geographic Envelope
        minlon, maxlon, minlat, maxlat = extent_geom.GetEnvelope()

        #### Transform geoms to target srs
        if not g_srs.IsSame(t_srs):
            ul_geom.Transform(gt_ct)
            ur_geom.Transform(gt_ct)
            ll_geom.Transform(gt_ct)
            lr_geom.Transform(gt_ct)
            extent_geom.Transform(gt_ct)
        logger.info("Projected extent: %s" %str(extent_geom))
        
        ## test user provided extent and ues if appropriate
        if target_extent_geom:
            if not extent_geom.Intersects(target_extent_geom):
                rc = 1
            else:
                logger.info("Using user-provided extent: %s" %str(target_extent_geom))
                extent_geom = target_extent_geom
        
        if rc <> 1:
            info.extent_geom = extent_geom
            info.geometry_wkt = extent_geom.ExportToWkt()
            #### Get centroid and back project to geographic coords (this is neccesary for images that cross 180)
            centroid = extent_geom.Centroid()
            centroid.Transform(tg_ct)
    
            #### Get projected Envelope
            minx, maxx, miny, maxy = extent_geom.GetEnvelope()
    
            #print lons
            logger.info("Centroid: %s" %str(centroid))
    
            if maxlon - minlon > 180:
    
                if centroid.GetX() < 0:
                    info.centerlong = '--config CENTER_LONG -180 '
                else:
                    info.centerlong = '--config CENTER_LONG 180 '
    
            info.extent = "-te %.12f %.12f %.12f %.12f " %(minx,miny,maxx,maxy)
    
            rasterxsize_m = abs(math.sqrt((ul_geom.GetX() - ur_geom.GetX())**2 + (ul_geom.GetY() - ur_geom.GetY())**2))
            rasterysize_m = abs(math.sqrt((ul_geom.GetX() - ll_geom.GetX())**2 + (ul_geom.GetY() - ll_geom.GetY())**2))
    
            resx = abs(math.sqrt((ul_geom.GetX() - ur_geom.GetX())**2 + (ul_geom.GetY() - ur_geom.GetY())**2)/ xsize)
            resy = abs(math.sqrt((ul_geom.GetX() - ll_geom.GetX())**2 + (ul_geom.GetY() - ll_geom.GetY())**2)/ ysize)
    
            ####  Make a string for Pixel Size Specification
            if args.resolution is not None:
                info.res = "-tr %s %s " %(args.resolution,args.resolution)
            else:
                info.res = "-tr %.12f %.12f " %(resx,resy)
            logger.info("Original image size: %f x %f, res: %.12f x %.12f" %(rasterxsize_m, rasterysize_m, resx, resy))
    
            #### Set RGB bands
            info.rgb_bands = ""
    
            if args.rgb is True:
                if info.bands == 1:
                    pass
                elif info.bands == 3:
                    info.rgb_bands = "-b 3 -b 2 -b 1 "
                elif info.bands == 4:
                    info.rgb_bands = "-b 3 -b 2 -b 1 "
                elif info.bands == 8:
                    info.rgb_bands = "-b 5 -b 3 -b 2 "
                else:
                    logger.error("Cannot get rgb bands from a {0} band image".format(info.bands))
                    rc = 1
    
            if args.bgrn is True:
                if info.bands == 1:
                    pass
                elif info.bands == 4:
                    pass
                elif info.bands == 8:
                    info.rgb_bands = "-b 2 -b 3 -b 5 -b 7 "
                else:
                    logger.error("Cannot get bgrn bands from a {0} band image".format(info.bands))
                    rc = 1
                
    else:
        logger.error("Cannot open dataset: %s" %info.localsrc)
        rc = 1

    return info, rc


def GetDGMetadataPath(srcfp):
    """
    Returns the filepath of the XML, if it can be found. Returns
    None if no valid filepath could be found.
    """

    filename = os.path.basename(srcfp)

    if os.path.isfile(os.path.splitext(srcfp)[0]+'.xml'):
        metapath = os.path.splitext(srcfp)[0]+'.xml'
    elif os.path.isfile(os.path.splitext(srcfp)[0]+'.XML'):
        metapath = os.path.splitext(srcfp)[0]+'.XML'
    else:
        # Tiled DG images may have a metadata file at the strip level
        metapath = None
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


def ExtractDGMetadataFile(srcfp, wd):
    """
    Searches the .tar for a valid XML. If found,
    extracts the metadata file. Returns
    None if no valid metadata could be found.
    """

    metapath = None
    filename = os.path.basename(srcfp)
    tarpath = os.path.splitext(srcfp)[0] + '.tar'
    if os.path.isfile(tarpath):
        match = re.search(DG_FILE, filename)
        if match:
            metaname = match.group('oname')

            try:
                tar = tarfile.open(tarpath, 'r')
                tarlist = tar.getnames()
                for t in tarlist:
                    if metaname.lower() in t.lower() and os.path.splitext(t)[1].lower() == ".xml":
                        tf = tar.extractfile(t)
                        metapath = os.path.join(wd, os.path.splitext(filename)[0]+os.path.splitext(t)[1].lower())
                        fpfh = open(metapath,"w")
                        tfstr = tf.read()
                        fpfh.write(tfstr)
                        fpfh.close()
                        tf.close()
            except Exception,e:
                logger.error("Cannot open Tar file: %s" %tarpath)

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


def WriteOutputMetadata(args,info):

    ####  Ortho metadata name
    omd = os.path.splitext(info.localdst)[0] + ".xml"

    ####  Get xml/pvl metadata
    ####  If DG
    if info.vendor == 'DigitalGlobe':
        metapath = info.metapath

        try:
            metad = ET.parse(metapath)
        except ET.ParseError:
            logger.error("Invalid xml formatting in metadata file: %s" %metapath)
            return 1
        else:
            imd = metad.find("IMD")

    ####  If GE
    elif info.vendor == 'GeoEye' and info.sat == "GE01":

        metad = utils.getGEMetadataAsXml(info.metapath)
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
        match = PGC_IK_FILE.search(info.srcfn)
        if match:
            component = match.group('cmp')
            
            metad = utils.getIKMetadataAsXml(info.metapath)
            imd = ET.Element("IMD")
            
            elem = metad.find('Source_Image_Metadata')
            elem.remove(elem.find('Number_of_Source_Images'))
            for child in elem.findall("Source_Image_ID"):
                prod_id_elem = child.find("Product_Image_ID")
                if not prod_id_elem.text == component[:3]:
                    elem.remove(child)
            imd.append(elem)
     
            elem = metad.find('Product_Component_Metadata')
            elem.remove(elem.find('Number_of_Components'))
            for child in elem.findall("Component_ID"):
                if not child.attrib['id'] == component:
                    elem.remove(child)
            imd.append(elem)
    
            
            
    ####  Determine custom MD
    dMD = {}
    tm = datetime.today()
    dMD["PROCESS_DATE"] = tm.strftime("%d-%b-%Y %H:%M:%S")
    if not args.skip_warp:
        if args.dem:
            dMD["ORTHO_DEM"] = os.path.basename(args.dem)
        elif args.ortho_height is not None:
            dMD["ORTHO_HEIGHT"] = str(args.ortho_height)
        else:
            h = get_rpc_height(info)
            dMD["ORTHO_HEIGHT"] = str(h)
    dMD["RESAMPLEMETHOD"] = args.resample
    dMD["STRETCH"] = args.stretch
    dMD["BITDEPTH"] = args.outtype
    dMD["FORMAT"] = args.format
    dMD["COMPRESSION"] = args.gtiff_compression
    #dMD["BANDNUMBER"]
    #dMD["BANDMAP"]
    dMD["EPSG_CODE"] = str(args.epsg)

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

    #ET.ElementTree(root).write(omd,xml_declaration=True)
    xmlstring = prettify(root)
    fh = open(omd,'w')
    fh.write(xmlstring)
    return 0


def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def WarpImage(args,info):

    rc = 0

    pf = platform.platform()
    if pf.startswith("Linux"):
        config_options = '-wm 2000 --config GDAL_CACHEMAX 2048 --config GDAL_NUM_THREADS 1'
    else:
        config_options = '--config GDAL_NUM_THREADS 1'

    if not os.path.isfile(info.warpfile):

        logger.info("Warping Image")

        if not args.skip_warp:

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
        (err,so,se) = taskhandler.exec_cmd(cmd)
        if err == 1:
            rc = 1

        if os.path.isfile(info.rawvrt) and info.bands > 3:
            vds = gdal.Open(info.rawvrt,gdalconst.GA_Update)
            if vds.GetRasterBand(4).GetColorInterpretation() == 6:
                vds.GetRasterBand(4).SetColorInterpretation(gdalconst.GCI_Undefined)
            vds = None

        nodata_list = ["0"] * info.bands

        if not args.skip_warp:
            if rc <> 1:
                ####  Set RPC_DEM or RPC_HEIGHT transformation option
                if args.dem != None:
                    logger.info('DEM: %s' %(os.path.basename(args.dem)))
                    to = "RPC_DEM=%s" %args.dem

                elif args.ortho_height is not None:
                    logger.info("Elevation: {0} meters".format(args.ortho_height))
                    to = "RPC_HEIGHT=%f" %args.ortho_height

                else:
                    #### Get Constant Elevation From XML
                    h = get_rpc_height(info)
                    logger.info("Average elevation: %f meters" %(h))
                    to = "RPC_HEIGHT=%f" %h
                    ds = None


                #### GDALWARP Command
                cmd = 'gdalwarp %s -srcnodata "%s" -of GTiff -ot UInt16 %s%s%s-co "TILED=YES" -co "BIGTIFF=IF_SAFER" -t_srs "%s" -r %s -et 0.01 -rpc -to "%s" "%s" "%s"' %(
                    config_options,
                    " ".join(nodata_list),
                    info.centerlong,
                    info.extent,
                    info.res,
                    args.spatial_ref.proj4,
                    args.resample,
                    to,
                    info.rawvrt,
                    info.warpfile
                    )

                (err,so,se) = taskhandler.exec_cmd(cmd)
                #print err
                if err == 1:
                    rc = 1

        else:
            #### GDALWARP Command
            cmd = 'gdalwarp %s -srcnodata "%s" -of GTiff -ot UInt16 %s-co "TILED=YES" -co "BIGTIFF=IF_SAFER" -t_srs "%s" -r %s "%s" "%s"' %(
                config_options,
                " ".join(nodata_list),
                info.res,
                args.spatial_ref.proj4,
                args.resample,
                info.rawvrt,
                info.warpfile
                )

            (err,so,se) = taskhandler.exec_cmd(cmd)
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
            logger.warning("Cannot determine avg elevation. Using 0.")
    else:
        h = 0
        logger.warning("Cannot determine avg elevation. Using 0.")
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
        elif info.bands == 3:
            bandList = range(0,3,1)

    else:
        logger.warning( "Vendor or sensor not recognized: %s, %s" %(info.vendor, info.sat))

    #logger.info("Calibration factors: %s"%calibDict)
    if len(calibDict) > 0:

        for band in bandList:
            if band in calibDict:
                CFlist.append(calibDict[band])

    logger.info("Calibration factor list: %s"%CFlist)
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
                logger.info("DEM extent: %s" %demGeometry)
                demSpatialReference = osr.SpatialReference(demProjection)

                coordinateTransformer = osr.CoordinateTransformation(imageSpatialReference, demSpatialReference)
                if not imageSpatialReference.IsSame(demSpatialReference):
                    #logger.info("Image Spatial Refernce: %s" %imageSpatialReference)
                    #logger.info("DEM Spatial ReferenceL %s" %demSpatialReference)
                    #logger.info("Image Geometry before transformation: %s" %imageGeometry)
                    logger.info("Transforming image geometry to dem spatial reference")
                    imageGeometry.Transform(coordinateTransformer)
                    #logger.info("Image Geometry after transformation: %s" %imageGeometry)

                dem = None
                overlap = imageGeometry.Within(demGeometry)

                if overlap is False:
                    logger.error("Image is not contained within DEM extent")

            else:
                logger.error("DEM has no spatial reference information: %s" %demPath)
                overlap = False

    else:
        logger.error("Cannot open DEM to determine extent: %s" %demPath)
        overlap = False


    return overlap


def ExtractRPB(item,rpb_p):
    rc = 0
    tar_p = os.path.splitext(item)[0]+".tar"
    logger.info(tar_p)
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
        logger.info("Tar file does not exist: %s" %tar_p)
        rc = 1

    if rc == 1:
        logger.info("Cannot extract RPC file.  Orthorectification will fail.")
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
                    logger.warning("Cannot find sensor and band in Esun lookup table: %s.  Try using --stretch ns." %satband)
                    return None
                else:
                    Esun = EsunDict[satband]

                abscal,effbandw = abscalfact_dict[band]

                #print abscal,des,Esun,math.cos(math.radians(sunAngle)),effbandw

                radfact = units_factor * (abscal/effbandw)
                reflfact = units_factor * ((abscal * des**2 * math.pi) / (Esun * math.cos(math.radians(sunAngle)) * effbandw))

                logger.info("{0}: \n\tabsCalFactor {1}\n\teffectiveBandwidth {2}\n\tEarth-Sun distance {3}\n\tEsun {4}\n\tSun angle {5}\n\tSun elev {6}\n\tUnits factor {9}\n\tReflectance correction {7}\n\tRadiance correction {8}".format(satband, abscal, effbandw, des, Esun, sunAngle, sunEl, reflfact, radfact, units_factor))

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

        logger.info("{0}: calibration coef {1}, Earth-Sun distance {2}, Esun {3}, sun angle {4}, bandwidth {5}, reflectance factor {6}, radience factor {7}".format(band, calCoef, des, Esun, sunAngle, bw, reflfact, radfact))

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
        ("Sun_Angle_Elevation", "SUN_ELEV"),        
    ]

    metad = utils.getIKMetadataAsXml(metafile)
    if metad is not None:
        metadict = {}
        search_keys = dict(ik2fp)

    else:
        logger.error("Unable to parse metadata from %s" % metafile)
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
            logger.error( "Could not find any Source Image ID fields in metadata %s" % metafile)
            return None

        siid_node = None
        for node in siid_nodes:
            if node.attrib["id"] == siid:
                siid_node = node
                break

        if siid_node is None:
            logger.error( "Could not locate SIID: %s in metadata %s" % (siid, metafile))
            return None


    # Now assemble the dict
    for node in siid_node.getiterator():
        if node.tag in search_keys:
            if node.tag == "Source_Image_ID":
                metadict[node.tag] = node.attrib["id"]
            else:
                metadict[node.tag] = node.text


    return metadict


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
    metad = utils.getGEMetadataAsXml(metafile)
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
                        logger.error("Unable to retrieve band number in GE metadata")
                    else:
                        node = node.find(".//%s" % key)
                        if node is not None:
                            vals[band] = node.text
                metadict[key] = vals
    else:
        logger.error("Unable to get metadata from %s" % metafile)

    return metadict


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




