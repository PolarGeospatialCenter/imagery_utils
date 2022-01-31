
import argparse
import glob
import logging
import math
import os
import platform
import re
import shutil
import tarfile
from datetime import datetime
from xml.dom import minidom
from xml.etree import cElementTree as ET

from osgeo import gdal, gdalconst, ogr, osr

from lib import taskhandler, utils

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

DGbandList = ['BAND_P', 'BAND_C', 'BAND_B', 'BAND_G', 'BAND_Y', 'BAND_R', 'BAND_RE', 'BAND_N', 'BAND_N2', 'BAND_S1',
              'BAND_S2', 'BAND_S3', 'BAND_S4', 'BAND_S5', 'BAND_S6', 'BAND_S7', 'BAND_S8']
formats = {'GTiff': '.tif', 'JP2OpenJPEG': '.jp2', 'ENVI': '.envi', 'HFA': '.img', 'JPEG': '.jpg'}
outtypes = ['Byte', 'UInt16', 'Float32']
stretches = ["ns", "rf", "mr", "rd", "au"]
resamples = ["near", "bilinear", "cubic", "cubicspline", "lanczos"]
gtiff_compressions = ["jpeg95", "lzw"]
exts = ['.ntf', '.tif']
ARGDEF_THREADS = 1
try:
    # Python 3.x only
    ARGDEF_CPUS_AVAIL = os.cpu_count()
except AttributeError:
    # Python 2.x only
    import multiprocessing
    ARGDEF_CPUS_AVAIL = multiprocessing.cpu_count()


srs_wgs84 = utils.osr_srs_preserve_axis_order(osr.SpatialReference())
srs_wgs84.ImportFromEPSG(4326)

formatVRT = "VRT"
VRTdriver = gdal.GetDriverByName(formatVRT)
ikMsiBands = ['blu', 'grn', 'red', 'nir']
satList = ['WV01', 'QB02', 'WV02', 'GE01', 'IK01']

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



EsunDict = {  # Spectral Irradiance in W/m2/um (from Thuillier 2003 - used by DG calibration team as of 2016v2)
    'QB02_BAND_P': 1370.92,
    'QB02_BAND_B': 1949.59,
    'QB02_BAND_G': 1823.64,
    'QB02_BAND_R': 1553.78,
    'QB02_BAND_N': 1102.85,

    'WV01_BAND_P': 1478.62,

    'WV02_BAND_P': 1571.36,
    'WV02_BAND_C': 1773.81,
    'WV02_BAND_B': 2007.27,
    'WV02_BAND_G': 1829.62,
    'WV02_BAND_Y': 1701.85,
    'WV02_BAND_R': 1538.85,
    'WV02_BAND_RE': 1346.09,
    'WV02_BAND_N': 1053.21,
    'WV02_BAND_N2': 856.599,

    'WV03_BAND_P': 1574.41,
    'WV03_BAND_C': 1757.89,
    'WV03_BAND_B': 2004.61,
    'WV03_BAND_G': 1830.18,
    'WV03_BAND_Y': 1712.07,
    'WV03_BAND_R': 1535.33,
    'WV03_BAND_RE': 1348.08,
    'WV03_BAND_N': 1055.94,
    'WV03_BAND_N2': 858.77,
    'WV03_BAND_S1': 479.019,
    'WV03_BAND_S2': 263.797,
    'WV03_BAND_S3': 225.283,
    'WV03_BAND_S4': 197.552,
    'WV03_BAND_S5': 90.4178,
    'WV03_BAND_S6': 85.0642,
    'WV03_BAND_S7': 76.9507,
    'WV03_BAND_S8': 68.0988,

    'GE01_BAND_P': 1610.73,
    'GE01_BAND_B': 1993.18,
    'GE01_BAND_G': 1828.83,
    'GE01_BAND_R': 1491.49,
    'GE01_BAND_N': 1022.58,

    'IK01_BAND_P': 1353.25,
    'IK01_BAND_B': 1921.26,
    'IK01_BAND_G': 1803.28,
    'IK01_BAND_R': 1517.76,
    'IK01_BAND_N': 1145.8
}


GainDict = {  # Spectral Irradiance in W/m2/um
    'QB02_BAND_P': 0.870,
    'QB02_BAND_B': 1.105,
    'QB02_BAND_G': 1.071,
    'QB02_BAND_R': 1.060,
    'QB02_BAND_N': 1.020,

    'WV01_BAND_P': 1.016,

    'WV02_BAND_P': 0.942,
    'WV02_BAND_C': 1.151,
    'WV02_BAND_B': 0.988,
    'WV02_BAND_G': 0.936,
    'WV02_BAND_Y': 0.949,
    'WV02_BAND_R': 0.952,
    'WV02_BAND_RE': 0.974,
    'WV02_BAND_N': 0.961,
    'WV02_BAND_N2': 1.002,

    'WV03_BAND_P': 0.950,
    'WV03_BAND_C': 0.905,
    'WV03_BAND_B': 0.940,
    'WV03_BAND_G': 0.938,
    'WV03_BAND_Y': 0.962,
    'WV03_BAND_R': 0.964,
    'WV03_BAND_RE': 1.000,
    'WV03_BAND_N': 0.961,
    'WV03_BAND_N2': 0.978,
    'WV03_BAND_S1': 1.200,
    'WV03_BAND_S2': 1.227,
    'WV03_BAND_S3': 1.199,
    'WV03_BAND_S4': 1.196,
    'WV03_BAND_S5': 1.262,
    'WV03_BAND_S6': 1.314,
    'WV03_BAND_S7': 1.346,
    'WV03_BAND_S8': 1.376,

    'GE01_BAND_P': 0.970,
    'GE01_BAND_B': 1.053,
    'GE01_BAND_G': 0.994,
    'GE01_BAND_R': 0.998,
    'GE01_BAND_N': 0.994,

    'IK01_BAND_P': 0.907,
    'IK01_BAND_B': 1.073,
    'IK01_BAND_G': 0.990,
    'IK01_BAND_R': 0.940,
    'IK01_BAND_N': 1.043
}

BiasDict = {  # Spectral Irradiance in W/m2/um
    'QB02_BAND_P': -1.491,
    'QB02_BAND_B': -2.820,
    'QB02_BAND_G': -3.338,
    'QB02_BAND_R': -2.954,
    'QB02_BAND_N': -4.722,

    'WV01_BAND_P': -1.824,

    'WV02_BAND_P': -2.704,
    'WV02_BAND_C': -7.478,
    'WV02_BAND_B': -5.736,
    'WV02_BAND_G': -3.546,
    'WV02_BAND_Y': -3.564,
    'WV02_BAND_R': -2.512,
    'WV02_BAND_RE': -4.120,
    'WV02_BAND_N': -3.300,
    'WV02_BAND_N2': -2.891,

    'WV03_BAND_P': -3.629,
    'WV03_BAND_C': -8.604,
    'WV03_BAND_B': -5.809,
    'WV03_BAND_G': -4.996,
    'WV03_BAND_Y': -3.649,
    'WV03_BAND_R': -3.021,
    'WV03_BAND_RE': -4.521,
    'WV03_BAND_N': -5.522,
    'WV03_BAND_N2': -2.992,
    'WV03_BAND_S1': -5.546,
    'WV03_BAND_S2': -2.600,
    'WV03_BAND_S3': -2.309,
    'WV03_BAND_S4': -1.676,
    'WV03_BAND_S5': -0.705,
    'WV03_BAND_S6': -0.669,
    'WV03_BAND_S7': -0.512,
    'WV03_BAND_S8': -0.372,

    'GE01_BAND_P': -1.926,
    'GE01_BAND_B': -4.537,
    'GE01_BAND_G': -4.175,
    'GE01_BAND_R': -3.754,
    'GE01_BAND_N': -3.870,

    'IK01_BAND_P': -4.461,
    'IK01_BAND_B': -9.699,
    'IK01_BAND_G': -7.937,
    'IK01_BAND_R': -4.767,
    'IK01_BAND_N': -8.869
}


class ImageInfo:
    pass


def thread_type():
    def posintorall(arg_input):
        try:
            input_value = int(arg_input)
        except ValueError:
            if arg_input == "ALL_CPUS":
                return arg_input
            else:
                raise argparse.ArgumentTypeError("Must be a positive integer or ALL_CPUS")
        else:
            if input_value < 1:
                raise argparse.ArgumentTypeError("Must be a positive integer or ALL_CPUS")
            else:
                return input_value
    return posintorall


def buildParentArgumentParser():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(add_help=False)

    #### Positional Arguments
    parser.add_argument("src", help="source image, text file, or directory")
    parser.add_argument("dst", help="destination directory")
    pos_arg_keys = ["src", "dst"]


    ####Optional Arguments
    parser.add_argument("-f", "--format", choices=formats.keys(), default="GTiff",
                        help="output to the given format (default=GTiff)")
    parser.add_argument("--gtiff-compression", choices=gtiff_compressions, default="lzw",
                        help="GTiff compression type (default=lzw)")
    parser.add_argument("-p", "--epsg", required=False, type=str,
                        help="EPSG projection code for output files [int: EPSG code, "
                             "'utm': closest UTM zone, 'auto': closest UTM zone or polar stereo "
                             "(polar stereo cutoff is at 60 N/S latitude)]")
    parser.add_argument("-d", "--dem",
                        help="the DEM to use for orthorectification (elevation values should be relative to the wgs84 "
                             "ellipoid")
    parser.add_argument("-t", "--outtype", choices=outtypes, default="Byte",
                        help="output data type (default=Byte)")
    parser.add_argument("-r", "--resolution", type=float,
                        help="output pixel resolution in units of the projection")
    parser.add_argument("-c", "--stretch", choices=stretches, default="rf",
                        help="stretch type [ns: nostretch, rf: reflectance (default), mr: modified reflectance, rd: "
                             "absolute radiance, au: automatically set (rf for images below 60S latitude, otherwise mr)]")
    parser.add_argument("--resample", choices=resamples, default="near",
                        help="resampling strategy - mimicks gdalwarp options")
    parser.add_argument("--tap", action="store_true", default=False,
                        help="use gdalwarp target aligned pixels option")
    parser.add_argument("--rgb", action="store_true", default=False,
                        help="output multispectral images as 3 band RGB")
    parser.add_argument("--bgrn", action="store_true", default=False,
                        help="output multispectral images as 4 band BGRN (reduce 8 band to 4)")
    parser.add_argument("-s", "--save-temps", action="store_true", default=False,
                        help="save temp files, they will be renamed with a .save extension")
    parser.add_argument("--wd",
                        help='local working directory for cluster jobs (default is dst dir)'
                             'If used with --save-temps ALL files will be preserved in working directory')
    parser.add_argument("--skip-warp", action='store_true', default=False,
                        help="skip warping step")
    parser.add_argument("--skip-dem-overlap-check", action='store_true', default=False,
                        help="skip verification of image-DEM overlap")
    parser.add_argument("--no-pyramids", action='store_true', default=False,
                        help='suppress calculation of output image pyramids')
    parser.add_argument("--pyramid-type", choices=['near', 'cubic'], default='near', help='pyramid resampling strategy')
    parser.add_argument("--ortho-height", type=int,
                        help='constant elevation to use for orthorectification (value should be in meters above '
                        'the wgs84 ellipoid)')
    parser.add_argument("--threads", type=thread_type(),
                        help='Number of threads to use for gdalwarp and gdal_pansharpen processes, if applicable '
                             '(default={0}, number on system={1}). Can use any positive integer, or ALL_CPUS. '
                             'Any value above system count will default to ALL_CPUS. If used with '
                             '--parallel-processes, the (threads * number of processes) must be <= system count. '
                             '--pbs/--slurm will only accept 1 thread.'
                        .format(ARGDEF_THREADS, ARGDEF_CPUS_AVAIL),
                        default=ARGDEF_THREADS)
    parser.add_argument("--version", action='version', version="imagery_utils v{}".format(utils.package_version))

    return parser, pos_arg_keys


def process_image(srcfp, dstfp, args, target_extent_geom=None):

    err = 0

    #### Handle threads (default to 1 if arg not supplied)
    gdal_thread_count = 1 if not hasattr(args, 'threads') else args.threads

    #### Instantiate ImageInfo object
    info = ImageInfo()
    info.srcfp = srcfp
    info.srcdir, info.srcfn = os.path.split(srcfp)
    info.dstfp = dstfp
    info.dstdir, info.dstfn = os.path.split(dstfp)

    starttime = datetime.today()
    logger.info('Image: %s', info.srcfn)

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
    logger.info("Working Dir: %s", wd)

    #### Derive names
    if args.wd:
        info.localsrc = os.path.join(wd, info.srcfn)
    else:
        info.localsrc = info.srcfp
    info.localdst = os.path.join(wd, info.dstfn)
    info.rawvrt = os.path.splitext(info.localdst)[0] + "_raw.vrt"
    info.warpfile = os.path.splitext(info.localdst)[0] + "_warp.tif"
    info.vrtfile = os.path.splitext(info.localdst)[0] + "_vrt.vrt"

    # Cleanup temp files from failed or interrupted processing attempt
    if args.wd:
        utils.delete_temp_files([info.dstfp, info.rawvrt, info.warpfile, info.vrtfile, info.localsrc])
    else:
        utils.delete_temp_files([info.dstfp, info.rawvrt, info.warpfile, info.vrtfile])

    #### Verify EPSG
    # epsg argument could also be 'utm' or 'auto', handled in GetImageStats
    if type(args.epsg) is int:
        info.epsg = args.epsg
        try:
            spatial_ref = utils.SpatialRef(info.epsg)
        except RuntimeError as e:
            logger.error(utils.capture_error_trace())
            logger.error("Invalid EPSG code: %i", info.epsg)
            err = 1
        else:
            info.spatial_ref = spatial_ref
    else:
        # Determine automatic epsg and srs in GetImageStats
        info.epsg = None
        info.spatial_ref = None

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
            metafile = ExtractDGMetadataFile(info.srcfp, wd)
        if metafile is None:
            metafile = GetIKMetadataPath(info.srcfp)
        if metafile is None:
            metafile = GetGEMetadataPath(info.srcfp)
        if metafile is None:
            logger.error("Cannot find metadata for image: %s", info.srcfp)
            err = 1
        else:
            info.metapath = metafile

    #### Check If Image is IKONOS msi that does not exist, if so, stack to dstdir, else, copy srcfn to dstdir
    if not err == 1:
        if "IK01" in info.srcfn and "msi" in info.srcfn and not os.path.isfile(info.srcfp):
            info.localsrc = os.path.join(wd, info.srcfn)
            logger.info("Converting IKONOS band images to composite image")
            members = [os.path.join(info.srcdir, info.srcfn.replace("msi", b)) for b in ikMsiBands]
            status = [os.path.isfile(member) for member in members]
            if sum(status) != 4:
                logger.error("1 or more IKONOS multispectral member images are missing %s", ' '.join(members))
                err = 1
            elif not os.path.isfile(info.localsrc):
                rc = stackIkBands(info.localsrc, members)
                #if not os.path.isfile(os.path.join(wd, os.path.basename(info.metapath))):
                #    shutil.copy(info.metapath, os.path.join(wd, os.path.basename(info.metapath)))
                if rc == 1:
                    logger.error("Error building merged Ikonos image: %s", info.srcfp)
                    err = 1

    if not err == 1 and args.wd:
        def copy_to_wd(source_fp, wd):
            logger.info("Copying image to working directory")
            copy_list = glob.glob("{}.*".format(os.path.splitext(source_fp)[0]))
            # copy_list.append(info.metapath)
            for fpi in copy_list:
                fpo = os.path.join(wd, os.path.basename(fpi))
                if not os.path.isfile(fpo):
                    shutil.copy2(fpi, fpo)

        if os.path.isfile(info.srcfp):
            copy_to_wd(info.srcfp, wd)

        elif os.path.isfile(info.localsrc) and not os.path.isfile(info.srcfp):
            copy_to_wd(info.localsrc, wd)

        else:
            logger.warning("Source image does not exist: %s", info.srcfp)
            err = 1


    #### Get Image Stats
    if not err == 1:
        info, rc = GetImageStats(args, info, target_extent_geom)
        if rc == 1:
            err = 1
            logger.error("Error in stats calculation")

    #### Check that DEM overlaps image
    if not err == 1:
        if args.dem and not args.skip_dem_overlap_check:
            overlap = overlap_check(info.geometry_wkt, info.spatial_ref, args.dem)
            if overlap is False:
                err = 1

    if not os.path.isfile(info.dstfp):
        #### Warp Image
        if not err == 1 and not os.path.isfile(info.warpfile):
            rc = WarpImage(args, info, gdal_thread_count=gdal_thread_count)
            if rc == 1:
                err = 1
                logger.error("Error in image warping")

        #### Calculate Output File
        if not err == 1 and os.path.isfile(info.warpfile):
            rc = calcStats(args, info)
            if rc == 1:
                err = 1
                logger.error("Error in image calculation")

    ####  Write Output Metadata
    if not err == 1:
        rc = WriteOutputMetadata(args, info)
        if rc == 1:
            err = 1
            logger.error("Error in writing metadata file")

    #### Copy image to final location if working dir is used
    if args.wd is not None:
        if not err == 1:
            logger.info("Copying to destination directory")
            for fpi in glob.glob("{}.*".format(os.path.splitext(info.localdst)[0])):
                fpo = os.path.join(info.dstdir, os.path.basename(fpi))
                if not os.path.isfile(fpo):
                    shutil.copy2(fpi, fpo)
        if not args.save_temps:
            utils.delete_temp_files([info.localdst])

    #### Check If Done, Delete Temp Files
    done = os.path.isfile(info.dstfp)
    if done is False:
        err = 1
        logger.error("Final image not present")

    if err == 1:
        logger.error("Processing failed: %s", info.srcfn)
        if not args.save_temps:
            if args.wd:
                utils.delete_temp_files([info.dstfp, info.rawvrt, info.warpfile, info.vrtfile, info.localsrc])
            else:
                utils.delete_temp_files([info.dstfp, info.rawvrt, info.warpfile, info.vrtfile])

    elif not args.save_temps:
        if args.wd:
            utils.delete_temp_files([info.rawvrt, info.warpfile, info.vrtfile, info.localsrc])
        else:
            utils.delete_temp_files([info.rawvrt, info.warpfile, info.vrtfile])
    # Rename temp files if --save-temps
    elif args.save_temps:
        os.rename(info.rawvrt, info.rawvrt + ".save")
        os.rename(info.vrtfile, info.vrtfile + ".save")
        os.rename(info.warpfile, info.warpfile + ".save")

    #### Calculate Total Time
    endtime = datetime.today()
    td = (endtime-starttime)
    logger.info("Total Processing Time: %s\n", td)

    return err


def stackIkBands(dstfp, members):

    rc = 0

    band_dict = {1: gdalconst.GCI_BlueBand,
                 2: gdalconst.GCI_GreenBand,
                 3: gdalconst.GCI_RedBand,
                 4: gdalconst.GCI_Undefined}
    remove_keys = ("NITF_FHDR", "NITF_IREP", "NITF_OSTAID", "NITF_IC", "NITF_ICORDS", "NITF_IGEOLO") #"NITF_FHDR"
    meta_dict = {"NITF_IREP": "MULTI"}

    srcfp = members[0]
    srcdir, srcfn = os.path.split(srcfp)
    dstdir, dstfn = os.path.split(dstfp)
    vrt = os.path.splitext(dstfp)[0] + "_merge.vrt"

    #### Gather metadata from original blue image and save as strings for merge command
    logger.info("Stacking IKONOS MSI bands")
    src_ds = gdal.Open(srcfp, gdalconst.GA_ReadOnly)
    if src_ds is not None:

        #### Get basic metadata
        m = src_ds.GetMetadata()
        if src_ds.GetGCPCount() > 1:
            proj = src_ds.GetGCPProjection()
        else:
            proj = src_ds.GetProjectionRef()
        s_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference(proj))
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
            if '"' not in m[k]:
                m_list.append('-co "{}={}"'.format(k.replace("NITF_", ""), m[k]))
        for k in meta_dict.keys():
            if '"' not in meta_dict[k]:
                m_list.append('-co "{}={}"'.format(k.replace("NITF_", ""), meta_dict[k]))

        #### Get the TRE metadata
        tres = src_ds.GetMetadata("TRE")
        #### Make the dictionary into a list
        tre_list = []
        for k in tres.keys():
            if '"' not in tres[k]:
                tre_list.append('-co "TRE={}={}"'.format(k, src_ds.GetMetadataItem(k, "TRE")))

        #### Close the source dataset
        src_ds = None

        #print("Merging bands")
        cmd = 'gdalbuildvrt -separate "{}" "{}"'.format(vrt, '" "'.join(members))

        (err, so, se) = taskhandler.exec_cmd(cmd)
        if err == 1:
            rc = 1

        cmd = 'gdal_translate -a_srs "{}" -of NITF -co "IC=NC" {} {} "{}" "{}"'.format(s_srs_proj4,
                                                                                       " ".join(m_list),
                                                                                       " ".join(tre_list),
                                                                                       vrt,
                                                                                       dstfp)

        (err, so, se) = taskhandler.exec_cmd(cmd)
        if err == 1:
            rc = 1

        #print("Writing metadata to output")
        dst_ds = gdal.Open(dstfp, gdalconst.GA_ReadOnly)
        if dst_ds is not None:
            #### check that ds has correct number of bands
            if not dst_ds.RasterCount == len(band_dict):
                logger.error("Missing MSI band in stacked dataset.  Band count: %i, Required band count: %i",
                             dst_ds.RasterCount, len(band_dict))
                rc = 1

            else:
                #### Set Color Interpretation
                for key in band_dict.keys():
                    rb = dst_ds.GetRasterBand(key)
                    rb.SetColorInterpretation(band_dict[key])

        #### Close Image
        dst_ds = None

        #### also copy blue and rgb aux files
        for fpi in glob.glob(os.path.join(srcdir, "{}.*".format(os.path.splitext(srcfn)[0]))):
            fpo = os.path.join(dstdir, os.path.basename(fpi).replace("blu", "msi"))
            if not os.path.isfile(fpo) and not os.path.basename(fpi) == srcfn:
                shutil.copy2(fpi, fpo)
        for fpi in glob.glob(os.path.join(srcdir, "{}.*".format(os.path.splitext(srcfn)[0].replace("blu", "rgb")))):
            fpo = os.path.join(dstdir, os.path.basename(fpi).replace("rgb", "msi"))
            if not os.path.isfile(fpo) and not os.path.basename(fpi) == srcfn:
                shutil.copy2(fpi, fpo)
        for fpi in glob.glob(os.path.join(srcdir, "{}.txt".format(os.path.splitext(srcfn)[0].replace("blu", "pan")))):
            fpo = os.path.join(dstdir, os.path.basename(fpi).replace("pan", "msi"))
            if not os.path.isfile(fpo):
                shutil.copy2(fpi, fpo)

    else:
        rc = 1
    try:
        os.remove(vrt)
    except Exception as e:
        logger.error(utils.capture_error_trace())
        logger.warning("Cannot remove file: %s, %s", vrt, e)
    return rc


def GetEPSGFromLatLon(lat, lon, mode='auto'):
    """
    Get the EPSG code of the UTM or polar stereographic
    projected coordinate system closest to the provided
    point latitude and longitude.

    Parameters
    ----------
    lat : float [-90, 90]
        Point latitude in decimal degrees.
    lon : float [-180, 180]
        Point longitude in decimal degrees.
    mode : str
        If 'utm', use closest UTM zone.
        If 'auto', use closest UTM zone when
        `lat` is in the range [-60, 60], otherwise
        use the proper polar stereographic projection.

    Returns
    -------
    epsg_code : int
        EPSG code of selected projected coordinate system.
    """
    mode_choices = ['utm', 'auto']

    if -90 <= lat <= 90:
        pass
    else:
        raise utils.InvalidArgumentError(
            "`lat` must be in the range [-90, 90] but was {}".format(lat)
        )
    if -180 <= lon <= 180:
        pass
    else:
        raise utils.InvalidArgumentError(
            "`lon` must be in the range [-180, 180] but was {}".format(lon)
        )
    if mode not in mode_choices:
        raise utils.InvalidArgumentError(
            "`mode` must be one of {} but was '{}'".format(mode_choices, mode)
        )

    epsg_code = None

    if mode == 'utm' or (mode == 'auto' and (-60 <= lat <= 60)):
        utm_zone_num = max(1, math.ceil((lon - (-180)) / 6))
        if lat >= 0:
            epsg_code = 32600 + utm_zone_num
        else:
            epsg_code = 32700 + utm_zone_num

    elif mode == 'auto':
        if lat > 60:
            epsg_code = 3413
        elif lat < 60:
            epsg_code = 3031

    assert type(epsg_code) is int
    return epsg_code



def calcStats(args, info):

    logger.info("Calculating image with stats")
    rc = 0

    #### Get Well-known Text String of Projection from EPSG Code
    p = info.spatial_ref.srs
    prj = p.ExportToWkt()

    imax = 2047.0

    if info.stretch == 'ns':
        if args.outtype == "Byte":
            omax = 255.0
        elif args.outtype == "UInt16":
            omax = 2047.0
        elif args.outtype == "Float32":
            omax = 2047.0
    elif info.stretch == 'mr':
        if args.outtype == "Byte":
            omax = 255.0
        elif args.outtype == "UInt16":
            omax = 2047.0
        elif args.outtype == "Float32":
            omax = 1.0
    elif info.stretch == 'rf':
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

    wds = gdal.Open(info.warpfile, gdalconst.GA_ReadOnly)
    if wds is not None:

        xsize = wds.RasterXSize
        ysize = wds.RasterYSize

        vds = VRTdriver.CreateCopy(info.vrtfile, wds, 0)
        if vds is not None:

            for band in range(1,vds.RasterCount+1):
                if info.stretch == "ns":
                    LUT = "0:0,{}:{}".format(imax,omax)
                else:
                    calfact,offset = CFlist[band-1]
                    if info.stretch == "rf":
                        LUT = "0:{},{}:{}".format(offset*omax, imax, (imax*calfact+offset)*omax)
                    elif info.stretch == "rd":
                        LUT = "0:{},{}:{}".format(offset, imax, imax*calfact+offset)
                    elif info.stretch == "mr":
                        # modified reflectance is rf with a non-linear curve applied according to the following histgram points
                        iLUT = [0, 0.125, 0.25, 0.375, 0.625, 1]
                        oLUT = [0, 0.375, 0.625, 0.75, 0.875, 1]
                        lLUT = map(lambda x: "{}:{}".format(
                            (iLUT[x]-offset)/calfact, ## find original DN for each 0-1 iLUT step by applying reverse reflectance transformation
                            omax*oLUT[x] ## output value for each 0-1 oLUT step multiplied by omax
                        ), range(len(iLUT)))
                        LUT = ",".join(lLUT)
                    #logger.debug(LUT)

                if info.stretch != "ns":
                    logger.debug("Band Calibration Factors: %i %f %f", band, CFlist[band - 1][0], CFlist[band - 1][1])
                logger.debug("Band stretch parameters: %i %s", band, LUT)

                ComplexSourceXML = ('<ComplexSource>'
                                    '   <SourceFilename relativeToVRT="0">{0}</SourceFilename>'
                                    '   <SourceBand>{1}</SourceBand>'
                                    '   <ScaleOffset>0</ScaleOffset>'
                                    '   <ScaleRatio>1</ScaleRatio>'
                                    '   <LUT>{2}</LUT>'
                                    '   <SrcRect xOff="0" yOff="0" xSize="{3}" ySize="{4}"/>'
                                    '   <DstRect xOff="0" yOff="0" xSize="{3}" ySize="{4}"/>'
                                    '</ComplexSource>)'.format(info.warpfile, band, LUT, xsize, ysize))

                vds.GetRasterBand(band).SetMetadataItem("source_0", ComplexSourceXML, "new_vrt_sources")
                vds.GetRasterBand(band).SetNoDataValue(0)
                if vds.GetRasterBand(band).GetColorInterpretation() == gdalconst.GCI_AlphaBand:
                    vds.GetRasterBand(band).SetColorInterpretation(gdalconst.GCI_Undefined)
        else:
            logger.error("Cannot create virtual dataset: %s", info.vrtfile)

    else:
        logger.error("Cannot open dataset: %s", info.warpfile)

    vds = None
    wds = None

    if args.format == 'GTiff':
        if args.gtiff_compression == 'lzw':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=IF_SAFER" '
        elif args.gtiff_compression == 'jpeg95':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "compress=jpeg" -co "jpeg_quality=95" -co ' \
                 '"BIGTIFF=IF_SAFER" '

    elif args.format == 'HFA':
        co = '-co "COMPRESSED=YES" -co "STATISTICS=YES" '

    elif args.format == 'JP2OpenJPEG':   #### add rgb constraint if openjpeg (3 bands only, also test if 16 bit possible)?
        co = '-co "QUALITY=25" '

    elif args.format == 'JPEG':
        co = ''

    else:
        co = ''

    pf = platform.platform()
    if pf.startswith("Linux"):
        config_options = '--config GDAL_CACHEMAX 2048'
    else:
        config_options = ''

    base_cmd = 'gdal_translate -stats'

    cmd = ('{} {} -ot {} -a_srs "{}" {}{}-of {} "{}" "{}"'.format(
        base_cmd,
        config_options,
        args.outtype,
        info.spatial_ref.proj4,
        info.rgb_bands,
        co,
        args.format,
        info.vrtfile,
        info.localdst
        ))

    (err, so, se) = taskhandler.exec_cmd(cmd)
    if err == 1:
        rc = 1

    #### Calculate Pyramids
    if not args.no_pyramids:
        if args.format in ["GTiff"]:
            if os.path.isfile(info.localdst):
                cmd = ('gdaladdo -r {} "{}" 2 4 8 16'.format(args.pyramid_type, info.localdst))
                (err, so, se) = taskhandler.exec_cmd(cmd)
                if err == 1:
                    rc = 1

    #### Write .prj File
    if os.path.isfile(info.localdst):
        txtpath = os.path.splitext(info.localdst)[0] + '.prj'
        txt = open(txtpath, 'w')
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

    if info.vendor == 'GeoEye' and info.sat == 'IK01' and "_msi_" in info.srcfn and not os.path.isfile(info.localsrc):
        src_image_name = info.srcfn.replace("_msi_", "_blu_")
        src_image = os.path.join(info.srcdir, src_image_name)
        info.bands = 4
    else:
        src_image = info.localsrc
        info.bands = None

    ds = gdal.Open(src_image, gdalconst.GA_ReadOnly)
    if ds is not None:

        ####  Get extent from GCPs
        num_gcps = ds.GetGCPCount()
        if info.bands is None:
            info.bands = ds.RasterCount

        if num_gcps == 4:
            gcps = ds.GetGCPs()
            proj = ds.GetGCPProjection()

            gcp_dict = {}

            id_dict = {"UpperLeft": 1,
                       "1": 1,
                       "UpperRight": 2,
                       "2": 2,
                       "LowerLeft": 4,
                       "4": 4,
                       "LowerRight": 3,
                       "3": 3}

            for gcp in gcps:
                gcp_dict[id_dict[gcp.Id]] = [float(gcp.GCPPixel), float(gcp.GCPLine), float(gcp.GCPX), float(gcp.GCPY),
                                             float(gcp.GCPZ)]

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
            lrx = gtf[0] + xsize * gtf[1] + ysize * gtf[2]
            lry = gtf[3] + xsize * gtf[4] + ysize * gtf[5]

        ds = None

        ####  Create geometry objects
        ul = "POINT ( {0:.12f} {1:.12f} )".format(ulx, uly)
        ur = "POINT ( {0:.12f} {1:.12f} )".format(urx, ury)
        ll = "POINT ( {0:.12f} {1:.12f} )".format(llx, lly)
        lr = "POINT ( {0:.12f} {1:.12f} )".format(lrx, lry)
        poly_wkt = 'POLYGON (( {0:.12f} {1:.12f}, {2:.12f} {3:.12f}, ' \
                   '{4:.12f} {5:.12f}, {6:.12f} {7:.12f}, ' \
                   '{8:.12f} {9:.12f} ))'.format(ulx, uly, urx, ury, lrx, lry, llx, lly, ulx, uly)

        ul_geom = ogr.CreateGeometryFromWkt(ul)
        ur_geom = ogr.CreateGeometryFromWkt(ur)
        ll_geom = ogr.CreateGeometryFromWkt(ll)
        lr_geom = ogr.CreateGeometryFromWkt(lr)
        extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)

        g_srs = srs_wgs84

        #### Create source srs objects
        s_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference(proj))
        sg_ct = osr.CoordinateTransformation(s_srs, g_srs)

        #### Transform geometries to geographic
        if not s_srs.IsSame(g_srs):
            ul_geom.Transform(sg_ct)
            ur_geom.Transform(sg_ct)
            ll_geom.Transform(sg_ct)
            lr_geom.Transform(sg_ct)
            extent_geom.Transform(sg_ct)
        logger.info("Geographic extent: %s", str(extent_geom))

        #### Get geographic Envelope
        minlon, maxlon, minlat, maxlat = extent_geom.GetEnvelope()

        ## Determine output image projection if applicable
        if type(args.epsg) is str:
            cent_lat = (minlat + maxlat) / 2
            cent_lon = (minlon + maxlon) / 2
            info.epsg = GetEPSGFromLatLon(cent_lat, cent_lon, mode=args.epsg)
            logger.info("Automatically selected output projection EPSG code: %d", info.epsg)
            try:
                spatial_ref = utils.SpatialRef(info.epsg)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error("Invalid EPSG code: %i", info.epsg)
                rc = 1
            else:
                info.spatial_ref = spatial_ref

        #### Create target srs objects
        t_srs = info.spatial_ref.srs
        gt_ct = osr.CoordinateTransformation(g_srs, t_srs)
        tg_ct = osr.CoordinateTransformation(t_srs, g_srs)

        #### Transform geoms to target srs
        if not g_srs.IsSame(t_srs):
            ul_geom.Transform(gt_ct)
            ur_geom.Transform(gt_ct)
            ll_geom.Transform(gt_ct)
            lr_geom.Transform(gt_ct)
            extent_geom.Transform(gt_ct)
        logger.info("Projected extent: %s", str(extent_geom))

        ## test user provided extent and ues if appropriate
        if target_extent_geom:
            if not extent_geom.Intersects(target_extent_geom):
                rc = 1
            else:
                logger.info("Using user-provided extent: %s", str(target_extent_geom))
                extent_geom = target_extent_geom

        if rc != 1:
            info.extent_geom = extent_geom
            info.geometry_wkt = extent_geom.ExportToWkt()
            #### Get centroid and back project to geographic coords (this is neccesary for images that cross 180)
            centroid = extent_geom.Centroid()
            centroid.Transform(tg_ct)

            #### Get projected Envelope
            minx, maxx, miny, maxy = extent_geom.GetEnvelope()

            #print(lons)
            logger.info("Centroid: %s", str(centroid))

            if maxlon - minlon > 180:

                if centroid.GetX() < 0:
                    info.centerlong = '--config CENTER_LONG -180 '
                else:
                    info.centerlong = '--config CENTER_LONG 180 '

            info.extent = "-te {0:.12f} {1:.12f} {2:.12f} {3:.12f} ".format(minx, miny, maxx, maxy)

            rasterxsize_m = abs(math.sqrt((ul_geom.GetX() - ur_geom.GetX())**2 + (ul_geom.GetY() - ur_geom.GetY())**2))
            rasterysize_m = abs(math.sqrt((ul_geom.GetX() - ll_geom.GetX())**2 + (ul_geom.GetY() - ll_geom.GetY())**2))

            resx = abs(math.sqrt((ul_geom.GetX() - ur_geom.GetX())**2 + (ul_geom.GetY() - ur_geom.GetY())**2) / xsize)
            resy = abs(math.sqrt((ul_geom.GetX() - ll_geom.GetX())**2 + (ul_geom.GetY() - ll_geom.GetY())**2) / ysize)

            ####  Make a string for Pixel Size Specification
            if args.resolution is not None:
                info.res = "-tr {} {} ".format(args.resolution, args.resolution)
            else:
                info.res = "-tr {0:.12f} {1:.12f} ".format(resx, resy)
            if args.tap:
                info.tap = "-tap "
            else:
                info.tap = ""
            logger.info("Original image size: %f x %f, res: %.12f x %.12f", rasterxsize_m, rasterysize_m, resx, resy)

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
                    logger.error("Cannot get rgb bands from a %i band image", info.bands)
                    rc = 1

            if args.bgrn is True:
                if info.bands == 1:
                    pass
                elif info.bands == 4:
                    pass
                elif info.bands == 8:
                    info.rgb_bands = "-b 2 -b 3 -b 5 -b 7 "
                else:
                    logger.error("Cannot get bgrn bands from a %i band image", info.bands)
                    rc = 1

            info.stretch = args.stretch
            if args.stretch == 'au':
                if ((maxlat + minlat) / 2) <= -60:
                    info.stretch = 'rf'
                else:
                    info.stretch = 'mr'
                logger.info("Automatically selected stretch: %s", info.stretch)
    else:
        logger.error("Cannot open dataset: %s", info.localsrc)
        rc = 1

    return info, rc


def GetImageGeometryInfo(src_image, spatial_ref, args, return_type='extent_geom'):
    return_type_choices = ['extent_geom', 'epsg_code']
    if return_type not in return_type_choices:
        raise utils.InvalidArgumentError(
            "`return_type` must be one of {} but was '{}'".format(
                return_type_choices, return_type
            )
        )

    ds = gdal.Open(src_image, gdalconst.GA_ReadOnly)
    if ds is not None:

        ####  Get extent from GCPs
        num_gcps = ds.GetGCPCount()

        if num_gcps == 4:
            gcps = ds.GetGCPs()
            proj = ds.GetGCPProjection()

            gcp_dict = {}
            id_dict = {"UpperLeft": 1,
                       "1": 1,
                       "UpperRight": 2,
                       "2": 2,
                       "LowerLeft": 4,
                       "4": 4,
                       "LowerRight": 3,
                       "3": 3}

            for gcp in gcps:
                gcp_dict[id_dict[gcp.Id]] = [float(gcp.GCPPixel), float(gcp.GCPLine), float(gcp.GCPX),
                                             float(gcp.GCPY), float(gcp.GCPZ)]
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

        ds = None

        ####  Create geometry objects
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(ulx, uly)
        ring.AddPoint(urx, ury)
        ring.AddPoint(lrx, lry)
        ring.AddPoint(llx, lly)
        ring.AddPoint(ulx, uly)

        extent_geom = ogr.Geometry(ogr.wkbPolygon)
        extent_geom.AddGeometry(ring)

        g_srs = srs_wgs84

        #### Create source srs objects
        s_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference(proj))
        sg_ct = osr.CoordinateTransformation(s_srs, g_srs)

        #### Transform geometries to geographic
        if not s_srs.IsSame(g_srs):
            extent_geom.Transform(sg_ct)
        # logger.info("Geographic extent: %s", str(extent_geom))

        #### Get geographic Envelope
        minlon, maxlon, minlat, maxlat = extent_geom.GetEnvelope()

        ## Determine output image projection if applicable
        if type(args.epsg) is str:
            cent_lat = (minlat + maxlat) / 2
            cent_lon = (minlon + maxlon) / 2
            img_epsg = GetEPSGFromLatLon(cent_lat, cent_lon, mode=args.epsg)
            try:
                spatial_ref = utils.SpatialRef(img_epsg)
            except RuntimeError as e:
                logger.error(utils.capture_error_trace())
                logger.error("Invalid EPSG code: %i", img_epsg)
                return None
        else:
            img_epsg = args.epsg

        if return_type == 'epsg_code':
            return img_epsg

        #### Create target srs objects
        t_srs = spatial_ref.srs
        gt_ct = osr.CoordinateTransformation(g_srs, t_srs)

        #### Transform geoms to target srs
        if not g_srs.IsSame(t_srs):
            extent_geom.Transform(gt_ct)
        # logger.info("Projected extent: %s", str(extent_geom))

        return extent_geom

    else:
        logger.error("Cannot open dataset: %s", src_image)
        return None


def GetDGMetadataPath(srcfp):
    """
    Returns the filepath of the XML, if it can be found. Returns
    None if no valid filepath could be found.
    """

    filename = os.path.basename(srcfp)

    if os.path.isfile(os.path.splitext(srcfp)[0] + '.xml'):
        metapath = os.path.splitext(srcfp)[0] + '.xml'
    elif os.path.isfile(os.path.splitext(srcfp)[0] + '.XML'):
        metapath = os.path.splitext(srcfp)[0] + '.XML'
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
                        metapath = os.path.join(wd, os.path.splitext(filename)[0] + os.path.splitext(t)[1].lower())
                        fpfh = open(metapath, "w")
                        tfstr = tf.read()
                        fpfh.write(tfstr)
                        fpfh.close()
                        tf.close()
            except Exception:
                logger.error(utils.capture_error_trace())
                logger.error("Cannot open Tar file: %s", tarpath)

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
    metapath = os.path.splitext(srcfp)[0] + '.txt'

    if not os.path.isfile(metapath):
        metapath = os.path.splitext(srcfp)[0] + '_metadata.txt'

    if not os.path.isfile(metapath):
        for b in ikMsiBands:
            mp = metapath.replace(b, 'rgb')
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
    metapath = os.path.splitext(srcfp)[0] + '.txt'
    if not os.path.isfile(metapath):
        metapath = os.path.splitext(srcfp)[0] + '.pvl'
    if os.path.isfile(metapath):
        return metapath
    else:
        return None


def WriteOutputMetadata(args, info):

    ####  Ortho metadata name
    omd = os.path.splitext(info.localdst)[0] + ".xml"

    til = None

    ####  Get xml/pvl metadata
    ####  If DG
    if info.vendor == 'DigitalGlobe':
        metapath = info.metapath

        try:
            metad = ET.parse(metapath)
        except ET.ParseError:
            logger.error("Invalid xml formatting in metadata file: %s", metapath)
            return 1
        else:
            imd = metad.find("IMD")
            til = metad.find("TIL")

    ####  If GE
    elif info.vendor == 'GeoEye' and info.sat == "GE01":

        metad = utils.getGEMetadataAsXml(info.metapath)
        imd = ET.Element("IMD")
        include_tags = ["sensorInfo", "inputImageInfo", "correctionParams", "bandSpecificInformation"]

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
    dMD["VERSION"] = "imagery_utils v{}".format(utils.package_version)
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
    dMD["EPSG_CODE"] = str(info.epsg)

    pgcmd = ET.Element("PGC_IMD")
    for tag in dMD:
        child = ET.SubElement(pgcmd, tag)
        child.text = dMD[tag]

    ####  Write output

    root = ET.Element("IMD")

    root.append(pgcmd)

    ref = ET.SubElement(root, "SOURCE_IMD")
    child = ET.SubElement(ref, "SOURCE_IMAGE")
    child.text = os.path.basename(info.localsrc)
    child = ET.SubElement(ref, "VENDOR")
    child.text = info.vendor

    if imd is not None:
        ref.append(imd)
    if til is not None:
        ref.append(til)

    #ET.ElementTree(root).write(omd,xml_declaration=True)
    xmlstring = prettify(root)
    fh = open(omd, 'w')
    fh.write(xmlstring)

    return 0


def prettify(root):
    """Return a pretty-printed XML string for the Element.
    """
    for elem in root.iter('*'):
        if elem.text is not None:
            elem.text = elem.text.strip()
        if elem.tail is not None:
            elem.tail = elem.tail.strip()

    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)

    return reparsed.toprettyxml(indent="\t")


def WarpImage(args, info, gdal_thread_count=1):

    rc = 0

    pf = platform.platform()
    if pf.startswith("Linux"):
        config_options = '-wm 2000 --config GDAL_CACHEMAX 2048 --config GDAL_NUM_THREADS {0} -wo NUM_THREADS={0}'.\
            format(gdal_thread_count)
    else:
        config_options = '--config GDAL_NUM_THREADS {0} -wo NUM_THREADS={0}'.format(gdal_thread_count)
    if type(gdal_thread_count) == str:
        if gdal_thread_count == "ALL_CPUS":
            config_options += ' -multi'
    elif type(gdal_thread_count) == int:
        if gdal_thread_count > 1:
            config_options += ' -multi'

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
                    logger.error("Cannot extract rpc's for Ikonos. Image cannot be terrain corrected with a DEM or "
                                 "avg elevation.")
                    rc = 1

                if rpb_p:
                    if not os.path.isfile(rpb_p):
                        err = ExtractRPB(info.localsrc, rpb_p)
                        if err == 1:
                            rc = 1
                    if not os.path.isfile(rpb_p):
                        logger.error("No RPC information found. Image cannot be terrain corrected with a DEM or avg "
                                     "elevation.")
                        rc = 1

        #### convert to VRT and modify 4th band
        cmd = 'gdal_translate -of VRT "{0}" "{1}"'.format(info.localsrc, info.rawvrt)
        (err, so, se) = taskhandler.exec_cmd(cmd)
        if err == 1:
            rc = 1

        if os.path.isfile(info.rawvrt) and info.bands > 3:
            vds = gdal.Open(info.rawvrt, gdalconst.GA_Update)
            if vds.GetRasterBand(4).GetColorInterpretation() == 6:
                vds.GetRasterBand(4).SetColorInterpretation(gdalconst.GCI_Undefined)
            vds = None

        nodata_list = ["0"] * info.bands

        if not args.skip_warp:
            if rc != 1:
                ####  Set RPC_DEM or RPC_HEIGHT transformation option
                if args.dem is not None:
                    logger.info('DEM: %s', os.path.basename(args.dem))
                    to = "RPC_DEM={}".format(args.dem)

                elif args.ortho_height is not None:
                    logger.info("Elevation: %f meters", args.ortho_height)
                    to = "RPC_HEIGHT={}".format(args.ortho_height)

                else:
                    #### Get Constant Elevation From XML
                    h = get_rpc_height(info)
                    logger.info("Average elevation: %f meters", h)
                    to = "RPC_HEIGHT={}".format(h)
                    ds = None


                #### GDALWARP Command
                cmd = 'gdalwarp {} -srcnodata "{}" -of GTiff -ot UInt16 {}{}{}{}-co "TILED=YES" -co "BIGTIFF=IF_SAFER" ' \
                      '-t_srs "{}" -r {} -et 0.01 -rpc -to "{}" "{}" "{}"'.format(
                    config_options,
                    " ".join(nodata_list),
                    info.centerlong,
                    info.extent,
                    info.res,
                    info.tap,
                    info.spatial_ref.proj4,
                    args.resample,
                    to,
                    info.rawvrt,
                    info.warpfile
                )

                (err, so, se) = taskhandler.exec_cmd(cmd)
                #print(err)
                if err == 1:
                    rc = 1

        else:
            #### GDALWARP Command
            cmd = 'gdalwarp {} -srcnodata "{}" -of GTiff -ot UInt16 {}{}-co "TILED=YES" -co "BIGTIFF=IF_SAFER" -t_srs ' \
                  '"{}" -r {} "{}" "{}"'.format(
                config_options,
                " ".join(nodata_list),
                info.res,
                info.tap,
                info.spatial_ref.proj4,
                args.resample,
                info.rawvrt,
                info.warpfile
            )

            (err, so, se) = taskhandler.exec_cmd(cmd)
            #print(err)
            if err == 1:
                rc = 1

        return rc


def get_rpc_height(info):
    ds = gdal.Open(info.localsrc, gdalconst.GA_ReadOnly)
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
        calibDict = getDGXmlData(xmlpath, info.stretch)
        bandList = DGbandList

    elif info.vendor == "GeoEye" and info.sat == "GE01":

        metapath = info.metapath
        calibDict = GetGEcalibDict(metapath, info.stretch)
        if info.bands == 1:
            bandList = [5]
        elif info.bands == 4:
            bandList = range(1, 5, 1)

    elif info.vendor == "GeoEye" and info.sat == "IK01":
        metapath = info.metapath
        calibDict = GetIKcalibDict(metapath, info.stretch)
        if info.bands == 1:
            bandList = [4]
        elif info.bands == 4:
            bandList = range(0, 4, 1)
        elif info.bands == 3:
            bandList = range(0, 3, 1)

    else:
        logger.warning("Vendor or sensor not recognized: %s, %s", info.vendor, info.sat)

    #logger.info("Calibration factors: %s", calibDict)
    if len(calibDict) > 0:

        for band in bandList:
            if band in calibDict:
                CFlist.append(calibDict[band])

    logger.debug("Calibration factor list: %s", CFlist)
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

            dem_geometry_wkt = 'POLYGON (( {} {}, {} {}, {} {}, {} {}, {} {} ))'.format(minx, miny, minx, maxy, maxx,
                                                                                        maxy, maxx, miny, minx, miny)
            demGeometry = ogr.CreateGeometryFromWkt(dem_geometry_wkt)
            logger.info("DEM extent: %s", str(demGeometry))
            demSpatialReference = utils.osr_srs_preserve_axis_order(osr.SpatialReference(demProjection))

            coordinateTransformer = osr.CoordinateTransformation(imageSpatialReference, demSpatialReference)
            if not imageSpatialReference.IsSame(demSpatialReference):
                #logger.info("Image Spatial Refernce: %s", imageSpatialReference)
                #logger.info("DEM Spatial ReferenceL %s", emSpatialReference)
                #logger.info("Image Geometry before transformation: %s", imageGeometry)
                logger.info("Transforming image geometry to dem spatial reference")
                imageGeometry.Transform(coordinateTransformer)
                #logger.info("Image Geometry after transformation: %s", imageGeometry)

            dem = None
            overlap = imageGeometry.Within(demGeometry)

            if overlap is False:
                logger.error("Image is not contained within DEM extent")

        else:
            logger.error("DEM has no spatial reference information: %s", demPath)
            overlap = False

    else:
        logger.error("Cannot open DEM to determine extent: %s", demPath)
        overlap = False

    return overlap


def ExtractRPB(item, rpb_p):
    rc = 0
    tar_p = os.path.splitext(item)[0] + ".tar"
    logger.info(tar_p)
    if os.path.isfile(tar_p):
        fp_extracted = list()
        try:
            tar = tarfile.open(tar_p, 'r')
            tarlist = tar.getnames()
            for t in tarlist:
                if '.rpb' in t.lower() or '_rpc' in t.lower(): #or '.til' in t.lower():
                    tf = tar.extractfile(t)
                    fp = os.path.splitext(rpb_p)[0] + os.path.splitext(t)[1]
                    fp_extracted.append(fp)
                    fpfh = open(fp, "w")
                    tfstr = tf.read()
                    if type(tfstr) is bytes and type(tfstr) is not str:
                        tfstr = tfstr.decode('utf-8')
                    #print(repr(tfstr))
                    fpfh.write(tfstr)
                    fpfh.close()
                    tf.close()
                    # status = 0
        except Exception:
            logger.error(utils.capture_error_trace())
            logger.error("Caught Exception when working on Tar file: %s", tar_p)
            fp_extracted = [fp for fp in fp_extracted if os.path.isfile(fp)]
            if len(fp_extracted) > 0:
                logger.error("Removing files extracted from Tar file:\n  {}".format('\n  '.join(fp_extracted)))
                for fp in fp_extracted:
                    os.remove(fp)
            rc = 1
    else:
        logger.info("Tar file does not exist: %s", tar_p)
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
    ut = hr + (minute / 60.) + (sec / 3600.)
    #print(ut)

    if month <= 2:
        year = year - 1
        month = month + 12

    a = int(year / 100)
    b = 2 - a + int(a / 4)
    jd = int(365.25 * (year + 4716)) + int(30.6001 * (month + 1)) + day + (ut / 24) + b - 1524.5
    #print(jd)

    g = 357.529 + 0.98560028 * (jd - 2451545.0)
    d = 1.00014 - 0.01671 * math.cos(math.radians(g)) - 0.00014 * math.cos(math.radians(2 * g))
    #print(d)

    return d


def getDGXmlData(xmlpath, stretch):
    calibDict = {}
    abscalfact_dict = {}
    try:
        xmldoc = minidom.parse(xmlpath)
    except Exception:
        logger.error(utils.capture_error_trace())
        logger.error("Cannot parse metadata file: %s", xmlpath)
        return None
    else:

        if len(xmldoc.getElementsByTagName('IMD')) >= 1:

            nodeIMD = xmldoc.getElementsByTagName('IMD')[0]

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

            sunAngle = 90.0 - sunEl
            des = calcEarthSunDist(datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ"))

            # get BAND tags
            for band in DGbandList:
                nodeBAND = nodeIMD.getElementsByTagName(band)
                #print(nodeBAND)
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

                    abscalfact_dict[band] = (abscal, effbandw)

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
                satband = sat + '_' + band
                if satband not in EsunDict:
                    logger.warning("Cannot find sensor and band in Esun lookup table: %s.  Try using --stretch "
                                   "ns.", satband)
                    return None
                else:
                    Esun = EsunDict[satband]
                    gain = GainDict[satband]
                    bias = BiasDict[satband]

                abscal, effbandw = abscalfact_dict[band]

                rad_fact = units_factor * gain * abscal / effbandw
                refl_fact = units_factor * (gain * abscal * des ** 2 * math.pi) / \
                            (Esun * math.cos(math.radians(sunAngle)) * effbandw)
                refl_offset = units_factor * (bias * des ** 2 * math.pi) / (Esun * math.cos(math.radians(sunAngle)))

                logger.debug("%s: \n\tabsCalFactor %f\n\teffectiveBandwidth %f\n\tEarth-Sun distance %f"
                            "\n\tEsun %f\n\tSun angle %f\n\tSun elev %f\n\tGain %f\n\tBias %f"
                            "\n\tUnits factor %f\n\tReflectance correction %f\n\tReflectance offset %f"
                            "\n\tRadiance correction %f\n\tRadiance offset %f", satband, abscal, effbandw,
                            des, Esun, sunAngle, sunEl, gain, bias, units_factor, refl_fact, refl_offset,
                            rad_fact, bias)

                if stretch == "rd":
                    calibDict[band] = (rad_fact, bias)
                else:
                    calibDict[band] = (refl_fact, refl_offset)

    # return correction factor and offset
    return calibDict


def GetIKcalibDict(metafile, stretch):
    fp_mode = "renamed"
    metadict = getIKMetadata(fp_mode, metafile)
    #print(metadict)

    calibDict = {}
    EsunDict = [1930.9, 1854.8, 1556.5, 1156.9, 1375.8] # B,G,R,N,Pan(TDI13)
    bwList = [71.3, 88.6, 65.8, 95.4, 403] # B,G,R,N,Pan(TDI13)
    calCoefs1 = [633, 649, 840, 746, 161] # B,G,R,N,Pan(TDI13) - Pre 2/22/01
    calCoefs2 = [728, 727, 949, 843, 161] # B,G,R,N,Pan(TDI13) = Post 2/22/01


    for band in range(0, 5, 1):
        sunElStr = metadict["Sun_Angle_Elevation"]
        sunAngle = float(sunElStr.strip(" degrees"))
        theta = 90.0 - sunAngle
        datestr = metadict["Acquisition_Date_Time"] # 2011-12-09 18:43 GMT
        d = datetime.strptime(datestr, "%Y-%m-%d %H:%M GMT")
        des = calcEarthSunDist(d)

        breakdate = datetime(2001, 2, 22)
        if d < breakdate:
            calCoef = calCoefs1[band]
        else:
            calCoef = calCoefs2[band]

        bw = bwList[band]
        Esun = EsunDict[band]

        #print(sunAngle, des, gain, Esun)
        rad_fact = 10000.0 / (calCoef * bw)
        refl_fact = (10000.0 * des ** 2 * math.pi) / (calCoef * bw * Esun * math.cos(math.radians(theta)))

        logger.debug("%i: calibration coef %f, Earth-Sun distance %f, Esun %f, sun angle %f, bandwidth %f, "
                    "reflectance factor %f radiance factor %f", band, calCoef, des, Esun, sunAngle, bw, refl_fact,
                    rad_fact)

        if stretch == "rd":
            calibDict[band] = (rad_fact, 0)
        else:
            calibDict[band] = (refl_fact, 0)

    # return correction factor and offset
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
        logger.error("Unable to parse metadata from %s", metafile)
        return None

    # metad_map = dict((c, p) for p in metad.getiterator() for c in p)  # Child/parent mapping
    # attribs = ["Source_Image_ID", "Component_ID"]  # nodes we need the attributes of

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
            logger.error("Could not find any Source Image ID fields in metadata %s", metafile)
            return None

        siid_node = None
        for node in siid_nodes:
            if node.attrib["id"] == siid:
                siid_node = node
                break

        if siid_node is None:
            logger.error("Could not locate SIID: %s in metadata %s", siid, metafile)
            return None


    # Now assemble the dict
    for node in siid_node.getiterator():
        if node.tag in search_keys:
            if node.tag == "Source_Image_ID":
                metadict[node.tag] = node.attrib["id"]
            else:
                metadict[node.tag] = node.text

    return metadict


def GetGEcalibDict(metafile, stretch):
    fp_mode = "renamed"
    metadict = getGEMetadata(fp_mode, metafile)
    #print(metadict)

    calibDict = {}
    EsunDict = [196.0, 185.3, 150.5, 103.9, 161.7]

    for band in metadict["gain"].keys():
        sunAngle = float(metadict["firstLineSunElevationAngle"])
        theta = 90.0 - sunAngle
        datestr = metadict["originalFirstLineAcquisitionDateTime"] # 2009-11-01T01:49:33.685421Z
        des = calcEarthSunDist(datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S.%fZ"))
        gain = float(metadict["gain"][band])
        Esun = EsunDict[band - 1]

        logger.debug("Band {}, Sun elev: {}, Earth-Sun distance: {}, Gain: {}, Esun: {}".format(band, theta, des, gain, Esun))
        rad_fact = gain * 10 # multiply by 10 to convert from mW/cm2/um to W/m2/um
        refl_fact = (gain * des ** 2 * math.pi) / (Esun * math.cos(math.radians(theta)))

        if stretch == "rd":
            calibDict[band] = (rad_fact, 0)
        else:
            calibDict[band] = (refl_fact, 0)

    # return correction factor and offset
    return calibDict


def getGEMetadata(fp_mode, metafile):
    metadict = {}
    metad = utils.getGEMetadataAsXml(metafile)
    if metad is not None:

        search_keys = ["originalFirstLineAcquisitionDateTime", "firstLineSunElevationAngle"]
        for key in search_keys:
            node = metad.find(".//{}".format(key))
            if node is not None:
                metadict[key] = node.text

        band_keys = ["gain", "offset"]
        for key in band_keys:
            nodes = metad.findall(".//bandSpecificInformation")

            vals = {}
            for node in nodes:
                try:
                    band = int(node.attrib["bandNumber"])
                except Exception:
                    logger.error(utils.capture_error_trace())
                    logger.error("Unable to retrieve band number in GE metadata")
                else:
                    node = node.find(".//{}".format(key))
                    if node is not None:
                        vals[band] = node.text
            metadict[key] = vals
    else:
        logger.error("Unable to get metadata from %s", metafile)

    return metadict


def XmlToJ2w(jp2p):

    xmlp = jp2p + ".aux.xml"
    xml = open(xmlp, 'r')
    for line in xml:
        if "<GeoTransform>" in line:
            gt = line[line.find("<GeoTransform>") + len("<GeoTransform>"):line.find("</GeoTransform>")]
            gtl = gt.split(",")
            wldl = [float(gtl[1]), float(gtl[2]), float(gtl[4]), float(gtl[5]), float(gtl[0]) + float(gtl[1]) * 0.5,
                    float(gtl[3]) + float(gtl[5]) * 0.5]
    xml.close()

    j2wp = xmlp[:xmlp.find(".")] + ".j2w"
    j2w = open(j2wp, "w")
    for param in wldl:
        #print(param)
        j2w.write("{}\n".format(param))
    j2w.close()
