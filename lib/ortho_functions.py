
import argparse
import glob
import logging
import math
import os
import platform
import re
import shutil
import tarfile
import configparser
from datetime import datetime
from xml.dom import minidom
from xml.etree import cElementTree as ET

from osgeo import gdal, gdalconst, ogr, osr

from lib import taskhandler, utils
from lib import VERSION
from lib.utils import Vendor, ImageType, OutputType

gdal.UseExceptions()

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

DGbandList = ['BAND_P', 'BAND_C', 'BAND_B',  # Pan, Visible, NIR, Pansharpened: P*, M*, and S* product codes
              'BAND_G', 'BAND_Y', 'BAND_R', 'BAND_RE', 'BAND_N', 'BAND_N2',
              'BAND_S1', 'BAND_S2', 'BAND_S3', 'BAND_S4', 'BAND_S5',  # SWIR: A* product codes
              'BAND_S6', 'BAND_S7', 'BAND_S8',
              'BAND_DC', 'BAND_CG', 'BAND_W2', 'BAND_CRS', 'BAND_SNO',  # CAVIS: C* product codes
              'BAND_A31', 'BAND_A1', 'BAND_A2',  'BAND_W1', 'BAND_W3', 'BAND_NDVI', 'BAND_A32',
              ]
formats = {'GTiff': '.tif', 'JP2OpenJPEG': '.jp2', 'ENVI': '.envi', 'HFA': '.img', 'JPEG': '.jpg'}
stretches = ["ns", "rf", "mr", "rd", "au"]
resamples = ["near", "bilinear", "cubic", "cubicspline", "lanczos"]
gtiff_compressions = ["jpeg95", "lzw"]
exts = ['.ntf', '.tif']
ARGDEF_THREADS = 1

# slurm partitions as of 7/3/2024: update here for acceptable inputs to '--queue' arg if cluster partitions change
slurm_partitions = ['batch','big_mem','low_priority']

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

# Calibration factors taken from https://resources.maxar.com/white-papers/absolute-radiometric-calibration-white-paper
# IKONOS and QuickBird factors from https://dg-cms-uploads-production.s3.amazonaws.com/uploads/document/file/209/ABSRADCAL_FLEET_2016v0_Rel20170606.pdf
EsunDict = {  # Spectral Irradiance in W/m2/um
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
    'WV03_BAND_DC': 1718.25,
    'WV03_BAND_A1': 2001.13,
    'WV03_BAND_CG': 1831.3,
    'WV03_BAND_A2': 1537.38,
    'WV03_BAND_W1': 955.658,
    'WV03_BAND_W2': 866.791,
    'WV03_BAND_W3': 807.875,
    'WV03_BAND_NDVI': 460.196,
    'WV03_BAND_CRS': 361.412,
    'WV03_BAND_SNO': 230.349,
    'WV03_BAND_A31': 89.1345,
    'WV03_BAND_A32': 89.1345,

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

GainDict = {
    'QB02_BAND_P': 0.870,
    'QB02_BAND_B': 1.105,
    'QB02_BAND_G': 1.071,
    'QB02_BAND_R': 1.060,
    'QB02_BAND_N': 1.020,

    'WV01_BAND_P': 1.016,

    'WV02_BAND_P': 0.949,
    'WV02_BAND_C': 1.203,
    'WV02_BAND_B': 1.002,
    'WV02_BAND_G': 0.953,
    'WV02_BAND_Y': 0.946,
    'WV02_BAND_R': 0.955,
    'WV02_BAND_RE': 0.980,
    'WV02_BAND_N': 0.966,
    'WV02_BAND_N2': 1.010,

    'WV03_BAND_P': 0.955,
    'WV03_BAND_C': 0.938,
    'WV03_BAND_B': 0.946,
    'WV03_BAND_G': 0.958,
    'WV03_BAND_Y': 0.979,
    'WV03_BAND_R': 0.969,
    'WV03_BAND_RE': 1.027,
    'WV03_BAND_N': 0.977,
    'WV03_BAND_N2': 1.007,
    'WV03_BAND_S1': 1.030,
    'WV03_BAND_S2': 1.052,
    'WV03_BAND_S3': 0.992,
    'WV03_BAND_S4': 1.014,
    'WV03_BAND_S5': 1.012,
    'WV03_BAND_S6': 1.082,
    'WV03_BAND_S7': 1.056,
    'WV03_BAND_S8': 1.101,
    'WV03_BAND_DC': 1.377,
    'WV03_BAND_A1': 1.051,
    'WV03_BAND_CG': 0.816,
    'WV03_BAND_A2': 0.869,
    'WV03_BAND_W1': 0.849,
    'WV03_BAND_W2': 0.677,
    'WV03_BAND_W3': 0.819,
    'WV03_BAND_NDVI': 0.842,
    'WV03_BAND_CRS': 1,
    'WV03_BAND_SNO': 0.897,
    'WV03_BAND_A31': 1.081,
    'WV03_BAND_A32': 1.076,

    'GE01_BAND_P': 1.001,
    'GE01_BAND_B': 1.041,
    'GE01_BAND_G': 0.972,
    'GE01_BAND_R': 0.979,
    'GE01_BAND_N': 0.951,

    'IK01_BAND_P': 0.907,
    'IK01_BAND_B': 1.073,
    'IK01_BAND_G': 0.990,
    'IK01_BAND_R': 0.940,
    'IK01_BAND_N': 1.043
}

BiasDict = {
    'QB02_BAND_P': -1.491,
    'QB02_BAND_B': -2.820,
    'QB02_BAND_G': -3.338,
    'QB02_BAND_R': -2.954,
    'QB02_BAND_N': -4.722,

    'WV01_BAND_P': -1.824,

    'WV02_BAND_P': -5.523,
    'WV02_BAND_C': -11.839,
    'WV02_BAND_B': -9.835,
    'WV02_BAND_G': -7.218,
    'WV02_BAND_Y': -5.675,
    'WV02_BAND_R': -5.046,
    'WV02_BAND_RE': -6.114,
    'WV02_BAND_N': -5.096,
    'WV02_BAND_N2': -4.059,

    'WV03_BAND_P': -5.505,
    'WV03_BAND_C': -13.099,
    'WV03_BAND_B': -9.409,
    'WV03_BAND_G': -7.771,
    'WV03_BAND_Y': -5.489,
    'WV03_BAND_R': -4.579,
    'WV03_BAND_RE': -5.552,
    'WV03_BAND_N': -6.508,
    'WV03_BAND_N2': -3.699,
    'WV03_BAND_S1': 0,
    'WV03_BAND_S2': 0,
    'WV03_BAND_S3': 0,
    'WV03_BAND_S4': 0,
    'WV03_BAND_S5': 0,
    'WV03_BAND_S6': 0,
    'WV03_BAND_S7': 0,
    'WV03_BAND_S8': 0,
    'WV03_BAND_DC': 0,
    'WV03_BAND_A1': 0,
    'WV03_BAND_CG': 0,
    'WV03_BAND_A2': 0,
    'WV03_BAND_W1': 0,
    'WV03_BAND_W2': 0,
    'WV03_BAND_W3': 0,
    'WV03_BAND_NDVI': 0,
    'WV03_BAND_CRS': 0,
    'WV03_BAND_SNO': 0,
    'WV03_BAND_A31': 0,
    'WV03_BAND_A32': 0,

    'GE01_BAND_P': 0,
    'GE01_BAND_B': 0,
    'GE01_BAND_G': 0,
    'GE01_BAND_R': 0,
    'GE01_BAND_N': 0,

    'IK01_BAND_P': -4.461,
    'IK01_BAND_B': -9.699,
    'IK01_BAND_G': -7.937,
    'IK01_BAND_R': -4.767,
    'IK01_BAND_N': -8.869
}

# Defines the relationship between the output type and the NoData value that will be assigned to the destination rasters
NO_DATA_DICT = {
    OutputType.BYTE: 0,
    OutputType.UINT16: 65535,
    OutputType.FLOAT32: -9999.0,
}

IMAGE_TYPE_DICT = {
    'M': ImageType.MULTI,
    'P': ImageType.PAN,
    'S': ImageType.PANSH,
    'C': ImageType.CAVIS,
    'A': ImageType.SWIR,
    'BLU': ImageType.MULTI,
    'GRN': ImageType.MULTI,
    'RED': ImageType.MULTI,
    'NIR': ImageType.MULTI,
    'BGRN': ImageType.MULTI,
    'MSI': ImageType.MULTI,
    'PAN': ImageType.PAN
}

VISIBLE_IMAGE_TYPES = [
    ImageType.MULTI,
    ImageType.PAN,
    ImageType.PANSH
]

SWIR_CAVIS_IMAGE_TYPES = [
    ImageType.SWIR,
    ImageType.CAVIS
]


class ImageInfo:
    def __init__(self, srcfp, dstdir, wd, args):
        self.srcfp = srcfp
        self.srcdir, self.srcfn = os.path.split(srcfp)
        self.dstdir = dstdir
        self.ext = os.path.splitext(self.srcfp)[1].lower()
        if args.wd:
            self.localsrc = os.path.join(wd, self.srcfn)
        else:
            self.localsrc = self.srcfp

        # Verify EPSG. Epsg argument can be an integer, an integer as a string,
        # or 'utm' or 'auto'. The latter is handled in get_image_stats
        self.epsg = None
        self.spatial_ref = None
        try:
            epsg_code = int(args.epsg)
        except ValueError:
            pass
        else:
            try:
                spatial_ref = utils.SpatialRef(epsg_code)
            except RuntimeError:
                raise RuntimeError("Invalid EPSG code: %i", epsg_code)
            else:
                self.epsg = epsg_code
                self.spatial_ref = spatial_ref

        ## Get vendor info and text-based metadata
        self.vendor, self.sat, self.prod_code, self.band_name, self.tile, self.regex = utils.get_sensor(self.srcfn)
        if self.vendor is None:
            raise RuntimeError("Vendor not recognized")

        if (self.vendor == Vendor.GE and self.sat == 'IK01' and "_msi_" in self.srcfn
                and not os.path.isfile(self.localsrc)):
            src_image_name = self.srcfn.replace("_msi_", "_blu_")
            self.src_image = os.path.join(self.srcdir, src_image_name)
            self.bands = 4
        else:
            self.src_image = self.localsrc
            self.bands = None

        # Get image metadata as an Etree dictionary and set image type
        self.image_type = None

        if self.vendor == Vendor.DG:
            _mp = get_dg_metadata_path(self.srcfp, self.regex)
            if _mp is None:
                _mp = extract_dg_metadata_file(self.srcfp, self.regex, wd)
            _func = utils.get_dg_metadata_as_xml
            self.image_type = IMAGE_TYPE_DICT[self.prod_code[0]]

        elif self.vendor == Vendor.GE and self.sat == "GE01":
            _mp = get_ge_metadata_path(self.srcfp)
            _func = utils.get_ge_metadata_as_xml
            self.image_type = IMAGE_TYPE_DICT[self.band_name]

        elif self.vendor == Vendor.GE and self.sat == "IK01":
            _mp = get_ik_metadata_path(self.srcfp, self.regex)
            _func = utils.get_ik_metadata_as_xml
            self.image_type = IMAGE_TYPE_DICT[self.band_name]

        else:
            raise RuntimeError(f"Vendor or sensor not recognized: {self.vendor} {self.sat}")

        if _mp:
            self.metapath = _mp
            self.metad_etree = _func(self.metapath)
        else:
            raise RuntimeError(f"Cannot find metadata file")

        # Initialize attribs set by get_image_stats
        self.extent = ''
        self.extent_geom = None
        self.image_geom = None
        self.minlat = None
        self.minlon = None
        self.maxlat = None
        self.maxlon = None
        self.cent_lat = None
        self.cent_lon = None
        self.centerlong = ''
        self.geometry_wkt = None
        self.res = ''
        self.tap = ''
        self.rgb_bands = ''
        self.stretch = args.stretch  # this is updated in get_image_stats if == "au"

        # If items needed for the output name are not set, we have to open the image
        if self.stretch == 'au' or self.epsg is None:
            _err = self.get_image_stats(args)
            if _err != 0:
                raise RuntimeError(f"Error in stats calculation")

        self.dstfn = "{}_{}{}{}{}".format(
            os.path.splitext(self.srcfn)[0],
            utils.get_bit_depth(args.outtype),
            self.stretch,
            self.epsg,
            formats[args.format]
        )
        self.dstfp = os.path.join(self.dstdir, self.dstfn)
        if args.wd is not None:
            wd = args.wd
        else:
            wd = self.dstdir
        self.localdst = os.path.join(wd, self.dstfn)
        self.rawvrt = os.path.splitext(self.localdst)[0] + "_raw.vrt"
        self.warpfile = os.path.splitext(self.localdst)[0] + "_warp.tif"
        self.vrtfile = os.path.splitext(self.localdst)[0] + "_vrt.vrt"

    def get_image_stats(self, args):
        rc = 0
        # If image_geom is already set, this method was already run, skip running it again
        if not self.image_geom:
            try:
                ds = gdal.Open(self.src_image, gdalconst.GA_ReadOnly)
            except RuntimeError:
                logger.error("Cannot open dataset: %s", self.src_image)
                rc = 1
            else:
                if self.bands is None:
                    self.bands = ds.RasterCount

                ##  Get extent from GCPs
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
                    lrx = gtf[0] + xsize * gtf[1] + ysize * gtf[2]
                    lry = gtf[3] + xsize * gtf[4] + ysize * gtf[5]
                ds = None

                ####  Create geometry objects
                ul_geom = ogr.Geometry(ogr.wkbPoint)
                ul_geom.AddPoint(ulx, uly)
                ur_geom = ogr.Geometry(ogr.wkbPoint)
                ur_geom.AddPoint(urx, ury)
                lr_geom = ogr.Geometry(ogr.wkbPoint)
                lr_geom.AddPoint(lrx, lry)
                ll_geom = ogr.Geometry(ogr.wkbPoint)
                ll_geom.AddPoint(llx, lly)

                ring = ogr.Geometry(ogr.wkbLinearRing)
                ring.AddPoint(ulx, uly)
                ring.AddPoint(urx, ury)
                ring.AddPoint(lrx, lry)
                ring.AddPoint(llx, lly)
                ring.AddPoint(ulx, uly)
                image_geom = ogr.Geometry(ogr.wkbPolygon)
                image_geom.AddGeometry(ring)
                g_srs = srs_wgs84

                #### Create source srs objects
                s_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference(proj))
                try:
                    sg_ct = osr.CoordinateTransformation(s_srs, g_srs)
                except RuntimeError as e:
                    logger.error(f"Source image coordinate system error: {self.src_image} - {e}")
                    rc = 1
                else:
                    #### Transform geometries to geographic
                    if not s_srs.IsSame(g_srs):
                        ul_geom.Transform(sg_ct)
                        ur_geom.Transform(sg_ct)
                        ll_geom.Transform(sg_ct)
                        lr_geom.Transform(sg_ct)
                        image_geom.Transform(sg_ct)
                    logger.debug("Geographic extent: %s", str(image_geom))

                    #### Get geographic Envelope
                    self.minlon, self.maxlon, self.minlat, self.maxlat = image_geom.GetEnvelope()

                    ## if self.epgs is None, then EPSG needs to be determined
                    if not self.epsg:
                        self.cent_lat = (self.minlat + self.maxlat) / 2
                        self.cent_lon = (self.minlon + self.maxlon) / 2
                        self.epsg = get_epsg_from_lat_lon(self.cent_lat, self.cent_lon, mode=args.epsg, utm_nad83=args.epsg_utm_nad83)
                        logger.info("Automatically selected output projection EPSG code: %d", self.epsg)
                        try:
                            spatial_ref = utils.SpatialRef(self.epsg)
                        except RuntimeError:
                            logger.error(utils.capture_error_trace())
                            logger.error("Invalid EPSG code: %i", self.epsg)
                            rc = 1
                        else:
                            self.spatial_ref = spatial_ref

                    #### Create target srs objects
                    t_srs = self.spatial_ref.srs
                    gt_ct = osr.CoordinateTransformation(g_srs, t_srs)

                    #### Transform geoms to target srs
                    if not g_srs.IsSame(t_srs):
                        ul_geom.Transform(gt_ct)
                        ur_geom.Transform(gt_ct)
                        ll_geom.Transform(gt_ct)
                        lr_geom.Transform(gt_ct)
                        image_geom.Transform(gt_ct)
                    logger.debug("Projected extent: %s", str(image_geom))
                    self.image_geom = image_geom

                    rasterxsize_m = abs(
                        math.sqrt((ul_geom.GetX() - ur_geom.GetX()) ** 2 + (ul_geom.GetY() - ur_geom.GetY()) ** 2))
                    rasterysize_m = abs(
                        math.sqrt((ul_geom.GetX() - ll_geom.GetX()) ** 2 + (ul_geom.GetY() - ll_geom.GetY()) ** 2))

                    resx = abs(
                        math.sqrt((ul_geom.GetX() - ur_geom.GetX()) ** 2 +
                                  (ul_geom.GetY() - ur_geom.GetY()) ** 2) / xsize)
                    resy = abs(
                        math.sqrt((ul_geom.GetX() - ll_geom.GetX()) ** 2 +
                                  (ul_geom.GetY() - ll_geom.GetY()) ** 2) / ysize)

                    ####  Make a string for Pixel Size Specification
                    if args.resolution is not None:
                        if len(args.resolution) == 1:
                            self.res = "-tr {} {} ".format(args.resolution[0], args.resolution[0])
                        elif len(args.resolution) == 2:
                            self.res = "-tr {} {} ".format(args.resolution[0], args.resolution[1])
                        else: # this should already be checked in the argument parser validation
                            logger.error(f'--resolution argument has the wrong number of values: {len(args.resolution)}')
                            rc = 1
                    else:
                        self.res = "-tr {0:.12f} {1:.12f} ".format(resx, resy)
                        logger.info("Calculating output resolution from input image: {}".format(self.res))
                    if args.tap:
                        self.tap = "-tap "

                    logger.info("Original image size: %f x %f, res: %.12f x %.12f", rasterxsize_m, rasterysize_m, resx,
                                resy)

                    #### Set RGB bands
                    if args.rgb is True:
                        if self.bands == 1:
                            pass
                        elif self.bands in (3, 4):
                            self.rgb_bands = "-b 3 -b 2 -b 1 "
                        elif self.bands in (6, 8):
                            self.rgb_bands = "-b 5 -b 3 -b 2 "
                        else:
                            logger.error("Cannot get rgb bands from a %i band image", self.bands)
                            rc = 1

                    if args.bgrn is True:
                        if self.bands == 1:
                            pass
                        elif self.bands == 4:
                            pass
                        elif self.bands == 8:
                            self.rgb_bands = "-b 2 -b 3 -b 5 -b 7 "
                        else:
                            logger.error("Cannot get bgrn bands from a %i band image", self.bands)
                            rc = 1

                    if self.stretch == 'au':
                        # SWIR AND CAVIS should use the rf stretch
                        if self.vendor == Vendor.DG and self.image_type in [ImageType.SWIR, ImageType.CAVIS]:
                            self.stretch = 'rf'
                        # Antarctic visible imagery should use the rf stretch
                        elif ((self.maxlat + self.minlat) / 2) <= -60:
                            self.stretch = 'rf'
                        # Non-antarctic visible imagery should be mr
                        else:
                            self.stretch = 'mr'
                        logger.info("Automatically selected stretch: %s", self.stretch)

        return rc

    def set_extent_geom(self, target_extent_geom=None):
        rc = 0
        if target_extent_geom:
            if not self.image_geom.Intersects(target_extent_geom):
                logger.error("User-provided extent does not overlap image")
                rc = 1
            else:
                logger.info("Using user-provided extent: %s", str(target_extent_geom))
                self.extent_geom = target_extent_geom
        else:
            self.extent_geom = self.image_geom.Clone()

        if rc != 1:
            self.geometry_wkt = self.extent_geom.ExportToWkt()
            ## Get centroid and back project to geographic coords
            # (this is neccesary for images that cross 180)
            centroid = self.extent_geom.Centroid()
            tg_ct = osr.CoordinateTransformation(self.spatial_ref.srs, srs_wgs84)
            centroid.Transform(tg_ct)

            ## Get projected Envelope
            logger.info("Centroid: %s", str(centroid))

            if self.maxlon - self.minlon > 180:

                if centroid.GetX() < 0:
                    self.centerlong = '--config CENTER_LONG -180 '
                else:
                    self.centerlong = '--config CENTER_LONG 180 '

            minx, maxx, miny, maxy = self.extent_geom.GetEnvelope()
            self.extent = "-te {0:.12f} {1:.12f} {2:.12f} {3:.12f} ".format(minx, miny, maxx, maxy)
        return rc


def get_destination_nodata(output_type: str | OutputType) -> int | float:
    """Determines the destination NoData value for a given output data type.

    Raises a ValueError if the provided value does not match one of the known OutputType variants."""
    if type(output_type) == str:
        output_type = OutputType(output_type)

    return NO_DATA_DICT[output_type]


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


def build_parent_argument_parser():

    # input arguments from command line
    script_path = os.path.dirname(os.path.realpath(__file__))
    default_config = os.path.join(os.path.abspath(os.path.join(script_path, os.pardir)), "doc/config.ini")

    #### Set Up Arguments
    parser = argparse.ArgumentParser(add_help=False)

    #### Positional Arguments
    parser.add_argument("src", help="source image, text file, or directory")
    parser.add_argument("dst", help="destination directory")
    pos_arg_keys = ["src", "dst"]

    ## Optional Arguments
    parser.add_argument("-f", "--format", choices=formats.keys(), default="GTiff",
                        help="output to the given format (default=GTiff)")
    parser.add_argument("--gtiff-compression", choices=gtiff_compressions, default="lzw",
                        help="GTiff compression type (default=lzw)")
    parser.add_argument("-p", "--epsg", required=False, type=str,
                        help="EPSG projection code for output files [int: EPSG code, "
                             "'utm': closest UTM zone, 'auto': closest UTM zone or polar stereo "
                             "(polar stereo cutoff is at 60 N/S latitude)]")
    parser.add_argument("--epsg-utm-nad83", action='store_true', default=False,
                        help="Use NAD83 datum instead of WGS84 for '--epsg auto/utm' UTM zone projection EPSG codes")
    parser.add_argument("-d", "--dem",
                        help="the DEM to use for orthorectification (elevation values should be relative to the wgs84 "
                             "ellipsoid,  'auto': closest dem overlapping the area")
    parser.add_argument("--config-file", help="Location of config file (default={})".format(default_config),
                        default=default_config)
    parser.add_argument("-t", "--outtype", choices=[output_type.value for output_type in OutputType], default=OutputType.BYTE.value,
                        help=f"output data type (default={OutputType.BYTE.value})")
    parser.add_argument("-r", "--resolution", type=float, nargs='*',
                        help="output pixel resolution in units of the projection (<xres> <yres>|square)")
    parser.add_argument("-c", "--stretch", choices=stretches, default="rf",
                        help="stretch type [ns: nostretch, rf: reflectance (default), mr: modified reflectance, rd: "
                             "absolute radiance, au: automatically set (rf for images below 60S latitude, "
                             "otherwise mr)]")
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
                        'the wgs84 ellipsoid)')
    parser.add_argument("--threads", type=thread_type(),
                        help='Number of threads to use for gdalwarp and gdal_pansharpen processes, if applicable '
                             '(default={0}, number on system={1}). Can use any positive integer, or ALL_CPUS. '
                             'Any value above system count will default to ALL_CPUS. If used with '
                             '--parallel-processes, the (threads * number of processes) must be <= system count. '
                             '--pbs/--slurm will only accept 1 thread.'
                        .format(ARGDEF_THREADS, ARGDEF_CPUS_AVAIL),
                        default=ARGDEF_THREADS)
    parser.add_argument("--skip-cmd-txt", action='store_true', default=True,
                        help='THIS OPTION IS DEPRECATED - '
                             'By default this arg is True and the cmd text file will not be written. '
                             'Input commands are written to the log for reference.')
    parser.add_argument("--version", action='version', version="imagery_utils v{}".format(VERSION))

    return parser, pos_arg_keys


def process_image(srcfp, dstfp, args, target_extent_geom=None):
    err = 0
    starttime = datetime.today()

    ## Handle threads (default to 1 if arg not supplied)
    gdal_thread_count = 1 if not hasattr(args, 'threads') else args.threads

    ## Get working dir
    if args.wd is not None:
        wd = args.wd
    else:
        wd = os.path.dirname(dstfp)
    if not os.path.isdir(wd):
        try:
            os.makedirs(wd)
        except OSError:
            pass
    logger.info("Working Dir: %s", wd)
    logger.info('Image: %s', os.path.basename(srcfp))

    ##  Initialize ImageInfo object with filename-based and argument-based attributes
    try:
        info = ImageInfo(srcfp, os.path.dirname(dstfp), wd, args)
    except Exception as e:
        logger.error(e)
        err = 1
    else:
        # Cleanup temp files from failed or interrupted processing attempt
        ik_stacked_sem = "{}.stacked".format(os.path.join(wd, info.srcfn))
        if args.wd or os.path.isfile(ik_stacked_sem):
            utils.delete_temp_files([info.dstfp, info.rawvrt, info.warpfile, info.vrtfile, info.localsrc])
        else:
            utils.delete_temp_files([info.dstfp, info.rawvrt, info.warpfile, info.vrtfile])

        ## Verify that dem and ortho_height are not both specified
        if args.dem is not None and args.ortho_height is not None:
            logger.error("--dem and --ortho_height options are mutually exclusive.  Please choose only one.")
            err = 1

        ## Verify that output type and stretch options are compatible
        if args.stretch == 'rd' and args.outtype == OutputType.BYTE.value:
            logger.error("Output type Byte is not compatible with absolution radiance (rd stretch)")
            err = 1

        if args.stretch == 'ns' and args.outtype == OutputType.BYTE.value and info.image_type != ImageType.CAVIS:
            logger.error('Output type Byte is not compatible with no stretch (ns stretch)')
            err = 1

        if args.stretch == 'ns' and args.outtype == OutputType.FLOAT32.value:
            logger.error('Output type Float32 is not reasonable with no stretch (ns stretch)')
            err = 1

        if args.stretch == 'mr' and args.outtype == OutputType.FLOAT32.value:
            logger.error('Output type Float32 is not reasonable with modified reflectance (mr stretch)')
            err = 1

        if args.stretch == 'mr' and args.outtype == OutputType.UINT16.value:
            logger.error('Output type UInt16 is not reasonable with modified reflectance (mr stretch)')
            err = 1

        if args.gtiff_compression == 'jpeg95' and args.outtype == OutputType.UINT16.value:
            logger.error('Output type UInt16 is not compatible with jpeg compression')
            err = 1

        ## Check if image is type and stretch are appropriate
        if info.prod_code:
            if info.prod_code[3] == 'M':
                logger.error("Cannot process mosaic product")
                err = 1
            if info.prod_code[1] == '3':
                logger.error("Cannot process 3* products")
                err = 1
            if (info.prod_code[1:3] == '2A' and info.tile is not None and info.ext == '.tif') and not args.skip_warp:
                logger.error("Cannot process 2A tiled Geotiffs")
                err = 1

            ## Log error if imagery is not optical (e.g. swir/cavis) and --bgrn options were used

            if args.bgrn and info.image_type not in VISIBLE_IMAGE_TYPES:
                logger.error(f"--bgrn option is not valid for this image type: {info.image_type.value}")
                err = 1

            ## Inform the user that --rgb uses bands 5,4,2 for SWIR and CAVIS
            if args.rgb and info.image_type in SWIR_CAVIS_IMAGE_TYPES:
                logger.info(f"--rgb option uses bands 5, 4, and 2 for this image type: {info.image_type.value}")

            ## Log error if imagery is not optical (e.g. swir/cavis) and the  "mr" stretch is used
            if (args.stretch == 'mr') and info.image_type in SWIR_CAVIS_IMAGE_TYPES:
                logger.error(
                    f"The modified reflectance (mr) stretch is not valid for this image type: {info.image_type.value}")
                err = 1

        ## Check If Image is IKONOS msi that does not exist, if so, stack to dstdir, else, copy srcfn to dstdir
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
                    rc = stack_ik_bands(info.localsrc, members)
                    #if not os.path.isfile(os.path.join(wd, os.path.basename(info.metapath))):
                    #    shutil.copy(info.metapath, os.path.join(wd, os.path.basename(info.metapath)))
                    if rc == 1:
                        logger.error("Error building merged Ikonos image: %s", info.srcfp)
                        err = 1
                    elif os.path.isfile(info.localsrc):
                        with open(ik_stacked_sem, 'w') as _:
                            pass

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

        ## Open raster to get further processing info
        if not err == 1:
            rc = info.get_image_stats(args)
            if rc != 1:
                # Set target_extent variables, including user-supplied extent if applicable
                rc = info.set_extent_geom(target_extent_geom)
            if rc == 1:
                err = 1
                logger.error("Error in stats calculation")

    # Check if DEM is set to 'auto'
    if args.dem == 'auto':
        try:
            # Attempt to read the config file
            config = configparser.ConfigParser()
            config_file_path = args.config_file
            if not os.path.isfile(config_file_path):
                logger.error("Config file not found: {}".format(config_file_path))
                logger.error("Please provide a valid config file path for 'auto' DEM setting.")
                err = 1

        except (configparser.NoSectionError, configparser.Error) as e:
            logger.error("Error reading config file: %s", e)
            logger.error("Please ensure the config file exists and is correctly formatted for 'auto' DEM.")
            err = 1

        else:
            config.read(config_file_path)
            gpkg_path = config.get("default", "gpkg_path", fallback=None)

            if not gpkg_path:
                logger.error("gpkg_path not found in config file. Please check the config file format.")
                err = 1
            elif not os.path.isfile(gpkg_path):
                logger.error("The gpkg file does not exist at the expected location: {}".format(gpkg_path))
                err = 1
            # Proceed with 'auto' DEM processing if no errors
            if not err == 1:
                try:
                    args.dem = check_image_auto_dem(info.geometry_wkt, info.spatial_ref, gpkg_path)
                except RuntimeError as e:
                    logger.error(e)
                    err = 1
                else:
                    if args.dem is None:
                        logger.info("No candidate DEM found overlapping image. Proceeding without a DEM")
                    else:
                        logger.info(f"Auto DEM selected: {args.dem}")

    if not err == 1:
        ## Check if image overlaps reference DEM
        if args.dem and not args.skip_dem_overlap_check:
            overlap = overlap_check(info.geometry_wkt, info.spatial_ref, args.dem)
            if overlap is False:
                err = 1

        if not os.path.isfile(info.dstfp):
            ## Warp Image
            if not err == 1 and not os.path.isfile(info.warpfile):
                rc = warp_image(args, info, gdal_thread_count=gdal_thread_count)
                if rc == 1:
                    err = 1
                    logger.error("Error in image warping")

            #### Calculate Output File
            if not err == 1 and os.path.isfile(info.warpfile):
                rc = calc_stats(args, info)
                if rc == 1:
                    err = 1
                    logger.error("Error in image calculation")

        ##  Write Output Metadata
        if not err == 1:
            rc = write_output_metadata(args, info)
            if rc == 1:
                err = 1
                logger.error("Error in writing metadata file")

        ## Copy image to final location if working dir is used
        if args.wd is not None:
            if not err == 1:
                logger.info("Copying to destination directory")
                for fpi in glob.glob("{}.*".format(os.path.splitext(info.localdst)[0])):
                    fpo = os.path.join(info.dstdir, os.path.basename(fpi))
                    if not os.path.isfile(fpo):
                        shutil.copy2(fpi, fpo)
            if not args.save_temps:
                utils.delete_temp_files([info.localdst])

        ## Check If Done, Delete Temp Files
        done = os.path.isfile(info.dstfp)
        if done is False:
            err = 1
            logger.error("Final image not present")

        if err == 1:
            logger.error("Processing failed: %s", info.srcfn)
            if not args.save_temps:
                if args.wd or os.path.isfile(ik_stacked_sem):
                    utils.delete_temp_files([info.dstfp, info.rawvrt, info.warpfile, info.vrtfile, info.localsrc])
                else:
                    utils.delete_temp_files([info.dstfp, info.rawvrt, info.warpfile, info.vrtfile])

        elif not args.save_temps:
            if args.wd or os.path.isfile(ik_stacked_sem):
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
    logger.info("Total Processing Time: %s", td)
    return err


def stack_ik_bands(dstfp, members):
    rc = 0
    band_dict = {1: gdalconst.GCI_BlueBand,
                 2: gdalconst.GCI_GreenBand,
                 3: gdalconst.GCI_RedBand,
                 4: gdalconst.GCI_Undefined}
    remove_keys = ("NITF_FHDR", "NITF_IREP", "NITF_OSTAID", "NITF_IC", "NITF_ICORDS", "NITF_IGEOLO", "IREPBAND")
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
        keys = list(m.keys())
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


def get_epsg_from_lat_lon(lat, lon, mode='auto', utm_nad83=False):
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
    utm_nad83 : bool
        If NAD83 datum should be used

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
            if utm_nad83:
                epsg_code = 26900 + utm_zone_num
                if 26901 <= epsg_code <= 26923:
                    pass
                else:
                    raise utils.InvalidArgumentError(
                        "--epsg-auto-nad83 option is only applicable for images in northern hemisphere UTM zones 1-23"
                    )
            else:
                epsg_code = 32600 + utm_zone_num
        else:
            if utm_nad83:
                raise utils.InvalidArgumentError(
                    "--epsg-auto-nad83 option is not applicable for images in the southern hemisphere"
                )
            else:
                epsg_code = 32700 + utm_zone_num

    elif mode == 'auto':
        if lat > 60:
            epsg_code = 3413
        elif lat < 60:
            epsg_code = 3031

    assert type(epsg_code) is int
    return epsg_code


def calc_stats(args, info):

    logger.info("Calculating image with stats")
    rc = 0

    ## Get Well-known Text String of the spatial reference systems
    p = info.spatial_ref.srs
    prj = p.ExportToWkt()

    ## Set input max from image type
    if info.image_type in VISIBLE_IMAGE_TYPES:  # Optical: 11 bit
        imax = 2047.0
    elif info.image_type in SWIR_CAVIS_IMAGE_TYPES:  # SWIR and CAVIS: 14 bit
        imax = 16383.0
    else:
        logger.error(f"Image type {info.image_type} not supported")
        return 1

    if info.stretch == 'ns':
        if args.outtype == OutputType.BYTE.value:
            omax = 255.0
        elif args.outtype == OutputType.UINT16.value:
            omax = imax
        elif args.outtype == OutputType.FLOAT32.value:
            omax = imax
    elif info.stretch == 'mr':
        if args.outtype == OutputType.BYTE.value:
            omax = 255.0
        elif args.outtype == OutputType.UINT16.value:
            omax = imax
        elif args.outtype == OutputType.FLOAT32.value:
            omax = 1.0
    elif info.stretch == 'rf':
        if args.outtype == OutputType.BYTE.value:
            omax = 200.0
        elif args.outtype == OutputType.UINT16.value:
            if imax == 2047.0:  # Optical
                omax = 2000.0
            elif imax == 16383.0:  # SWIR and CAVIS
                omax = 16000.0
        elif args.outtype == OutputType.FLOAT32.value:
            omax = 1.0

    dst_nodata = get_destination_nodata(args.outtype)

    #### Stretch
    if info.stretch != "ns":
        CFlist = get_calibration_factors(info)
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
            for band in range(1, vds.RasterCount+1):
                if info.stretch == "ns":
                    LUT = "0:0,{}:{}".format(imax, omax)
                else:
                    calfact, offset = CFlist[band-1]
                    if info.stretch == "rf":
                        LUT = "0:{},{}:{}".format(offset*omax, imax, (imax*calfact+offset)*omax)
                    elif info.stretch == "rd":
                        LUT = "0:{},{}:{}".format(offset, imax, imax*calfact+offset)
                    elif info.stretch == "mr":
                        # modified reflectance is rf with a non-linear curve applied according
                        # to the following histgram points
                        iLUT = [0, 0.125, 0.25, 0.375, 0.625, 1]
                        oLUT = [0, 0.375, 0.625, 0.75, 0.875, 1]
                        lLUT = map(lambda x: "{}:{}".format(
                            (iLUT[x]-offset)/calfact,  # find original DN for each 0-1 iLUT
                            # step by applying reverse reflectance transformation
                            omax*oLUT[x]  # output value for each 0-1 oLUT step multiplied by omax
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
                                    '   <NODATA>{5}</NODATA>'
                                    '</ComplexSource>)'.format(info.warpfile, band, LUT, xsize, ysize, dst_nodata))

                vds.GetRasterBand(band).SetMetadataItem("source_0", ComplexSourceXML, "vrt_sources")
                vds.GetRasterBand(band).SetNoDataValue(dst_nodata)
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
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=YES" '
        elif args.gtiff_compression == 'jpeg95':
            co = '-co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "compress=jpeg" -co "jpeg_quality=95" -co ' \
                 '"BIGTIFF=YES" '

    elif args.format == 'HFA':
        co = '-co "COMPRESSED=YES" -co "STATISTICS=YES" '

    elif args.format == 'JP2OpenJPEG':
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


def get_image_geometry_info(src_image, spatial_ref, args, return_type='extent_geom'):
    return_type_choices = ['extent_geom', 'epsg_code']
    if return_type not in return_type_choices:
        raise utils.InvalidArgumentError(
            "`return_type` must be one of {} but was '{}'".format(
                return_type_choices, return_type
            )
        )

    srcfn = os.path.basename(src_image)
    if not os.path.isfile(src_image) and srcfn.startswith("IK01") and "_msi_" in srcfn:
        srcfn = srcfn.replace("_msi_", "_blu_")
        src_image = os.path.join(os.path.dirname(src_image), srcfn)
    try:
        ds = gdal.Open(src_image, gdalconst.GA_ReadOnly)
    except RuntimeError as e:
        logger.error(f"Cannot open dataset: {e}")
        return None

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
    try:
        sg_ct = osr.CoordinateTransformation(s_srs, g_srs)
    except RuntimeError as e:
        logger.error(f"Source image coordinate system error: {src_image} - {e}")
        return None

    #### Transform geometries to geographic
    if not s_srs.IsSame(g_srs):
        extent_geom.Transform(sg_ct)
    # logger.info("Geographic extent: %s", str(extent_geom))

    #### Get geographic Envelope
    minlon, maxlon, minlat, maxlat = extent_geom.GetEnvelope()

    ## Determine output image projection if applicable
    try:
        epsg_code = int(args.epsg)
    except ValueError:
        cent_lat = (minlat + maxlat) / 2
        cent_lon = (minlon + maxlon) / 2
        epsg_code = get_epsg_from_lat_lon(cent_lat, cent_lon, mode=args.epsg, utm_nad83=args.epsg_utm_nad83)

    try:
        spatial_ref = utils.SpatialRef(epsg_code)
    except RuntimeError as e:
        logger.error(utils.capture_error_trace())
        logger.error("Invalid EPSG code: %i", epsg_code)
        return None

    if return_type == 'epsg_code':
        return epsg_code

    #### Create target srs objects
    t_srs = spatial_ref.srs
    gt_ct = osr.CoordinateTransformation(g_srs, t_srs)

    #### Transform geoms to target srs
    if not g_srs.IsSame(t_srs):
        extent_geom.Transform(gt_ct)
    # logger.info("Projected extent: %s", str(extent_geom))

    return extent_geom


def get_dg_metadata_path(srcfp, regex):
    """
    Returns the filepath of the XML, if it can be found. Returns
    None if no valid filepath could be found.
    """
    filebasename = os.path.splitext(os.path.basename(srcfp))[0]
    srcdir = os.path.dirname(srcfp)
    metapath = os.path.join(srcdir, filebasename) + '.xml'
    if not os.path.isfile(metapath):
        metapath = os.path.join(srcdir, filebasename) + '.XML'
    if not os.path.isfile(metapath):
        # Tiled DG images may have a metadata file at the strip level
        match = re.match(regex, filebasename.lower())
        metapath = None
        if match:
            if match.group('tile'):
                tile_removed = filebasename[:match.start('tile') - 1] + filebasename[match.end('tile'):]
                metapath = os.path.join(srcdir, tile_removed) + '.xml'
                if not os.path.isfile(metapath):
                    os.path.join(srcdir, tile_removed) + ".XML"
                if not os.path.isfile(metapath):
                    metapath = None

    return metapath


def get_ik_metadata_path(srcfp, regex):
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
        for b in ikMsiBands:
            mp = metapath.replace(b, 'rgb')
            if os.path.isfile(mp):
                metapath = mp
                break

    if not os.path.isfile(metapath):
        metapath = os.path.splitext(srcfp)[0] + '_metadata.txt'

    if not os.path.isfile(metapath):
        for b in ikMsiBands:
            mp = metapath.replace(b, 'rgb')
            if os.path.isfile(mp):
                metapath = mp
                break

    if not os.path.isfile(metapath):
        filebasename = os.path.splitext(os.path.basename(srcfp))[0]
        srcdir = os.path.dirname(srcfp)
        match = re.match(regex, filebasename.lower())
        metapath = None
        if match:
            if match.group('po'):
                tile_removed = filebasename[:match.end('po')]
                metapath = os.path.join(srcdir, tile_removed) + '_metadata.txt'
                if not os.path.isfile(metapath):
                    metapath = None

    return metapath


def get_ge_metadata_path(srcfp):
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


def extract_dg_metadata_file(srcfp, regex, wd):
    """
    Searches the .tar for a valid XML. If found,
    extracts the metadata file. Returns
    None if no valid metadata could be found.
    """

    metapath = None
    filename = os.path.basename(srcfp)
    tarpath = os.path.splitext(srcfp)[0] + '.tar'
    if os.path.isfile(tarpath):
        match = re.search(regex, filename)
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


def write_output_metadata(args, info):
    ####  Ortho metadata name
    omd = os.path.splitext(info.localdst)[0] + ".xml"
    til = None
    imd = None

    #  If DG
    if info.vendor == Vendor.DG:
        imd = info.metad_etree.find("IMD")
        til = info.metad_etree.find("TIL")

    #  If GE
    elif info.vendor == Vendor.GE and info.sat == "GE01":
        imd = ET.Element("IMD")
        include_tags = ["sensorInfo", "inputImageInfo", "correctionParams", "bandSpecificInformation"]

        elem = info.metad_etree.find("productInfo")
        if elem is not None:
            rpc = elem.find("rationalFunctions")
            elem.remove(rpc)
            imd.append(elem)

        elem = info.metad_etree.find('productOrderInfo')
        elem.remove(elem.find('numberOfAOICoordinates'))
        for child in elem.findall('aoiGeoCoordinate'):
            elem.remove(child)
        for child in elem.findall('aoiMapCoordinate'):
            elem.remove(child)
        imd.append(elem)

        for tag in include_tags:
            elems = info.metad_etree.findall(tag)
            imd.extend(elems)

    elif info.sat in ['IK01']:
        match = re.search(info.regex, info.srcfn)
        if match:
            component = match.group('cmp')
            imd = ET.Element("IMD")

            elem = info.metad_etree.find('Source_Image_Metadata')
            elem.remove(elem.find('Number_of_Source_Images'))
            for child in elem.findall("Source_Image_ID"):
                prod_id_elem = child.find("Product_Image_ID")
                if not prod_id_elem.text == component[:3]:
                    elem.remove(child)
            imd.append(elem)

            elem = info.metad_etree.find('Product_Component_Metadata')
            elem.remove(elem.find('Number_of_Components'))
            for child in elem.findall("Component_ID"):
                if not child.attrib['id'] == component:
                    elem.remove(child)
            imd.append(elem)

            elem = info.metad_etree.find('Product_Order_Metadata')
            elem.remove(elem.find('Product_Order_Area_Geographic_Coordinates'))
            elem.remove(elem.find('Product_Order_Area_Map_Coordinates_in_Map_Units'))
            imd.append(elem)

    ####  Determine custom MD
    tm = datetime.today()
    dem_val = None
    if not args.skip_warp:
        if args.dem:
            dem_val = os.path.basename(args.dem)
        elif args.ortho_height is not None:
            dem_val = str(args.ortho_height)
        else:
            dem_val = str(get_rpc_height(info))

    dMD = {
        "VERSION": "imagery_utils v{}".format(VERSION),
        "PROCESS_DATE": tm.strftime("%d-%b-%Y %H:%M:%S"),
        "ORTHO_HEIGHT": dem_val,
        "RESAMPLEMETHOD": args.resample,
        "STRETCH": info.stretch,
        "BITDEPTH": args.outtype,
        "FORMAT": args.format,
        "COMPRESSION": args.gtiff_compression,
        "EPSG_CODE": str(info.epsg)
    }

    pgcmd = ET.Element("PGC_IMD")
    for tag in dMD:
        if dMD[tag]:
            child = ET.SubElement(pgcmd, tag)
            child.text = dMD[tag]

    ####  Write output
    root = ET.Element("IMD")
    root.append(pgcmd)

    ref = ET.SubElement(root, "SOURCE_IMD")
    child = ET.SubElement(ref, "SOURCE_IMAGE")
    child.text = os.path.basename(info.localsrc)
    child = ET.SubElement(ref, "VENDOR")
    child.text = info.vendor.value

    if imd is not None:
        ref.append(imd)
    if til is not None:
        ref.append(til)

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


def warp_image(args, info, gdal_thread_count=1):

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
                if info.vendor == Vendor.DG:
                    rpb_p = os.path.splitext(info.localsrc)[0] + ".RPB"

                elif info.vendor == Vendor.GE and info.sat == "GE01":
                    rpb_p = os.path.splitext(info.localsrc)[0] + "_rpc.txt"

                elif info.sat in ['IK01']:
                    rpb_p = os.path.splitext(info.localsrc)[0] + "_rpc.txt"

                else:
                    rpb_p = None
                    # logger.error("Cannot extract rpc's for Ikonos. Image cannot be terrain corrected with a DEM or "
                    #              "avg elevation.")
                    # rc = 1

                # if rpb_p:
                if True:
                    if rpb_p is None or not os.path.isfile(rpb_p):
                        err = extract_rpb(info.localsrc, rpb_p)
                        if err == 1:
                            rc = 1
                    if rpb_p is None or not os.path.isfile(rpb_p):
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

        # This sets 0 as the NoData value in all bands in the source image
        src_nodata_list = ["0"] * info.bands

        # This sets the NoData value in all bands in the destination image based on output data type
        dst_nodata = get_destination_nodata(args.outtype)
        dst_nodata_list = [str(dst_nodata)] * info.bands

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
                cmd = 'gdalwarp {} -srcnodata "{}" -dstnodata "{}" -of GTiff -ot Float32 {}{}{}{}-co "TILED=YES" -co "BIGTIFF=YES" ' \
                      '-t_srs "{}" -r {} -et 0.01 -rpc -to "{}" "{}" "{}"'.format(
                        config_options,
                        " ".join(src_nodata_list),
                        " ".join(dst_nodata_list),
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
                if err == 1:
                    rc = 1

        else:
            #### GDALWARP Command
            cmd = 'gdalwarp {} -srcnodata "{}" -dstnodata "{}" -of GTiff -ot UInt16 {}{}-co "TILED=YES" -co "BIGTIFF=YES" -t_srs ' \
                  '"{}" -r {} "{}" "{}"'.format(
                    config_options,
                    " ".join(src_nodata_list),
                    " ".join(dst_nodata_list),
                    info.res,
                    info.tap,
                    info.spatial_ref.proj4,
                    args.resample,
                    info.rawvrt,
                    info.warpfile
                    )

            (err, so, se) = taskhandler.exec_cmd(cmd)
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


def get_calibration_factors(info):

    calibDict = {}
    CFlist = []
    bandList = []

    if info.vendor == Vendor.DG:
        try:
            calibDict = get_dg_calib_dict(info.metad_etree, info.stretch)
        except utils.InvalidMetadataError as e:
            logger.error(e)
        bandList = DGbandList

    elif info.vendor == Vendor.GE and info.sat == "GE01":
        try:
            calibDict = get_ge_calib_dict(info.metad_etree, info.stretch)
        except utils.InvalidMetadataError as e:
            logger.error(e)
        if info.bands == 1:
            bandList = [5]
        elif info.bands == 4:
            bandList = range(1, 5, 1)

    elif info.vendor == Vendor.GE and info.sat == "IK01":
        try:
            calibDict = get_ik_calib_dict(info.metad_etree, info.metapath, info.regex, info.stretch)
        except utils.InvalidMetadataError as e:
            logger.error(e)
        if info.bands == 1:
            bandList = [4]
        elif info.bands == 4:
            bandList = range(0, 4, 1)
        elif info.bands == 3:
            bandList = range(0, 3, 1)

    else:
        logger.error(f"Vendor or sensor not recognized: {info.vendor} {info.sat}")

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

            demSpatialReference = utils.osr_srs_preserve_axis_order(osr.SpatialReference(demProjection))

            coordinateTransformer = osr.CoordinateTransformation(imageSpatialReference, demSpatialReference)
            if not imageSpatialReference.IsSame(demSpatialReference):
                imageGeometry.Transform(coordinateTransformer)

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

def check_image_auto_dem(geometry_wkt, spatial_ref, gpkg_path):
    """
    Parameters:
        geometry_wkt (wkt): Geometry of input image
        spatial_ref
        gpkg_path (str): Path to the GeoPackage file.

    Returns:
        str: The value of the "dempath" field from the final overlapping layer, or None if no overlap is found.
    """
    # image properties
    imageSpatialReference = spatial_ref.srs
    image_geometry = ogr.CreateGeometryFromWkt(geometry_wkt)

    # Open the GeoPackage dataset
    try:
        dataset = gdal.OpenEx(gpkg_path, gdal.OF_VECTOR)
    except Exception as e:
        raise RuntimeError("Error opening the GeoPackage file: %s", e)

    num_layers = dataset.GetLayerCount()
    overlapping_dems = []
    dempath = None

    selected_rank = 9999
    selected_dem = None
    # Iterate over each layer in the dataset
    for i in range(num_layers):
        layer = dataset.GetLayerByIndex(i)
        if layer is None:
            # Skip this layer if it's None
            continue

        layer_spatial_ref = layer.GetSpatialRef()
        if layer_spatial_ref is None:
            # Skip this layer if its spatial reference is None
            continue

        # Check if the image geometry is in the same spatial reference as the current layer
        if not imageSpatialReference.IsSame(layer_spatial_ref):
            coordinate_transformer = osr.CoordinateTransformation(imageSpatialReference, layer_spatial_ref)
            image_geometry_transformed = image_geometry.Clone()
            image_geometry_transformed.Transform(coordinate_transformer)
        else:
            image_geometry_transformed = image_geometry

        # Find the overlapping feature in the layer
        dem = layer.GetNextFeature()
        while dem:
            dem_geometry = dem.GetGeometryRef()
            if dem_geometry is None:
                logger.debug("Skipping feature %s in layer %d because its geometry is None", (dem, i))
                dem = layer.GetNextFeature()
                continue
            try:
                if image_geometry_transformed.Within(dem_geometry) and dem['rank'] < selected_rank:
                    selected_dem = dem
                    selected_rank = dem['rank']
            except Exception as e:
                raise RuntimeError("Error processing feature in layer %d: %s", i, e)
            dem = layer.GetNextFeature()


    if selected_dem is None:
        return None

    try:
        if platform.system() == "Windows": # this is for selecting between specific versions of the PGC reference DEM file
            dempath = selected_dem.GetField("windowspath")
        else:
            dempath = selected_dem.GetField("dempath")
    except Exception as e:
        raise RuntimeError("Error getting 'dempath' field: %s", e)

    # Close the dataset
    del dataset
    return dempath

def extract_rpb(item, rpb_p):
    rc = 0
    tar_p = os.path.splitext(item)[0] + ".tar"
    logger.info(tar_p)
    if os.path.isfile(tar_p):
        fp_extracted = list()
        try:
            tar = tarfile.open(tar_p, 'r')
            tarlist = tar.getnames()
            for t in tarlist:
                if '.rpb' in t.lower() or '_rpc' in t.lower():  # or '.til' in t.lower():
                    tf = tar.extractfile(t)
                    fp = os.path.splitext(rpb_p)[0] + os.path.splitext(t)[1]
                    fp_extracted.append(fp)
                    fpfh = open(fp, "w")
                    tfstr = tf.read()
                    if type(tfstr) is bytes and type(tfstr) is not str:
                        tfstr = tfstr.decode('utf-8')
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


def calc_earth_sun_dist(t):
    year = t.year
    month = t.month
    day = t.day
    hr = t.hour
    minute = t.minute
    sec = t.second
    ut = hr + (minute / 60.) + (sec / 3600.)

    if month <= 2:
        year = year - 1
        month = month + 12

    a = int(year / 100)
    b = 2 - a + int(a / 4)
    jd = int(365.25 * (year + 4716)) + int(30.6001 * (month + 1)) + day + (ut / 24) + b - 1524.5

    g = 357.529 + 0.98560028 * (jd - 2451545.0)
    d = 1.00014 - 0.01671 * math.cos(math.radians(g)) - 0.00014 * math.cos(math.radians(2 * g))

    return d


def get_dg_calib_dict(metad_etree, stretch):

    calibDict = {}
    abscalfact_dict = {}
    nodeIMD = metad_etree.find('IMD')
    if nodeIMD is None:
        raise utils.InvalidMetadataError(f"Metadata file is missing the IMD xml section")
    else:
        nodeIMAGE = nodeIMD.find('IMAGE')
        nodeMPP = nodeIMD.find('MAP_PROJECTED_PRODUCT')

        sat = nodeIMAGE.find('SATID').text
        elem = nodeIMAGE.find('FIRSTLINETIME')
        if elem is None:
            if nodeMPP is not None:
                elem = nodeMPP.find('EARLIESTACQTIME')
        if elem is not None:
            t = elem.text
        else:
            raise utils.InvalidMetadataError(f"Metadata file is missing the FIRSTLINETIME and EARLIESTACQTIME xml tags")

        elem = nodeIMAGE.find('MEANSUNEL')
        if elem is None:
            elem = nodeIMAGE.find('SUNEL')
        if elem is not None:
            sunEl = float(elem.text)
        else:
            raise utils.InvalidMetadataError(f"Metadata file is missing the MEANSUNEL and SUNEL xml tags")

        sun_angle = 90.0 - sunEl
        des = calc_earth_sun_dist(datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ"))

        # get BAND tags
        for band in DGbandList:
            nodeBAND = nodeIMD.find(band)
            if nodeBAND is not None:
                elem = nodeBAND.find('ABSCALFACTOR')
                if elem is not None:
                    abscal = float(elem.text)
                else:
                    raise utils.InvalidMetadataError(
                        f"Metadata file is missing the ABSCALFACTOR xml tag")

                elem = nodeBAND.find('EFFECTIVEBANDWIDTH')
                if elem is not None:
                    effbandw = float(elem.text)
                else:
                    raise utils.InvalidMetadataError(
                        f"Metadata file is missing the EFFECTIVEBANDWIDTH xml tag")

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
                        (Esun * math.cos(math.radians(sun_angle)) * effbandw)
            refl_offset = units_factor * (bias * des ** 2 * math.pi) / (Esun * math.cos(math.radians(sun_angle)))

            logger.debug("%s: \n\tabsCalFactor %f\n\teffectiveBandwidth %f\n\tEarth-Sun distance %f"
                            "\n\tEsun %f\n\tSun angle %f\n\tSun elev %f\n\tGain %f\n\tBias %f"
                            "\n\tUnits factor %f\n\tReflectance correction %f\n\tReflectance offset %f"
                            "\n\tRadiance correction %f\n\tRadiance offset %f", satband, abscal, effbandw,
                            des, Esun, sun_angle, sunEl, gain, bias, units_factor, refl_fact, refl_offset,
                            rad_fact, bias)

            if stretch == "rd":
                calibDict[band] = (rad_fact, bias)
            else:
                calibDict[band] = (refl_fact, refl_offset)

    # return correction factor and offset
    return calibDict


def get_ik_calib_dict(metad_etree, metafile, regex, stretch):

    calibDict = {}
    EsunDict = [1930.9, 1854.8, 1556.5, 1156.9, 1375.8]  # B,G,R,N,Pan(TDI13)
    bwList = [71.3, 88.6, 65.8, 95.4, 403]  # B,G,R,N,Pan(TDI13)
    calCoefs1 = [633, 649, 840, 746, 161]  # B,G,R,N,Pan(TDI13) - Pre 2/22/01
    calCoefs2 = [728, 727, 949, 843, 161]  # B,G,R,N,Pan(TDI13) = Post 2/22/01

    metadict = get_ik_metadata(metad_etree, metafile, regex)
    for band in range(0, 5, 1):
        sunElStr = metadict["Sun_Angle_Elevation"]
        sunAngle = float(sunElStr.strip(" degrees"))
        theta = 90.0 - sunAngle
        datestr = metadict["Acquisition_Date_Time"]  # 2011-12-09 18:43 GMT
        d = datetime.strptime(datestr, "%Y-%m-%d %H:%M GMT")
        des = calc_earth_sun_dist(d)

        breakdate = datetime(2001, 2, 22)
        if d < breakdate:
            calCoef = calCoefs1[band]
        else:
            calCoef = calCoefs2[band]

        bw = bwList[band]
        Esun = EsunDict[band]
        rad_fact = 10000.0 / (calCoef * bw)
        refl_fact = (10000.0 * des ** 2 * math.pi) / (calCoef * bw * Esun * math.cos(math.radians(theta)))

        logger.debug("%i: calibration coef %f, Earth-Sun distance %f, Esun %f, sun angle %f, bandwidth %f, "
                        "reflectance factor %f radiance factor %f", band, calCoef, des, Esun, sunAngle,
                        bw, refl_fact, rad_fact)

        if stretch == "rd":
            calibDict[band] = (rad_fact, 0)
        else:
            calibDict[band] = (refl_fact, 0)

    # return correction factor and offset
    return calibDict


def get_ik_metadata(metad_etree, metafile, regex):
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

    metadict = {}
    search_keys = dict(ik2fp)
    siid_node = None

    match = re.search(regex, os.path.basename(metafile.lower()))
    if match:
        siid = match.group('catid')
    else:
        raise RuntimeError(f"Could not match IKONOS image name: {metafile}")

    siid_nodes = metad_etree.findall(r".//Source_Image_ID")
    if siid_nodes is None:
        raise utils.InvalidMetadataError(f"Could not find any Source Image ID fields in metadata: {metafile}")

    for node in siid_nodes:
        if node.attrib["id"] == siid:
            siid_node = node
            break

    if siid_node is None:
        raise utils.InvalidMetadataError(f"Could not locate SIID: {siid} in metadata {metafile}")

    # Now assemble the dict
    for node in siid_node.iter():
        if node.tag in search_keys:
            if node.tag == "Source_Image_ID":
                metadict[node.tag] = node.attrib["id"]
            else:
                metadict[node.tag] = node.text

    return metadict


def get_ge_calib_dict(metad_etree, stretch):

    calibDict = {}
    EsunDict = [196.0, 185.3, 150.5, 103.9, 161.7]

    metadict = get_ge_metadata(metad_etree)
    for band in metadict["gain"].keys():
        sunAngle = float(metadict["firstLineSunElevationAngle"])
        theta = 90.0 - sunAngle
        datestr = metadict["originalFirstLineAcquisitionDateTime"]  # 2009-11-01T01:49:33.685421Z
        des = calc_earth_sun_dist(datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S.%fZ"))
        gain = float(metadict["gain"][band])
        Esun = EsunDict[band - 1]

        logger.debug("Band {}, Sun elev: {}, Earth-Sun distance: {}, Gain: {}, "
                     "Esun: {}".format(band, theta, des, gain, Esun))
        rad_fact = gain * 10  # multiply by 10 to convert from mW/cm2/um to W/m2/um
        refl_fact = (gain * des ** 2 * math.pi) / (Esun * math.cos(math.radians(theta)))

        if stretch == "rd":
            calibDict[band] = (rad_fact, 0)
        else:
            calibDict[band] = (refl_fact, 0)

    # return correction factor and offset
    return calibDict


def get_ge_metadata(metad_etree):
    metadict = {}
    search_keys = ["originalFirstLineAcquisitionDateTime", "firstLineSunElevationAngle"]
    for key in search_keys:
        node = metad_etree.find(".//{}".format(key))
        if node is not None:
            metadict[key] = node.text

    band_keys = ["gain", "offset"]
    for key in band_keys:
        nodes = metad_etree.findall(".//bandSpecificInformation")

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

    return metadict


def xml_to_j2w(jp2p):

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
        j2w.write("{}\n".format(param))
    j2w.close()

