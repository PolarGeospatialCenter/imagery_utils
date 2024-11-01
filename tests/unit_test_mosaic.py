import unittest, os, sys, glob, argparse, math
from osgeo import gdal, ogr
import numpy as np

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

from lib import mosaic


class TestMosaicImageInfo(unittest.TestCase):
    
    def setUp(self):
        self.srcdir = os.path.join(os.path.join(test_dir, 'mosaic', 'ortho'))

    def test_image_info_ge01(self):
        image = 'GE01_20090707163115_297600_5V090707P0002976004A222012202432M_001529596_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir, image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        # self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
        self.assertEqual(image_info.bands, 1)
        self.assertEqual(image_info.datatype, 1)

        mosaic_args = MosaicArgs()
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        image_info.getScore(mosaic_params)
        
        self.assertEqual(image_info.sensor, 'GE01')
        self.assertEqual(image_info.sunel, 45.98)
        self.assertEqual(image_info.ona, 26.86)
        self.assertEqual(image_info.cloudcover, 0.0)
        self.assertEqual(image_info.tdi, 8.0)
        self.assertEqual(image_info.panfactor, 1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 79.1422222)
        
        image_info.get_raster_stats()
        stat_dct = {1: [57.0, 255.0, 171.47750552856309, 42.22407526523467]}
        datapixelcount_dct = {1: 4435601}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)

    def test_image_info_wv01(self):
        image = 'WV01_20080807153945_1020010003A5AC00_08AUG07153945-P1BS-052060421010_01_P011_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir, image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        # self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
        self.assertEqual(image_info.bands, 1)
        self.assertEqual(image_info.datatype, 1)

        mosaic_args = MosaicArgs()
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        image_info.getScore(mosaic_params)
        
        self.assertEqual(image_info.sensor, 'WV01')
        self.assertEqual(image_info.sunel, 39.0)
        self.assertEqual(image_info.ona, 18.5)
        self.assertEqual(image_info.cloudcover, 0.0)
        self.assertEqual(image_info.tdi, 16.0)
        self.assertEqual(image_info.panfactor, 1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 79.2)
        
        image_info.get_raster_stats()
        stat_dct = {1: [6.0, 234.0, 73.77702002, 22.52309144]}
        datapixelcount_dct = {1: 1405893}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)

    # @unittest.skip("skipping")
    def test_image_info_wv02_ndvi(self):
        srcdir = os.path.join(os.path.join(test_dir, 'mosaic', 'ndvi'))
        image = 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413_ndvi.tif'
        image_info = mosaic.ImageInfo(os.path.join(srcdir, image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        # self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
        self.assertEqual(image_info.bands, 1)
        self.assertEqual(image_info.datatype, 6)

        mosaic_args = MosaicArgs()
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        image_info.getScore(mosaic_params)
        
        self.assertEqual(image_info.sensor, 'WV02')
        self.assertEqual(image_info.sunel, 37.7)
        self.assertEqual(image_info.ona, 19.4)
        self.assertEqual(image_info.cloudcover, 0.0)
        self.assertEqual(image_info.tdi, 24.0)
        self.assertEqual(image_info.panfactor, 1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 78.555555555)
        
        image_info.get_raster_stats()
        stat_dct = {1: [-1.0, 1.0,  0.5187682, 0.35876602]}
        datapixelcount_dct = {1: 1208656}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)

    # @unittest.skip("skipping")
    def test_image_info_wv02_ndvi_int16(self):
        srcdir = os.path.join(os.path.join(test_dir, 'mosaic', 'pansh_ndvi'))
        image = 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413_pansh_ndvi.tif'
        image_info = mosaic.ImageInfo(os.path.join(srcdir, image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        # self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
        self.assertEqual(image_info.bands, 1)
        self.assertEqual(image_info.datatype, 3)

        mosaic_args = MosaicArgs()
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        image_info.getScore(mosaic_params)
        
        self.assertEqual(image_info.sensor, 'WV02')
        self.assertEqual(image_info.sunel, 37.7)
        self.assertEqual(image_info.ona, 19.4)
        self.assertEqual(image_info.cloudcover, 0.0)
        self.assertEqual(image_info.tdi, 24.0)
        self.assertEqual(image_info.panfactor, 1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 78.555555555)
        
        image_info.get_raster_stats()
        stat_dct = {1: [-1000.0, 1000.0, 549.7191938, 308.80771976]}
        datapixelcount_dct = {1: 1206259}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)  
    
    def test_image_info_multispectral_dg_ge01(self):
        image = 'GE01_20130728161916_1050410002608900_13JUL28161916-M1BS-054448357040_01_P002_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir, image), 'IMAGE')
        self.maxDiff = None
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        # self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
        self.assertEqual(image_info.bands, 4)
        self.assertEqual(image_info.datatype, 1)

        mosaic_args = MosaicArgs()
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        image_info.getScore(mosaic_params)
        
        self.assertEqual(image_info.sensor, 'GE01')
        self.assertEqual(image_info.sunel, 42.2)
        self.assertEqual(image_info.ona, 25.0)
        self.assertEqual(image_info.cloudcover, 0.0)
        self.assertEqual(image_info.tdi, 6.0)
        self.assertEqual(image_info.panfactor, 1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 78.462222222)
        
        image_info.get_raster_stats()
        stat_dct = {
            1: [1.0, 245.0, 89.28827106, 18.75882356],
            2: [2.0, 245.0, 72.48547016, 21.73902804],
            3: [1.0, 251.0, 58.33183442, 21.82633595],
            4: [1.0, 235.0, 61.06978454, 26.274778]
        }
        datapixelcount_dct = {1: 1152457, 2: 1152456, 3: 1152394, 4: 1146271}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct) 
    
    def test_image_info_wv01_with_tday_and_exposure(self):
        image = 'WV01_20080807153945_1020010003A5AC00_08AUG07153945-P1BS-052060421010_01_P011_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir, image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        #self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
        self.assertEqual(image_info.bands, 1)
        self.assertEqual(image_info.datatype, 1)

        mosaic_args = MosaicArgs()
        mosaic_args.tday = '09-01'
        mosaic_args.use_exposure = True
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        
        self.assertEqual(mosaic_params.m, 9)
        self.assertEqual(mosaic_params.d, 1)
        
        image_info.getScore(mosaic_params)
        self.assertEqual(image_info.sensor, 'WV01')
        self.assertEqual(image_info.sunel, 39.0)
        self.assertEqual(image_info.ona, 18.5)
        self.assertEqual(image_info.cloudcover, 0.0)
        self.assertEqual(image_info.tdi, 16.0)
        self.assertEqual(image_info.panfactor, 1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, 24)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 86.0924408)

    def test_image_info_wv01_with_tyear(self):
        image = 'WV01_20080807153945_1020010003A5AC00_08AUG07153945-P1BS-052060421010_01_P011_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir, image), 'IMAGE')

        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        #self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
        self.assertEqual(image_info.bands, 1)
        self.assertEqual(image_info.datatype, 1)

        mosaic_args = MosaicArgs()
        mosaic_args.tyear = 2008
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)

        image_info.getScore(mosaic_params)
        self.assertEqual(image_info.sensor, 'WV01')
        self.assertEqual(image_info.sunel, 39.0)
        self.assertEqual(image_info.ona, 18.5)
        self.assertEqual(image_info.cloudcover, 0.0)
        self.assertEqual(image_info.tdi, 16.0)
        self.assertEqual(image_info.panfactor, 1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, 0)
        self.assertAlmostEqual(image_info.score, 134.2)

    def test_image_info_wv02_with_cc_max(self):
        image = 'WV02_20110504155551_103001000BA45E00_11MAY04155551-P1BS-500085264180_01_P002_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir, image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        #self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
        self.assertEqual(image_info.bands, 1)
        self.assertEqual(image_info.datatype, 1)

        mosaic_args = MosaicArgs()
        mosaic_args.max_cc = 0.20
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        image_info.getScore(mosaic_params)
        
        self.assertEqual(image_info.sensor, 'WV02')
        self.assertEqual(image_info.sunel, 39.2)
        self.assertEqual(image_info.ona, 19.0)
        self.assertEqual(image_info.cloudcover, 0.29)
        self.assertEqual(image_info.tdi, 48.0)
        self.assertEqual(image_info.panfactor, 1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, -1)
        
        image_info.get_raster_stats()
        stat_dct = {1: [1.0, 239.0, 232.17920063, 11.26401958]}
        datapixelcount_dct = {1: 1155208}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)

    def test_filter_images(self):
        image_list = glob.glob(os.path.join(self.srcdir, '*.tif'))
        imginfo_list = [mosaic.ImageInfo(image, "IMAGE") for image in image_list]
        filter_list = [iinfo.srcfn for iinfo in imginfo_list]
        self.assertIn('WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u08mr3413.tif',
                      filter_list)
        self.assertIn('GE01_20130728161916_1050410002608900_13JUL28161916-P1BS-054448357040_01_P002_u16rf3413.tif',
                      filter_list)
        self.assertIn('WV02_20131123162834_10300100293C3400_13NOV23162834-P1BS-500408660030_01_P005_u08mr3413.tif',
                      filter_list)

        mosaic_args = MosaicArgs()
        mosaic_args.extent = [-820000.0, -800000.0, -2420000.0, -2400000.0]
        mosaic_args.tilesize = [20000, 20000]
        mosaic_args.bands = 1
        mosaic_params = mosaic.getMosaicParameters(imginfo_list[0], mosaic_args)
        imginfo_list2 = mosaic.filterMatchingImages(imginfo_list, mosaic_params)
        filter_list = [iinfo.srcfn for iinfo in imginfo_list2]
        
        self.assertEqual(len(imginfo_list2), 8)
        self.assertNotIn('WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u08mr3413.tif',
                         filter_list)
        self.assertNotIn('GE01_20130728161916_1050410002608900_13JUL28161916-P1BS-054448357040_01_P002_u16rf3413.tif',
                         filter_list)
        
        imginfo_list3 = mosaic.filter_images_by_geometry(imginfo_list2, mosaic_params)
        filter_list = [iinfo.srcfn for iinfo in imginfo_list3]
        self.assertEqual(len(imginfo_list3), 7)
        self.assertNotIn('WV02_20131123162834_10300100293C3400_13NOV23162834-P1BS-500408660030_01_P005_u08mr3413.tif',
                         filter_list)


def img_as_array(img_file, band=1):
    """
    Open image as numpy array using GDAL.

    :param img_file: <str> path to image file
    :param band: <int> band to be opened (default=1)
    :return: <numpy.ndarray> image as 2d array
    """
    new_img = gdal.Open(img_file, gdal.GA_ReadOnly)
    band = new_img.GetRasterBand(band)
    new_arr = band.ReadAsArray()

    return new_arr


class TestMosaicDataValues(unittest.TestCase):

    def setUp(self):
        image_new = os.path.join(test_dir, 'output')
        image_old = os.path.join(test_dir, 'output_static')

        # find images
        self.new_imgs = sorted(glob.glob(os.path.join(image_new, "*.tif")))
        self.old_imgs = sorted(glob.glob(os.path.join(image_old, "*.tif")))

        # if no images found, explain why
        if not self.new_imgs:
            raise Exception("No images in self.new_imgs; run 'func_test_mosaic.py' to generate images")
        if not self.old_imgs:
            raise Exception("No images in self.old_imgs; create or populate 'output_static' directory with mosaics "
                            "using previous version of the codebase")

    # one
    def test_mosaic_one_equivalence(self):
        # select images
        target_image = 'testmosaic1_1_1.tif'
        new = [i for i in self.new_imgs if target_image in i][0]
        old = [i for i in self.old_imgs if target_image in i][0]

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # two
    def test_mosaic_two_equivalence(self):
        # select images
        target_image = 'testmosaic2_1_1.tif'
        new = [i for i in self.new_imgs if target_image in i][0]
        old = [i for i in self.old_imgs if target_image in i][0]

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # three
    def test_mosaic_three_equivalence(self):
        # select images
        target_image = 'testmosaic3_1_1.tif'
        new = [i for i in self.new_imgs if target_image in i][0]
        old = [i for i in self.old_imgs if target_image in i][0]

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # four
    def test_mosaic_four_equivalence(self):
        # select images
        target_image = 'testmosaic4_1_1.tif'
        new = [i for i in self.new_imgs if target_image in i][0]
        old = [i for i in self.old_imgs if target_image in i][0]

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))


# same function used to create 'cutlines' and 'components', hence only 'cutlines' being verified here
class TestMosaicCutlinesShp(unittest.TestCase):


    def setUp(self):
        shp_new = os.path.join(test_dir, 'output')
        shp_old = os.path.join(test_dir, 'output_static')

        # find images
        self.new_shps = sorted(glob.glob(os.path.join(shp_new, "*cutlines.shp")))
        self.old_shps = sorted(glob.glob(os.path.join(shp_old, "*cutlines.shp")))

        # if no images found, explain why
        if not self.new_shps:
            raise Exception("No cutline shapefiles in self.new_shps; run 'func_test_mosaic.py' to generate shapefiles")
        if not self.old_shps:
            raise Exception("No cutline shapefiles in self.old_shps; create or populate 'output_static' directory with "
                  "shapefiles using previous version of the codebase")

    def test_cutline_one_equivalence(self):

        # select shapefiles
        target_image = 'testmosaic1_cutlines.shp'
        new = [i for i in self.new_shps if target_image in i][0]
        old = [i for i in self.old_shps if target_image in i][0]

        # get layers
        new_shp = ogr.Open(new)
        new_lyr = new_shp.GetLayer()

        old_shp = ogr.Open(old)
        old_lyr = old_shp.GetLayer()

        # run tests
        self.assertEqual(new_lyr.GetExtent(), old_lyr.GetExtent())  # layer extent tuples
        self.assertEqual(new_lyr.GetSpatialRef().ExportToWkt(), old_lyr.GetSpatialRef().ExportToWkt())  # spatial ref
        self.assertEqual(new_lyr.GetGeomType(), old_lyr.GetGeomType())  # geom type
        self.assertEqual(new_lyr.GetLayerDefn().GetGeomFieldCount(), old_lyr.GetLayerDefn().GetGeomFieldCount())  # field ct
        self.assertEqual(new_lyr.GetFeatureCount(), old_lyr.GetFeatureCount())  # feature ct
        self.assertEqual(new_lyr.GetFeature(0).ExportToJson(), old_lyr.GetFeature(0).ExportToJson()) # first feature's data + geom


    def test_cutline_two_equivalence(self):

        # select shapefiles
        target_image = 'testmosaic2_cutlines.shp'
        new = [i for i in self.new_shps if target_image in i][0]
        old = [i for i in self.old_shps if target_image in i][0]

        # get layers
        new_shp = ogr.Open(new)
        new_lyr = new_shp.GetLayer()

        old_shp = ogr.Open(old)
        old_lyr = old_shp.GetLayer()

        # run tests
        self.assertEqual(new_lyr.GetExtent(), old_lyr.GetExtent())  # layer extent tuples
        self.assertEqual(new_lyr.GetSpatialRef().ExportToWkt(), old_lyr.GetSpatialRef().ExportToWkt())  # spatial ref
        self.assertEqual(new_lyr.GetGeomType(), old_lyr.GetGeomType())  # geom type
        self.assertEqual(new_lyr.GetLayerDefn().GetGeomFieldCount(), old_lyr.GetLayerDefn().GetGeomFieldCount())  # field ct
        self.assertEqual(new_lyr.GetFeatureCount(), old_lyr.GetFeatureCount())  # feature ct
        self.assertEqual(new_lyr.GetFeature(0).ExportToJson(), old_lyr.GetFeature(0).ExportToJson()) # first feature's data + geom


    def test_cutline_three_equivalence(self):

        # select shapefiles
        target_image = 'testmosaic3_cutlines.shp'
        new = [i for i in self.new_shps if target_image in i][0]
        old = [i for i in self.old_shps if target_image in i][0]

        # get layers
        new_shp = ogr.Open(new)
        new_lyr = new_shp.GetLayer()

        old_shp = ogr.Open(old)
        old_lyr = old_shp.GetLayer()

        # run tests
        self.assertEqual(new_lyr.GetExtent(), old_lyr.GetExtent())  # layer extent tuples
        self.assertEqual(new_lyr.GetSpatialRef().ExportToWkt(), old_lyr.GetSpatialRef().ExportToWkt())  # spatial ref
        self.assertEqual(new_lyr.GetGeomType(), old_lyr.GetGeomType())  # geom type
        self.assertEqual(new_lyr.GetLayerDefn().GetGeomFieldCount(), old_lyr.GetLayerDefn().GetGeomFieldCount())  # field ct
        self.assertEqual(new_lyr.GetFeatureCount(), old_lyr.GetFeatureCount())  # feature ct
        self.assertEqual(new_lyr.GetFeature(0).ExportToJson(), old_lyr.GetFeature(0).ExportToJson()) # first feature's data + geom


    def test_cutline_four_equivalence(self):

        # select shapefiles
        target_image = 'testmosaic4_cutlines.shp'
        new = [i for i in self.new_shps if target_image in i][0]
        old = [i for i in self.old_shps if target_image in i][0]

        # get layers
        new_shp = ogr.Open(new)
        new_lyr = new_shp.GetLayer()

        old_shp = ogr.Open(old)
        old_lyr = old_shp.GetLayer()

        # run tests
        self.assertEqual(new_lyr.GetExtent(), old_lyr.GetExtent())  # layer extent tuples
        self.assertEqual(new_lyr.GetSpatialRef().ExportToWkt(), old_lyr.GetSpatialRef().ExportToWkt())  # spatial ref
        self.assertEqual(new_lyr.GetGeomType(), old_lyr.GetGeomType())  # geom type
        self.assertEqual(new_lyr.GetLayerDefn().GetGeomFieldCount(), old_lyr.GetLayerDefn().GetGeomFieldCount())  # field ct
        self.assertEqual(new_lyr.GetFeatureCount(), old_lyr.GetFeatureCount())  # feature ct
        self.assertEqual(new_lyr.GetFeature(0).ExportToJson(), old_lyr.GetFeature(0).ExportToJson()) # first feature's data + geom


class TestMosaicTilesShp(unittest.TestCase):

    def setUp(self):
        shp_new = os.path.join(test_dir, 'output')
        shp_old = os.path.join(test_dir, 'output_static')

        # find images
        self.new_shps = sorted(glob.glob(os.path.join(shp_new, "*tiles.shp")))
        self.old_shps = sorted(glob.glob(os.path.join(shp_old, "*tiles.shp")))

        # if no images found, explain why
        if not self.new_shps:
            raise Exception("No tiles shapefiles in self.new_shps; run 'func_test_mosaic.py' to generate shapefiles")
        if not self.old_shps:
            raise Exception("No tiles shapefiles in self.old_shps; create or populate 'output_static' directory with "
                            "shapefiles using previous version of the codebase")

    def test_tile_one_equivalence(self):

        # select shapefiles
        target_image = 'testmosaic1_tiles.shp'
        new = [i for i in self.new_shps if target_image in i][0]
        old = [i for i in self.old_shps if target_image in i][0]

        # get layers
        new_shp = ogr.Open(new)
        new_lyr = new_shp.GetLayer()

        old_shp = ogr.Open(old)
        old_lyr = old_shp.GetLayer()

        # run tests
        self.assertEqual(new_lyr.GetExtent(), old_lyr.GetExtent())  # layer extent tuples
        self.assertEqual(new_lyr.GetSpatialRef().ExportToWkt(), old_lyr.GetSpatialRef().ExportToWkt())  # spatial ref
        self.assertEqual(new_lyr.GetGeomType(), old_lyr.GetGeomType())  # geom type
        self.assertEqual(new_lyr.GetLayerDefn().GetGeomFieldCount(), old_lyr.GetLayerDefn().GetGeomFieldCount())  # field ct
        self.assertEqual(new_lyr.GetFeatureCount(), old_lyr.GetFeatureCount())  # feature ct
        self.assertEqual(new_lyr.GetFeature(0).ExportToJson(), old_lyr.GetFeature(0).ExportToJson()) # first feature's data + geom


    def test_tile_two_equivalence(self):

        # select shapefiles
        target_image = 'testmosaic2_tiles.shp'
        new = [i for i in self.new_shps if target_image in i][0]
        old = [i for i in self.old_shps if target_image in i][0]

        # get layers
        new_shp = ogr.Open(new)
        new_lyr = new_shp.GetLayer()

        old_shp = ogr.Open(old)
        old_lyr = old_shp.GetLayer()

        # run tests
        self.assertEqual(new_lyr.GetExtent(), old_lyr.GetExtent())  # layer extent tuples
        self.assertEqual(new_lyr.GetSpatialRef().ExportToWkt(), old_lyr.GetSpatialRef().ExportToWkt())  # spatial ref
        self.assertEqual(new_lyr.GetGeomType(), old_lyr.GetGeomType())  # geom type
        self.assertEqual(new_lyr.GetLayerDefn().GetGeomFieldCount(), old_lyr.GetLayerDefn().GetGeomFieldCount())  # field ct
        self.assertEqual(new_lyr.GetFeatureCount(), old_lyr.GetFeatureCount())  # feature ct
        self.assertEqual(new_lyr.GetFeature(0).ExportToJson(), old_lyr.GetFeature(0).ExportToJson()) # first feature's data + geom


    def test_tile_three_equivalence(self):

        # select shapefiles
        target_image = 'testmosaic3_tiles.shp'
        new = [i for i in self.new_shps if target_image in i][0]
        old = [i for i in self.old_shps if target_image in i][0]

        # get layers
        new_shp = ogr.Open(new)
        new_lyr = new_shp.GetLayer()

        old_shp = ogr.Open(old)
        old_lyr = old_shp.GetLayer()

        # run tests
        self.assertEqual(new_lyr.GetExtent(), old_lyr.GetExtent())  # layer extent tuples
        self.assertEqual(new_lyr.GetSpatialRef().ExportToWkt(), old_lyr.GetSpatialRef().ExportToWkt())  # spatial ref
        self.assertEqual(new_lyr.GetGeomType(), old_lyr.GetGeomType())  # geom type
        self.assertEqual(new_lyr.GetLayerDefn().GetGeomFieldCount(), old_lyr.GetLayerDefn().GetGeomFieldCount())  # field ct
        self.assertEqual(new_lyr.GetFeatureCount(), old_lyr.GetFeatureCount())  # feature ct
        self.assertEqual(new_lyr.GetFeature(0).ExportToJson(), old_lyr.GetFeature(0).ExportToJson()) # first feature's data + geom


    def test_tile_four_equivalence(self):

        # select shapefiles
        target_image = 'testmosaic4_tiles.shp'
        new = [i for i in self.new_shps if target_image in i][0]
        old = [i for i in self.old_shps if target_image in i][0]

        # get layers
        new_shp = ogr.Open(new)
        new_lyr = new_shp.GetLayer()

        old_shp = ogr.Open(old)
        old_lyr = old_shp.GetLayer()

        # run tests
        self.assertEqual(new_lyr.GetExtent(), old_lyr.GetExtent())  # layer extent tuples
        self.assertEqual(new_lyr.GetSpatialRef().ExportToWkt(), old_lyr.GetSpatialRef().ExportToWkt())  # spatial ref
        self.assertEqual(new_lyr.GetGeomType(), old_lyr.GetGeomType())  # geom type
        self.assertEqual(new_lyr.GetLayerDefn().GetGeomFieldCount(), old_lyr.GetLayerDefn().GetGeomFieldCount())  # field ct
        self.assertEqual(new_lyr.GetFeatureCount(), old_lyr.GetFeatureCount())  # feature ct
        self.assertEqual(new_lyr.GetFeature(0).ExportToJson(), old_lyr.GetFeature(0).ExportToJson()) # first feature's data + geom


class TestMiscFunctions(unittest.TestCase):
    def setUp(self):
        self.srcdir = os.path.join(test_dir, 'metadata_files')
        self.srcfn = 'QB02_20021009211710_101001000153C800_02OCT09211710-M2AS_R1C1-052075481010_01_P001.xml'
        self.srcfile = os.path.join(self.srcdir, self.srcfn)
        #print(self.srcfile)
        self.dstdir = os.path.join(test_dir, 'output')
        self.dstfile_xml = os.path.join(self.dstdir, self.srcfn)
        self.dem = os.path.join(test_dir, 'dem', 'ramp_lowres.tif')

        self.resolution = None
        self.xres = 0.5
        self.yres = 0.5
        self.bands = 1
        self.proj = 'PROJCS["WGS_1984_Stereographic_South_Pole",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",-71],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'
        self.datatype = 3
        self.use_exposure = True
        self.tday = None
        self.tyear = None
        self.extent = [150000, -1400000, 420000, -1200000]
        self.tilesize = [20000, 20000]
        self.max_cc = 0.6
        self.force_pan_to_multi = False
        self.include_all_ms = False
        self.median_remove = False
        self.min_contribution_area = 20000000

        self.imginfo_list = mosaic.ImageInfo(self.dem, 'IMAGE')

        poly_wkt = 'POLYGON (( {} {}, {} {}, {} {}, {} {}, {} {} ))'.format(self.extent[0], self.extent[1],
                                                                            self.extent[0], self.extent[3],
                                                                            self.extent[2], self.extent[3],
                                                                            self.extent[2], self.extent[1],
                                                                            self.extent[0], self.extent[1])
        self.extent_geom = ogr.CreateGeometryFromWkt(poly_wkt)
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    def test_get_exact_trimmed_geom(self):
        xs_expected = [2502000.0, 2868000.0, 2868000.0, -2868000.0, -2868000.0, -2501000.0]
        ys_expected = [2457900.0, 457900.0, -1542100.0, -1542100.0, 457900.0, 2457900.0]
        geom, xs, ys = mosaic.GetExactTrimmedGeom(self.dem, step=10000)
        self.assertEqual(xs, xs_expected)
        self.assertEqual(ys, ys_expected)

    '''
    NOTE: findVertices() is not used in the codebase, and will not be tested here
    '''

    def test_pl2xy(self):
        # test using random, but plausible values
        gtf = [0, 50, 10, 1000, 5, 50]
        p_var = 10
        l_var = 10
        x, y = mosaic.pl2xy(gtf, None, p_var, l_var)
        self.assertEqual(x, 500)
        self.assertEqual(y, 1525.0)

        # same test as above, but negative x coordinate (gtf[0])
        gtf = [-50, 50, 10, 1000, 5, 50]
        p_var = 10
        l_var = 10
        x, y = mosaic.pl2xy(gtf, None, p_var, l_var)
        self.assertEqual(x, 450)
        self.assertEqual(y, 1525.0)

    def test_drange(self):
        self.assertEqual(list(mosaic.drange(0, 5, 1)), [0, 1, 2, 3, 4])
        self.assertEqual(list(mosaic.drange(5, 0, 1)), [])

    def test_buffernum(self):
        # note: buffernum() gives strange value if 'num' is negative (buffernum(-5, 3) returns '0-5')
        self.assertEqual(mosaic.buffernum(10, 5), '00010')
        self.assertEqual(mosaic.buffernum(5, 2), '05')

    def test_copyall(self):
        # make sure basic file copying works
        mosaic.copyall(self.srcfile, self.dstdir)
        self.assertTrue(os.path.isfile(self.dstfile_xml))

        # should return AttributeError
        with self.assertRaises(TypeError) as cm:
            mosaic.copyall(None, None)

    def tearDown(self):
        if os.path.isfile(self.dstfile_xml):
            os.remove(self.dstfile_xml)


class MosaicArgs(object):
    def __init__(self):
        self.resolution = None
        self.bands = None
        self.use_exposure = False
        self.tday = None
        self.tyear = None
        self.extent = None
        self.tilesize = None
        self.max_cc = 0.5
        self.force_pan_to_multi = False
        self.include_all_ms = False
        self.median_remove = False
        

if __name__ == '__main__':
    
    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Test imagery_utils mosaic package"
        )

    parser.add_argument('--testdata', help="test data directory (default is testdata folder within script directory)")

    #### Parse Arguments
    args = parser.parse_args()
    global test_dir
    
    if args.testdata:
        test_dir = os.path.abspath(args.testdata)
    else:
        test_dir = os.path.join(script_dir, 'testdata')
    
    if not os.path.isdir(test_dir):
        parser.error("Test data folder does not exist: {}".format(test_dir))
        
    test_cases = [
        TestMosaicImageInfo,
        # TestMosaicDataValues, # TODO: output_static folder missing from test data
        # TestMosaicCutlinesShp, # TODO: output_static folder missing from test data
        # TestMosaicTilesShp, # TODO: output_static folder missing from test data
        TestMiscFunctions
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
