import unittest, os, sys, glob, shutil, argparse, logging, math
import gdal, ogr, osr, gdalconst

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

from lib import mosaic

logger = logging.getLogger("logger")
# lso = logging.StreamHandler()
# lso.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
# lso.setFormatter(formatter)
# logger.addHandler(lso)


class TestMosaicImageInfo(unittest.TestCase):
    
    def setUp(self):
        self.srcdir = os.path.join(os.path.join(test_dir,'mosaic','ortho'))

    def test_image_info_ge01(self):
        image = 'GE01_20090707163115_297600_5V090707P0002976004A222012202432M_001529596_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir,image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        self.assertEqual(image_info.panfactor ,1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 79.1422222)
        
        image_info.get_raster_stats()
        stat_dct = {1: [40.0, 200.0, 135.82811420290182, 33.54100534555833]}
        datapixelcount_dct = {1: 4435509}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)

    def test_image_info_wv01(self):
        image = 'WV01_20080807153945_1020010003A5AC00_08AUG07153945-P1BS-052060421010_01_P011_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir,image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        self.assertEqual(image_info.panfactor ,1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 79.2)
        
        image_info.get_raster_stats()
        stat_dct = {1: [24.0, 192.0, 60.0042506228806, 18.321626067645923]}
        datapixelcount_dct = {1: 1403559}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)
    
    def test_image_info_wv02_ndvi(self):
        srcdir = os.path.join(os.path.join(test_dir,'mosaic','ndvi'))
        image = 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413_ndvi.tif'
        image_info = mosaic.ImageInfo(os.path.join(srcdir,image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 64.0)
        self.assertEqual(image_info.yres, 64.0)
        self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        self.assertEqual(image_info.panfactor ,1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 78.555555555)
        
        image_info.get_raster_stats()
        datapixelcount_dct = {1: 102448}
        for i in range(len(image_info.stat_dct[1])):
            self.assertTrue(math.isnan(image_info.stat_dct[1][i]))
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)
        self.assertTrue(math.isnan(image_info.median[1]))
             
    def test_image_info_wv02_ndvi_int16(self):
        srcdir = os.path.join(os.path.join(test_dir,'mosaic','pansh_ndvi'))
        image = 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413_pansh_ndvi.tif'
        image_info = mosaic.ImageInfo(os.path.join(srcdir,image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        self.assertEqual(image_info.panfactor ,1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 78.555555555)
        
        image_info.get_raster_stats()
        stat_dct = {1: [-991.0, 996.0, 536.7883746333843, 250.83677803422484]}
        datapixelcount_dct = {1: 1202904}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)  
    
    def test_image_info_multispectral_dg_ge01(self):
        image = 'GE01_20130728161916_1050410002608900_13JUL28161916-M1BS-054448357040_01_P002_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir,image), 'IMAGE')
        self.maxDiff = None
        
        self.assertEqual(image_info.xres, 32.0)
        self.assertEqual(image_info.yres, 32.0)
        self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        self.assertEqual(image_info.panfactor ,1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 78.462222222)
        
        image_info.get_raster_stats()
        #print image_info.stat_dct
        stat_dct = {
            1: [44.0, 180.0, 73.05224947061919, 13.760346025453206],
            2: [28.0, 182.0, 62.26321535738713, 16.410250286247617],
            3: [9.0, 187.0, 51.73902037892776, 17.01731873722769],
            4: [7.0, 178.0, 57.128591347040505, 20.162025784223044]
        }
        datapixelcount_dct = {1: 287601, 2: 287601, 3: 287601, 4: 287601}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct) 
    
    def test_image_info_wv01_with_tday_and_exposure(self):
        image = 'WV01_20080807153945_1020010003A5AC00_08AUG07153945-P1BS-052060421010_01_P011_u08mr3413.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir,image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir,image), 'IMAGE')

        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir,image), 'IMAGE')
        
        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        self.assertEqual(image_info.proj, 'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        self.assertEqual(image_info.panfactor ,1)
        #self.assertEqual(image_info.exposure_factor, 0)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, -1)
        
        image_info.get_raster_stats()
        stat_dct =  {1: [80.0, 191.0, 185.7102777536714, 6.965814755751974]}
        datapixelcount_dct =  {1: 1152100}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)

    def test_filter_images(self):
        image_list = glob.glob(os.path.join(self.srcdir,'*.tif'))
        imginfo_list = [mosaic.ImageInfo(image,"IMAGE") for image in image_list]
        filter_list = [iinfo.srcfn for iinfo in imginfo_list]
        self.assertIn('WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u08rf3413.tif', filter_list)
        self.assertIn('GE01_20130728161916_1050410002608900_13JUL28161916-P1BS-054448357040_01_P002_u16rf3413.tif', filter_list)
        self.assertIn('WV02_20131123162834_10300100293C3400_13NOV23162834-P1BS-500408660030_01_P005_u08mr3413.tif', filter_list)

        mosaic_args = MosaicArgs()
        mosaic_args.extent = [-820000.0, -800000.0, -2420000.0, -2400000.0]
        mosaic_args.tilesize = [20000, 20000]
        mosaic_args.bands = 1
        mosaic_params = mosaic.getMosaicParameters(imginfo_list[0], mosaic_args)
        imginfo_list2 = mosaic.filterMatchingImages(imginfo_list, mosaic_params)
        filter_list = [iinfo.srcfn for iinfo in imginfo_list2]
        
        self.assertEqual(len(imginfo_list2), 8)
        self.assertNotIn('WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u08rf3413.tif', filter_list)
        self.assertNotIn('GE01_20130728161916_1050410002608900_13JUL28161916-P1BS-054448357040_01_P002_u16rf3413.tif', filter_list)
        
        imginfo_list3 = mosaic.filter_images_by_geometry(imginfo_list2, mosaic_params)
        filter_list = [iinfo.srcfn for iinfo in imginfo_list3]
        self.assertEqual(len(imginfo_list3), 7)
        self.assertNotIn('WV02_20131123162834_10300100293C3400_13NOV23162834-P1BS-500408660030_01_P005_u08mr3413.tif', filter_list)

 
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
        test_dir = os.path.join(script_dir,'testdata')
    
    if not os.path.isdir(test_dir):
        parser.error("Test data folder does not exist: %s" %test_dir)
        
    test_cases = [
        TestMosaicImageInfo
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)