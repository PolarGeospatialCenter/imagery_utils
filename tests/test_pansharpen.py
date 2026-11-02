import shutil
import unittest, os, subprocess
import sys
from osgeo import gdal, gdalconst

__test_dir__ = os.path.dirname(os.path.abspath(__file__))
__app_dir__ = os.path.dirname(__test_dir__)
sys.path.append(__app_dir__)
testdata_dir = os.path.join(__test_dir__, 'testdata')

from lib import mosaic


class TestPanshFunc(unittest.TestCase):

    def setUp(self):

        self.scriptpath = os.path.join(__app_dir__, "pgc_pansharpen.py")
        self.srcdir = os.path.join(testdata_dir, 'pansharpen', 'src')
        self.dstdir = os.path.join(__test_dir__, 'tmp_output')
        os.makedirs(self.dstdir, exist_ok=True)

    def test_pansharpen_4band(self):

        src_fn = 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008'
        src = os.path.join(self.srcdir, src_fn+'.ntf')
        cmd = 'python {} {} {} -r 10 --skip-cmd-txt -p 3413'.format(
            self.scriptpath,
            src,
            self.dstdir,
        )

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        print(so)
        print(se)

        # make sure output data exist
        dstfp = os.path.join(self.dstdir, src_fn+'_u08rf3413_pansh.tif')
        dstfp_xml = os.path.join(self.dstdir, src_fn+'_u08rf3413_pansh.xml')

        self.assertTrue(os.path.isfile(dstfp))
        self.assertTrue(os.path.isfile(dstfp_xml))

        # verify data type
        ds = gdal.Open(dstfp, gdal.GA_ReadOnly)
        dt = ds.GetRasterBand(1).DataType
        self.assertEqual(dt, 1)
        ndval = ds.GetRasterBand(1).GetNoDataValue()
        self.assertEqual(ndval, 255)
        ds = None

        image_info = mosaic.ImageInfo(dstfp, 'IMAGE')
        self.assertEqual(image_info.bands, 4)
        self.assertEqual(image_info.datatype, 1)

        mosaic_args = MosaicArgs()
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        image_info.getScore(mosaic_params)

        self.assertEqual(image_info.sensor, 'WV02')
        self.assertEqual(image_info.sunel, 37.8)
        self.assertEqual(image_info.ona, 23.5)
        self.assertEqual(image_info.cloudcover, 0.003)
        self.assertEqual(image_info.tdi, 18.0)
        self.assertEqual(image_info.panfactor, 1)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 77.34933333333333)

        image_info.get_raster_stats()
        stat_dct = {1: [0.0, 254.0, 22.50, 7.486],
                    2: [0.0, 187.0, 17.30, 6.303],
                    3: [0.0, 149.0, 10.87, 6.708],
                    4: [0.0, 254.0, 32.92, 26.554]}
        datapixelcount_dct = {1: 3092783, 2: 3092783, 3: 3092783, 4: 3092783}
        datapixelcount_threshold = 0.0000001 # percentage
        minmax_threshold = 0
        for i in range(1,len(image_info.stat_dct)+1):# check stats are similar
            # Check min and max values
            for j in range(2):
                self.assertTrue(abs(image_info.stat_dct[i][j] - stat_dct[i][j]) <= minmax_threshold,
                                f'found:{image_info.stat_dct[i][j]}, expected {stat_dct[i][j]} +-{minmax_threshold} ')
            # Check mean and stddev
            for j in range(2,4):
                self.assertAlmostEqual(image_info.stat_dct[i][j], stat_dct[i][j], 1,
                                       f'found:{image_info.stat_dct[i][j]}, expected {stat_dct[i][j]} within 1 decimal places')
            # Check data pixel count
            self.assertTrue(abs(image_info.datapixelcount_dct[i] - datapixelcount_dct[i]) / float(datapixelcount_dct[i]) <= datapixelcount_threshold,
                            f'found:{image_info.datapixelcount_dct[i]}, expected {datapixelcount_dct[i]} +-{datapixelcount_threshold/100} percent')

            # Check custom weights were not used
            self.assertNotIn('Using custom 8-band weights for pansharpening', so.decode('utf-8'))
            self.assertIn('Custom pansharpening weights not available for 3 and 4-band imagery: each band will be weighted equally', so.decode('utf-8'))

    def test_pansharpen_8band(self):

        src_fn = "WV03_20260603215825_10400100BA300C00_26JUN03215825-M1BS-510422458010_02_P003"
        src = os.path.join(self.srcdir, src_fn + ".tif")
        cmd = 'python {} {} {} -c mr -r 10 --skip-cmd-txt -p 3413'.format(
            self.scriptpath,
            src,
            self.dstdir,
        )

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        print(so)
        print(se)

        # make sure output data exist
        dstfp = os.path.join(self.dstdir, src_fn + "_u08mr3413_pansh.tif")
        dstfp_xml = os.path.join(self.dstdir, src_fn + "_u08mr3413_pansh.xml")
        self.assertTrue(os.path.isfile(dstfp))
        self.assertTrue(os.path.isfile(dstfp_xml))

        # verify data type
        ds = gdal.Open(dstfp, gdal.GA_ReadOnly)
        dt = ds.GetRasterBand(1).DataType
        self.assertEqual(dt, 1)
        ndval = ds.GetRasterBand(1).GetNoDataValue()
        self.assertEqual(ndval, 255)
        ds = None

        image_info = mosaic.ImageInfo(dstfp, 'IMAGE')
        # self.assertAlmostEqual(image_info.xres, 0.564193804791, 3)
        # self.assertAlmostEqual(image_info.yres, 0.560335413717, 3)
        self.assertEqual(image_info.bands, 8)
        self.assertEqual(image_info.datatype, 1)

        mosaic_args = MosaicArgs()
        mosaic_params = mosaic.getMosaicParameters(image_info, mosaic_args)
        image_info.getScore(mosaic_params)

        self.assertEqual(image_info.sensor, 'WV03')
        self.assertEqual(image_info.sunel, 51.7)
        self.assertEqual(image_info.ona, 29.9)
        self.assertEqual(image_info.cloudcover, 0.000)
        self.assertEqual(image_info.tdi, 14.0)
        self.assertEqual(image_info.panfactor, 1)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 80.111, 2)

        image_info.get_raster_stats()
        stat_dct = {
            1: [29.0, 254.0, 114.70, 14.584],
            2: [23.0, 254.0, 97.62, 13.642],
            3: [20.0, 233.0, 80.77,12.560],
            4: [8.0, 222.0, 69.19, 11.529],
            5: [12.0, 229.0, 65.13, 12.123],
            6: [0.0, 244.0, 49.02, 20.787],
            7: [0.0, 254.0, 35.39, 36.005],
            8: [0.0, 254.0, 23.74, 36.214],
        }
        datapixelcount = 2196619
        datapixelcount_threshold = 0.0000000 # percentage
        minmax_threshold = 0
        for i in range(1,len(image_info.stat_dct)+1):# check stats are similar
            # Check min and max values
            for j in range(2):
                self.assertTrue(abs(image_info.stat_dct[i][j] - stat_dct[i][j]) <= minmax_threshold,
                                f'found:{image_info.stat_dct[i][j]}, expected {stat_dct[i][j]} +-{minmax_threshold} ')
            # Check mean and stddev
            for j in range(2,4):
                self.assertAlmostEqual(image_info.stat_dct[i][j], stat_dct[i][j], 1,
                                       f'found:{image_info.stat_dct[i][j]}, expected {stat_dct[i][j]} within 1 decimal places')
            # Check data pixel count
            self.assertTrue(abs(image_info.datapixelcount_dct[i] - datapixelcount) / float(datapixelcount) <= datapixelcount_threshold,
                            f'found:{image_info.datapixelcount_dct[i]}, expected {datapixelcount} +-{datapixelcount_threshold/100} percent')

            # Check custom weights were not used
            self.assertIn('Using custom 8-band weights for pansharpening', so.decode('utf-8'))
            self.assertNotIn('Custom pansharpening weights not available for 3 and 4-band imagery: each band will be weighted equally', so.decode('utf-8'))

    def test_pansharpen_full_res(self):

        src_fn = "WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008"
        src = os.path.join(self.srcdir, src_fn + ".ntf")
        cmd = 'python {} {} {} --skip-cmd-txt -p 3413'.format(
            self.scriptpath,
            src,
            self.dstdir,
        )

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        print(so)
        print(se)

        # make sure output data exist
        dstfp = os.path.join(self.dstdir, src_fn + "_u08rf3413_pansh.tif")
        dstfp_xml = os.path.join(self.dstdir, src_fn + "_u08rf3413_pansh.xml")
        self.assertTrue(os.path.isfile(dstfp))
        self.assertTrue(os.path.isfile(dstfp_xml))

        image_info = mosaic.ImageInfo(dstfp, 'IMAGE')
        self.assertAlmostEqual(image_info.xres, 0.564, 3)
        self.assertAlmostEqual(image_info.yres, 0.560, 3)

    def test_pansharpen_cog(self):
        src = os.path.join(self.srcdir, "WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008.ntf")
        cmd = 'python {} {} {} --skip-cmd-txt -p 3413 -r 10 -f COG'.format(
            self.scriptpath,
            src,
            self.dstdir,
        )

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        print(so)
        print(se)

        dstfp = os.path.join(self.dstdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u08rf3413_pansh.tif')
        dstfp_xml = os.path.join(self.dstdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u08rf3413_pansh.xml')

        self.assertTrue(os.path.isfile(dstfp))
        self.assertTrue(os.path.isfile(dstfp_xml))

        with gdal.Open(dstfp, gdalconst.GA_ReadOnly) as ds:
            self.assertIn('LAYOUT=COG', ds.GetMetadata_List('IMAGE_STRUCTURE'))

    def test_pansharpen_rgb(self):
        src = os.path.join(self.srcdir, "WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008.ntf")
        cmd = 'python {} {} {} --skip-cmd-txt -p 3413 -r 10 --rgb'.format(
            self.scriptpath,
            src,
            self.dstdir,
        )

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        print(so)
        print(se)

        dstfp = os.path.join(self.dstdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u08rf3413_pansh.tif')
        dstfp_xml = os.path.join(self.dstdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u08rf3413_pansh.xml')

        self.assertTrue(os.path.isfile(dstfp))
        self.assertTrue(os.path.isfile(dstfp_xml))

        with gdal.Open(dstfp, gdalconst.GA_ReadOnly) as ds:
            self.assertTrue(ds.RasterCount == 3)

    def tearDown(self):
       shutil.rmtree(self.dstdir, ignore_errors=True)

# Used to test pansharpen output
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

    test_cases = [
        TestPanshFunc
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
