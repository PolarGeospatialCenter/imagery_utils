import shutil
import unittest, os, subprocess
import sys
from osgeo import gdal

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
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    def test_pansharpen(self):

        src = self.srcdir
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
        dstfp = os.path.join(self.dstdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u08rf3413_pansh.tif')
        dstfp_xml = os.path.join(self.dstdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u08rf3413_pansh.xml')

        self.assertTrue(os.path.isfile(dstfp))
        self.assertTrue(os.path.isfile(dstfp_xml))

        # check second image from proccessing
        dstfp_2 = os.path.join(self.dstdir, 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u08rf3413_pansh.tif')
        dstfp_xml_2 = os.path.join(self.dstdir, 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u08rf3413_pansh.xml')

        self.assertTrue(os.path.isfile(dstfp_2))
        self.assertTrue(os.path.isfile(dstfp_xml_2))

        # verify data type
        ds = gdal.Open(dstfp, gdal.GA_ReadOnly)
        dt = ds.GetRasterBand(1).DataType
        self.assertEqual(dt, 1)
        ds = None

        image_info = mosaic.ImageInfo(dstfp, 'IMAGE')
        self.assertAlmostEqual(image_info.xres, 0.564193804791, 11)
        self.assertAlmostEqual(image_info.yres, 0.560335413717, 11)
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
        stat_dct = {1: [2.0, 153.0, 21.934843, 7.315011],
                    2: [1.0, 141.0, 17.149106, 6.760020],
                    3: [1.0, 145.0, 11.088902, 7.401054],
                    4: [1.0, 172.0, 37.812614, 27.618598]}
        datapixelcount_dct = {1: 857617457, 2: 857617457, 3: 857617457, 4: 857617457}
        datapixelcount_threshold = 0.00001 # percentage
        minmax_threshold = 5
        for i in range(1,len(image_info.stat_dct)+1):# check stats are similar
            # Check min and max values +- 3
            for j in range(2):
                self.assertTrue(abs(image_info.stat_dct[i][j] - stat_dct[i][j]) <= minmax_threshold,
                                f'found:{image_info.stat_dct[i][j]}, expected {stat_dct[i][j]} +-{minmax_threshold} ')
            # Check mean and stddev within 2 decimal places
            for j in range(2,4):
                self.assertAlmostEqual(image_info.stat_dct[i][j], stat_dct[i][j], 2,
                                       f'found:{image_info.stat_dct[i][j]}, expected {stat_dct[i][j]} within 2 decimal places')
            # Check data pixel count within 0.001%
            self.assertTrue(abs(image_info.datapixelcount_dct[i] - datapixelcount_dct[i]) / float(datapixelcount_dct[i]) <= datapixelcount_threshold,
                            f'found:{image_info.datapixelcount_dct[i]}, expected {datapixelcount_dct[i]} +-{datapixelcount_threshold/100} percent')

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
