import shutil
import unittest, os, subprocess
from osgeo import gdal

__test_dir__ = os.path.dirname(os.path.abspath(__file__))
__app_dir__ = os.path.dirname(__test_dir__)
testdata_dir = os.path.join(__test_dir__, 'testdata')

class TestPanshFunc(unittest.TestCase):

    def setUp(self):

        self.scriptpath = os.path.join(__app_dir__, "pgc_pansharpen.py")
        self.srcdir = os.path.join(testdata_dir, 'pansharpen', 'src')
        self.dstdir = os.path.join(__test_dir__, 'tmp_output')
        # if os.path.isdir(self.dstdir):
        #     shutil.rmtree(self.dstdir)
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    def test_pansharpen(self):

        src = os.path.join(self.srcdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008.ntf')
        cmd = 'python {} {} {} -r 10 --skip-cmd-txt -p 3413'.format(
            self.scriptpath,
            src,
            self.dstdir,
        )

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)

        # make sure output data exist
        dstfp = os.path.join(self.dstdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u08rf3413_pansh.tif')
        dstfp_xml = os.path.join(self.dstdir, 'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u08rf3413_pansh.xml')

        self.assertTrue(os.path.isfile(dstfp))
        self.assertTrue(os.path.isfile(dstfp_xml))

        # verify data type
        ds = gdal.Open(dstfp, gdal.GA_ReadOnly)
        dt = ds.GetRasterBand(1).DataType
        self.assertEqual(dt, 1)
        ds = None

    def tearDown(self):
       shutil.rmtree(self.dstdir)


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
