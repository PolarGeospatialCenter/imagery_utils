import unittest, os, sys, glob, shutil, argparse, logging, subprocess
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


class TestNdviFunc(unittest.TestCase):

    def setUp(self):

        self.scriptpath = os.path.join(root_dir, "pgc_pansharpen.py")
        self.dstdir = os.path.join(script_dir, 'testdata', 'output')
        # if os.path.isdir(self.dstdir):
        #     shutil.rmtree(self.dstdir)
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    #@unittest.skip("skipping")
    def test_pansharpen(self):

        srcdir = os.path.join(os.path.join(test_dir, 'pansharpen'))

        # TODO: keep converting from 'ndvi' to 'pansharpen' where necessary
        cmd = 'python {} {} {}'.format(
            self.scriptpath,
            srcdir,
            self.dstdir,
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)

        # TODO: convert this to use only a single NTF (make sure it's small, else this will take for-ev-er)
        for f in os.listdir(srcdir):
            if f.endswith('.tif'):
                dstfp = os.path.join(self.dstdir, f[:-4] + '_ndvi.tif')
                dstfp_xml = os.path.join(self.dstdir, f[:-4] + '_ndvi.xml')
                self.assertTrue(os.path.isfile(dstfp))
                self.assertTrue(os.path.isfile(dstfp_xml))
                ds = gdal.Open(dstfp)
                dt = ds.GetRasterBand(1).DataType
                self.assertEqual(dt, 6)
                ds = None

    #@unittest.skip("skipping")
    def test_ndvi_int16(self):

        srcdir = os.path.join(os.path.join(test_dir, 'ndvi', 'ortho'))

        cmd = 'python {} {} {} -t Int16'.format(
            self.scriptpath,
            srcdir,
            self.dstdir,
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)
        # TODO: convert this to use only a single NTF (make sure it's small, else this will take for-ev-er)
        for f in os.listdir(srcdir):
            if f.endswith('.tif'):
                dstfp = os.path.join(self.dstdir, f[:-4] + '_ndvi.tif')
                dstfp_xml = os.path.join(self.dstdir, f[:-4] + '_ndvi.xml')
                self.assertTrue(os.path.isfile(dstfp))
                self.assertTrue(os.path.isfile(dstfp_xml))
                ds = gdal.Open(dstfp)
                dt = ds.GetRasterBand(1).DataType
                self.assertEqual(dt, 3)
                ds = None

    #def tearDown(self):
    #    shutil.rmtree(self.dstdir)


if __name__ == '__main__':

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Test imagery_utils pgc_pansharpen.py package"
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
        TestNdviFunc
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
