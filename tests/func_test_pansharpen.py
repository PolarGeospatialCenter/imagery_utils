import unittest, os, sys, argparse, logging, subprocess
import gdal


script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

logger = logging.getLogger("logger")
# lso = logging.StreamHandler()
# lso.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
# lso.setFormatter(formatter)
# logger.addHandler(lso)


class TestPanshFunc(unittest.TestCase):

    def setUp(self):

        self.scriptpath = os.path.join(root_dir, "pgc_pansharpen.py")
        self.srcdir = os.path.join(script_dir, 'testdata', 'pansharpen_subset')
        self.dstdir = os.path.join(script_dir, 'testdata', 'output')
        # if os.path.isdir(self.dstdir):
        #     shutil.rmtree(self.dstdir)
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    def test_pansharpen(self):

        cmd = 'python {} {} {} -p 3413'.format(
            self.scriptpath,
            self.srcdir,
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


if __name__ == '__main__':

    # Set Up Arguments
    parser = argparse.ArgumentParser(description="Test pgc_pansharpen.py")

    parser.add_argument('--testdata', help="test data directory (default is testdata folder within script directory)")

    # Parse Arguments
    args = parser.parse_args()
    global test_dir

    if args.testdata:
        test_dir = os.path.abspath(args.testdata)
    else:
        test_dir = os.path.join(script_dir, 'testdata')

    if not os.path.isdir(test_dir):
        parser.error("Test data folder does not exist: {}".format(test_dir))

    test_cases = [
        TestPanshFunc
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
