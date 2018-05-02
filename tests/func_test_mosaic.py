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


class TestMosaicFunc(unittest.TestCase):
    
    def setUp(self):
        self.srcdir = os.path.join(os.path.join(test_dir, 'mosaic', 'ortho'))
        self.scriptpath = os.path.join(root_dir, "pgc_mosaic.py")
        self.dstdir = os.path.join(script_dir, 'testdata', 'output')
        # if os.path.isdir(self.dstdir):
        #     shutil.rmtree(self.dstdir)
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    #@unittest.skip("skipping")
    def test_pan_mosaic(self):   
        # extent = -820000.0, -800000.0, -2420000.0, -2400000.0
        # tilesize = 10000, 10000
        # bands = 1
        mosaicname = os.path.join(self.dstdir, 'testmosaic1')
        args = '--component-shp -e -820000.0 -800000.0 -2420000.0 -2400000.0 -t 10000 10000 -b 1'
        cmd = 'python {} {} {} {}'.format(
            self.scriptpath,
            self.srcdir,
            mosaicname,
            args
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se,so = p.communicate()
        # print(so)
        # print(se)
        
        self.assertTrue(os.path.isfile(mosaicname + '_1_1.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_1_2.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_2_1.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_2_2.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_cutlines.shp'))
        self.assertTrue(os.path.isfile(mosaicname + '_components.shp'))
        self.assertTrue(os.path.isfile(mosaicname + '_tiles.shp'))
        
        ## test if intersects files have correct number of files
        intersects_files = {
            mosaicname + '_1_1_intersects.txt': 2,
            mosaicname + '_2_1_intersects.txt': 3,
            mosaicname + '_1_2_intersects.txt': 2,
            mosaicname + '_2_2_intersects.txt': 2,
        }
        
        for f, cnt in intersects_files.items():
            fh = open(f)
            lines = fh.readlines()
            self.assertEqual(len(lines), cnt)
            
        ## TODO test if culines does not have stats and median

    #@unittest.skip("skipping")
    def test_bgrn_mosaic_with_stats(self):   
        # extent = -3260000, -3240000, 520000, 540000
        # tilesize = 10000, 10000
        # bands = 4
        mosaicname = os.path.join(self.dstdir, 'testmosaic2')
        args = '--component-shp -e -3260000 -3240000 520000 540000 -t 10000 10000 -b 4 --calc-stats --median-remove'
        cmd = 'python {} {} {} {}'.format(
            self.scriptpath,
            self.srcdir,
            mosaicname,
            args
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)
        
        self.assertTrue(os.path.isfile(mosaicname + '_1_1.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_1_2.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_2_1.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_2_2.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_cutlines.shp'))
        self.assertTrue(os.path.isfile(mosaicname + '_components.shp'))
        self.assertTrue(os.path.isfile(mosaicname + '_tiles.shp'))
        
        ## TODO test if culines has stats and median
    
    #@unittest.skip("skipping")    
    def test_ndvi_pansh_mosaic(self):   
        # extent = -3260000, -3240000, 520000, 540000
        # tilesize = 10000, 10000
        # bands = 1
        srcdir = os.path.join(os.path.join(test_dir, 'mosaic', 'pansh_ndvi'))
        mosaicname = os.path.join(self.dstdir, 'testmosaic3')
        args = '--component-shp -e -3260000 -3240000 520000 540000 -t 10000 10000 -b 1'
        cmd = 'python {} {} {} {}'.format(
            self.scriptpath,
            srcdir,
            mosaicname,
            args
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)
        
        self.assertTrue(os.path.isfile(mosaicname + '_1_1.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_1_2.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_2_1.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_2_2.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_cutlines.shp'))
        self.assertTrue(os.path.isfile(mosaicname + '_components.shp'))
        self.assertTrue(os.path.isfile(mosaicname + '_tiles.shp'))

    #@unittest.skip("skipping")    
    def test_ndvi_pansh_mosaic_with_stats(self):   
        # extent = -3260000, -3240000, 520000, 540000
        # tilesize = 10000, 10000
        # bands = 1
        srcdir = os.path.join(os.path.join(test_dir, 'mosaic', 'pansh_ndvi'))
        mosaicname = os.path.join(self.dstdir, 'testmosaic4')
        args = '--component-shp -e -3260000 -3240000 520000 540000 -t 10000 10000 -b 1 --calc-stats --median-remove'
        cmd = 'python {} {} {} {}'.format(
            self.scriptpath,
            srcdir,
            mosaicname,
            args
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)
        
        self.assertTrue(os.path.isfile(mosaicname + '_1_1.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_1_2.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_2_1.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_2_2.tif'))
        self.assertTrue(os.path.isfile(mosaicname + '_cutlines.shp'))
        self.assertTrue(os.path.isfile(mosaicname + '_components.shp'))
        self.assertTrue(os.path.isfile(mosaicname + '_tiles.shp'))

    # def tearDown(self):
    #     shutil.rmtree(self.dstdir)

    ## test_mosaic_pbs


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
        TestMosaicFunc
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
