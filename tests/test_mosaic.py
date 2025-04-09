import shutil
import unittest, os, subprocess

__test_dir__ = os.path.dirname(os.path.abspath(__file__))
__app_dir__ = os.path.dirname(__test_dir__)
testdata_dir = os.path.join(__test_dir__, 'testdata')


class TestMosaicFunc(unittest.TestCase):
    
    def setUp(self):
        self.srcdir = os.path.join(os.path.join(testdata_dir, 'mosaic', 'ortho'))
        self.scriptpath = os.path.join(__app_dir__, "pgc_mosaic.py")
        self.dstdir = os.path.join(__test_dir__, 'tmp_output')
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    def test_pan_mosaic(self):
        # extent = -820000.0, -800000.0, -2420000.0, -2400000.0
        # tilesize = 10000, 10000
        # bands = 1
        mosaicname = os.path.join(self.dstdir, 'testmosaic1')
        args = '--skip-cmd-txt --component-shp -e -820000.0 -800000.0 -2420000.0 -2400000.0 -t 10000 10000 -b 1'
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

    def test_bgrn_mosaic_with_stats(self):
        # extent = -3260000, -3240000, 520000, 540000
        # tilesize = 10000, 10000
        # bands = 4
        mosaicname = os.path.join(self.dstdir, 'testmosaic2')
        args = '--skip-cmd-txt --component-shp -e -3260000 -3240000 520000 540000 -t 10000 10000 -b 4 --calc-stats --median-remove'
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

    def test_ndvi_pansh_mosaic(self):
        # extent = -3260000, -3240000, 520000, 540000
        # tilesize = 10000, 10000
        # bands = 1
        srcdir = os.path.join(os.path.join(testdata_dir, 'mosaic', 'pansh_ndvi'))
        mosaicname = os.path.join(self.dstdir, 'testmosaic3')
        args = '--skip-cmd-txt --component-shp -e -3260000 -3240000 520000 540000 -t 10000 10000 -b 1'
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

    def test_ndvi_pansh_mosaic_with_stats(self):
        # extent = -3260000, -3240000, 520000, 540000
        # tilesize = 10000, 10000
        # bands = 1
        srcdir = os.path.join(os.path.join(testdata_dir, 'mosaic', 'pansh_ndvi'))
        mosaicname = os.path.join(self.dstdir, 'testmosaic4')
        args = '--skip-cmd-txt --component-shp -e -3260000 -3240000 520000 540000 -t 10000 10000 -b 1 --calc-stats --median-remove'
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

    def tearDown(self):
        shutil.rmtree(self.dstdir)


if __name__ == '__main__':

    test_cases = [
        TestMosaicFunc
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
