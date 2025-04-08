import shutil
import unittest, os, subprocess
from osgeo import gdal

__test_dir__ = os.path.dirname(os.path.abspath(__file__))
__app_dir__ = os.path.dirname(__test_dir__)
testdata_dir = os.path.join(__test_dir__, 'testdata')


class TestNdviFunc(unittest.TestCase):
    
    def setUp(self):
        
        self.scriptpath = os.path.join(__app_dir__, "pgc_ndvi.py")
        self.dstdir = os.path.join(__test_dir__, 'tmp_output')
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    # @unittest.skip("skipping")
    def test_ndvi(self):
        
        srcdir = os.path.join(os.path.join(testdata_dir, 'ndvi', 'ortho'))
        
        cmd = 'python {} {} {} --skip-cmd-txt '.format(
            self.scriptpath,
            srcdir,
            self.dstdir,
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)
        
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
                
    # @unittest.skip("skipping")
    def test_ndvi_int16(self):
        
        srcdir = os.path.join(os.path.join(testdata_dir, 'ndvi', 'ortho'))
        
        cmd = 'python {} {} {} --skip-cmd-txt -t Int16'.format(
            self.scriptpath,
            srcdir,
            self.dstdir,
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)
        
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
                
    # @unittest.skip("skipping")
    def test_ndvi_from_pansharp(self):
        
        srcdir = os.path.join(os.path.join(testdata_dir, 'ndvi', 'pansh'))
        
        cmd = 'python {} {} {} -t Int16 --skip-cmd-txt'.format(
            self.scriptpath,
            srcdir,
            self.dstdir,
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)
        
        for f in os.listdir(srcdir):
            if f.endswith('.tif'):
                dstfp = os.path.join(self.dstdir, f[:-4] + '_ndvi.tif')
                dstfp_xml = os.path.join(self.dstdir, f[:-4] + '_ndvi.xml')
                self.assertTrue(os.path.isfile(dstfp))
                self.assertTrue(os.path.isfile(dstfp_xml))
                ds = gdal.Open(dstfp)
                dt = ds.GetRasterBand(1).DataType
                self.assertEqual(dt, 3)
    
    def tearDown(self):
       shutil.rmtree(self.dstdir)


if __name__ == '__main__':
        
    test_cases = [
        TestNdviFunc
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
