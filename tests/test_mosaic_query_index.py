import shutil
from osgeo import gdal, gdalconst
import pyogrio
import unittest, os, subprocess

__test_dir__ = os.path.dirname(os.path.abspath(__file__))
__app_dir__ = os.path.dirname(__test_dir__)
testdata_dir = os.path.join(__test_dir__, 'testdata')


class TestMosaicQuery(unittest.TestCase):

    def setUp(self):
        self.srcdir = os.path.join(os.path.join(testdata_dir, 'mosaic', 'query'))
        self.scriptpath = os.path.join(__app_dir__, "pgc_mosaic_query_index.py")
        self.dstdir = os.path.join(__test_dir__, 'tmp_output')
        self.querydir = os.path.join(self.dstdir, 'query')
        self.index = os.path.join(self.srcdir, "pgcImageryIndexV6_2026jul06_arctic_only.gdb", "arctic_only")
        self.tile_csv = os.path.join(self.srcdir, "pgc_imagery_mosaic_tiles_arctic_2024.csv")
        self.ttile = "30_23"
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    def test_default_pan_mosaic_query(self):
        # default panchromatic query
        queryname = 'test_query_default'
        args = f'--ttile {self.ttile} --build-shp '
        cmd = 'python {} {} {} {} {} {}'.format(
            self.scriptpath,
            self.index,
            self.tile_csv,
            self.dstdir,
            queryname,
            args
        )
        print(cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)

        # test that all output files exist
        imagery_shp = os.path.join(self.querydir, f"{queryname}_{self.ttile}_imagery.shp")
        orig_csv = os.path.join(self.querydir, f"{queryname}_{self.ttile}_orig_ontape.csv")
        orig_txt = os.path.join(self.querydir, f"{queryname}_{self.ttile}_orig.txt")
        ortho_txt = os.path.join(self.querydir, f"{queryname}_{self.ttile}_ortho.txt")

        self.assertTrue(os.path.isfile(imagery_shp))
        self.assertTrue(os.path.isfile(orig_csv))
        self.assertTrue(os.path.isfile(orig_txt))
        self.assertTrue(os.path.isfile(ortho_txt))

        ## test if outputs have correct number of lines (showing the selected images from the query)
        query_files = {
            orig_csv: 96,
            orig_txt: 95,
            ortho_txt: 95,
        }

        for f, cnt in query_files.items():
            fh = open(f)
            lines = fh.readlines()
            self.assertEqual(len(lines), cnt)

        ## test shp feature count
        shp_info = pyogrio.read_info(imagery_shp)
        self.assertEqual(shp_info["features"], 95)

    def test_coverage_mosaic_query(self):
        # default panchromatic query
        queryname = 'test_query_layers_3'
        args = f'--ttile {self.ttile} --build-shp --mosaic-layers 3 '
        cmd = 'python {} {} {} {} {} {}'.format(
            self.scriptpath,
            self.index,
            self.tile_csv,
            self.dstdir,
            queryname,
            args
        )
        print(cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)

        # test that all output files exist
        imagery_shp = os.path.join(self.querydir, f"{queryname}_{self.ttile}_imagery.shp")
        orig_csv = os.path.join(self.querydir, f"{queryname}_{self.ttile}_orig_ontape.csv")
        orig_txt = os.path.join(self.querydir, f"{queryname}_{self.ttile}_orig.txt")
        ortho_txt = os.path.join(self.querydir, f"{queryname}_{self.ttile}_ortho.txt")

        self.assertTrue(os.path.isfile(imagery_shp))
        self.assertTrue(os.path.isfile(orig_csv))
        self.assertTrue(os.path.isfile(orig_txt))
        self.assertTrue(os.path.isfile(ortho_txt))

        ## test if outputs have correct number of lines (showing the selected images from the query)
        query_files = {
            orig_csv: 301,
            orig_txt: 300,
            ortho_txt: 300,
        }

        for f, cnt in query_files.items():
            fh = open(f)
            lines = fh.readlines()
            self.assertEqual(len(lines), cnt)

        ## test shp feature count
        shp_info = pyogrio.read_info(imagery_shp)
        self.assertEqual(shp_info["features"], 300)

    def test_num_images_mosaic_query(self):
        # default panchromatic query
        queryname = 'test_query_num_images'
        args = f'--ttile {self.ttile} --build-shp --num-images 400 '
        cmd = 'python {} {} {} {} {} {}'.format(
            self.scriptpath,
            self.index,
            self.tile_csv,
            self.dstdir,
            queryname,
            args
        )
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        se, so = p.communicate()
        # print(so)
        # print(se)

        # test that all output files exist
        imagery_shp = os.path.join(self.querydir, f"{queryname}_{self.ttile}_qa_scenes_components.shp")
        qa_scenes_txt = os.path.join(self.querydir, f"{queryname}_{self.ttile}_qa_scenes.txt")

        self.assertTrue(os.path.isfile(imagery_shp))
        self.assertTrue(os.path.isfile(qa_scenes_txt))

        ## test if outputs have correct number of lines (showing the selected images from the query)
        query_files = {
            qa_scenes_txt: 401,
        }

        for f, cnt in query_files.items():
            fh = open(f)
            lines = fh.readlines()
            self.assertEqual(len(lines), cnt)

        ## test shp feature count
        shp_info = pyogrio.read_info(imagery_shp)
        self.assertEqual(shp_info["features"], 400)

    def tearDown(self):
        shutil.rmtree(self.dstdir)


if __name__ == '__main__':

    test_cases = [
        TestMosaicQuery
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
