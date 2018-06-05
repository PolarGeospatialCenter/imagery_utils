import unittest, os, sys, glob, argparse, logging
import gdal
import numpy as np

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

from lib import mosaic

logger = logging.getLogger("logger")


class TestNDVIImageInfo(unittest.TestCase):

    def setUp(self):
        self.srcdir = os.path.join(os.path.join(test_dir, 'output'))

    def test_ndvi_info_wv02(self):
        image = 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413_ndvi.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir, image), 'IMAGE')

        self.assertEqual(image_info.xres, 64.0)
        self.assertEqual(image_info.yres, 64.0)
        self.assertEqual(image_info.proj,
                         'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        self.assertEqual(image_info.panfactor, 1)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 78.55555555555556)

        image_info.get_raster_stats()
        stat_dct = {1: [-0.50704223, 0.88185203, 0.51517793, 0.27062701]}
        datapixelcount_dct = {1: 75466}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)

    def test_ndvi_info_wv02_pansh(self):
        image = 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413_pansh_ndvi.tif'
        image_info = mosaic.ImageInfo(os.path.join(self.srcdir, image), 'IMAGE')

        self.assertEqual(image_info.xres, 16.0)
        self.assertEqual(image_info.yres, 16.0)
        self.assertEqual(image_info.proj,
                         'PROJCS["unnamed",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Polar_Stereographic"],PARAMETER["latitude_of_origin",70],PARAMETER["central_meridian",-45],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]')
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
        self.assertEqual(image_info.panfactor, 1)
        self.assertEqual(image_info.date_diff, -9999)
        self.assertEqual(image_info.year_diff, -9999)
        self.assertAlmostEqual(image_info.score, 78.55555555555556)

        image_info.get_raster_stats()
        stat_dct = {1: [-991.0, 996.0, 536.20172830189938, 250.75913591790697]}
        datapixelcount_dct = {1: 1203262}
        for i in range(len(image_info.stat_dct[1])):
            self.assertAlmostEqual(image_info.stat_dct[1][i], stat_dct[1][i])
        self.assertEqual(image_info.datapixelcount_dct, datapixelcount_dct)

def img_as_array(img_file, band=1):
    """
    Open image as numpy array using GDAL.

    :param img_file: <str> path to image file
    :param band: <int> band to be opened (default=1)
    :return: <numpy.ndarray> image as 2d array
    """
    new_img = gdal.Open(img_file, gdal.GA_ReadOnly)
    band = new_img.GetRasterBand(band)
    new_arr = band.ReadAsArray()

    return new_arr


def get_images(img_list, img_target):
    """

    :param img_list: <list> full image paths
    :param img_target: <str> image to find from list
    :return: <str> target image path
    """
    img_target_path = [i for i in img_list if img_target in i]

    if not img_target_path:
        raise Exception("No images found for target {0}".format(img_target))
    elif len(img_target_path) > 1:
        raise Exception("Multiple results found for target {0}; there should only be one".format(img_target))

    return img_target_path[0]


class TestNDVIDataValues(unittest.TestCase):

    def setUp(self):
        image_new = os.path.join(test_dir, 'output')
        image_old = os.path.join(test_dir, 'output_static')

        # find images
        self.new_imgs = sorted(glob.glob(os.path.join(image_new, "*.tif")))
        self.old_imgs = sorted(glob.glob(os.path.join(image_old, "*.tif")))

        # if no images found, explain why
        if not self.new_imgs:
            raise Exception("No images in self.new_imgs; run 'func_test_ndvi.py' to generate images")
        if not self.old_imgs:
            raise Exception("No images in self.old_imgs; create or populate 'output_static' directory with mosaics "
                            "using previous version of the codebase")


    # one
    def test_ndvi_equivalence(self):
        # select images
        target_image = 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413_ndvi.tif'
        new = get_images(self.new_imgs, target_image)
        old = get_images(self.old_imgs, target_image)

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # two
    def test_ndvi_pansh_equivalence(self):
        # select images
        target_image = 'WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413_pansh_ndvi.tif'
        new = get_images(self.new_imgs, target_image)
        old = get_images(self.old_imgs, target_image)

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))


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
    parser = argparse.ArgumentParser(description="Test imagery_utils pgc_ndvi.py output")

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
        TestNDVIImageInfo,
        TestNDVIDataValues,
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
