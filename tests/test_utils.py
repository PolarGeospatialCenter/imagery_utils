import unittest
import os
import sys
import shutil
import osgeo  # necessary for data type check
from osgeo import ogr
import platform

__test_dir__ = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(__test_dir__))
testdata_dir = os.path.join(__test_dir__, 'testdata')

from lib import utils

class TestUtils(unittest.TestCase):

    def setUp(self):
        self.output = os.path.join(__test_dir__, 'tmp_output')
        if not os.path.isdir(self.output):
            os.makedirs(self.output)

        # test SpatialRef class
        self.epsg_npole = 3413
        self.epsg_spole = 3031
        self.epsg_bad_1 = 'bad'
        self.epsg_bad_2 = 1010

        # test scenes to get sensor information
        self.basedir = os.path.join(testdata_dir, 'ortho')
        self.srcdir_ge = os.path.join(self.basedir, 'GE01_110108M0010160234A222000100252M_000500940.ntf')
        self.srcdir_ik = os.path.join(self.basedir,
                                      'IK01_20050319201700_2005031920171340000011627450_po_333838_blu_0000000.ntf')
        self.srcdir_dg = os.path.join(self.basedir,
                                      'WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007.NTF'
                                      )

        # find images from srcdir_ndvi, use self.imglist as verification list
        self.srcdir_ndvi = os.path.join(testdata_dir, 'ndvi', 'ortho')
        im_names = ['WV02_20110901210434_103001000B41DC00_11SEP01210434-M1BS-052730735130_01_P007_u16rf3413.tif',
                    'WV02_20110901210435_103001000B41DC00_11SEP01210435-M1BS-052730735130_01_P008_u16rf3413.tif',
                    'WV02_20110901210500_103001000D52C800_11SEP01210500-M1BS-052560788010_01_P006_u16rf3413.tif',
                    'WV02_20110901210501_103001000D52C800_11SEP01210501-M1BS-052560788010_01_P007_u16rf3413.tif',
                    'WV02_20110901210502_103001000D52C800_11SEP01210502-M1BS-052560788010_01_P008_u16rf3413.tif']
        self.imglist = [os.path.join(self.srcdir_ndvi, f) for f in im_names]
        self.txtfile = os.path.join(self.output, 'img_list.txt')
        with open(self.txtfile, 'w') as f:
            f.write("\n".join(self.imglist))

        # file to be excluded
        self.excllist = ['WV02_20110901210501_103001000D52C800_11SEP01210501-M1BS-052560788010_01_P007_u16rf3413.tif']
        self.exclfile = os.path.join(self.output, 'excl_list.txt')
        with open(self.exclfile, 'w') as f:
            f.write("\n".join(self.excllist))

        # create dir and empty files to be deleted
        self.dummydir = os.path.join(testdata_dir, 'dummy_dir')
        if not os.path.isdir(self.dummydir):
            os.makedirs(self.dummydir)
        self.dummyfns = [os.path.join(self.dummydir, 'stuff1.txt'), os.path.join(self.dummydir, 'stuff1.tif'),
                         os.path.join(self.dummydir, 'stuff1.xml')]
        [open(x, 'a').close() for x in self.dummyfns]

        # well-known text, to be turned into geometries to test 180th parallel crossing
        self.poly_no180 = 'POLYGON (( {} {}, {} {}, {} {}, {} {}, {} {} ))'.format(-183.1, -75.2,
                                                                                   -183.1, -74,
                                                                                   -177.5, -74,
                                                                                   -177.5, -75.2,
                                                                                   -183.1, -75.2)
        self.poly_yes180 = 'POLYGON (( {} {}, {} {}, {} {}, {} {}, {} {} ))'.format(-179.1, -75.2,
                                                                                    -179.1, -74,
                                                                                    179.5, -74,
                                                                                    179.5, -75.2,
                                                                                    -179.1, -75.2)

    def test_spatial_ref(self):
        sref_np = utils.SpatialRef(self.epsg_npole)
        sref_sp = utils.SpatialRef(self.epsg_spole)
        with self.assertRaises(RuntimeError) as cm:
            utils.SpatialRef(self.epsg_bad_1)  # breaks for not being an integer
        with self.assertRaises(RuntimeError) as cm:
            utils.SpatialRef(self.epsg_bad_2)  # break for invalid EPSG code

        self.assertTrue(isinstance(sref_np.srs, osgeo.osr.SpatialReference))
        self.assertTrue(isinstance(sref_sp.srs, osgeo.osr.SpatialReference))

        self.assertTrue(sref_np.proj4, '+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs ')
        self.assertTrue(sref_sp.proj4, '+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs ')

        self.assertTrue(sref_np.epsg, 3413)
        self.assertTrue(sref_sp.epsg, 3031)

    def test_get_bit_depth(self):
        self.assertEqual(utils.get_bit_depth("Byte"), "u08")
        self.assertEqual(utils.get_bit_depth("UInt16"), "u16")
        self.assertEqual(utils.get_bit_depth("Float32"), "f32")
        self.assertEqual(utils.get_bit_depth("Uint16"), None)  # function logs error, and returns None

    def test_get_sensor(self):
        vendor, sat, _, _, _, _ = utils.get_sensor(self.srcdir_ge)
        self.assertEqual(vendor.value, 'GeoEye')
        self.assertEqual(sat, 'GE01')

        vendor, sat, _, _, _, _ = utils.get_sensor(self.srcdir_ik)
        self.assertEqual(vendor.value, 'GeoEye')
        self.assertEqual(sat, 'IK01')

        vendor, sat, _, _, _, _ = utils.get_sensor(self.srcdir_dg)
        self.assertEqual(vendor.value, 'DigitalGlobe')
        self.assertEqual(sat, 'WV01')

    def test_find_images(self):
        # without text file
        image_list = utils.find_images(self.srcdir_ndvi, False, '.tif')
        # if windows, convert slashes so lists are comparable
        if platform.system() == "Windows":
            image_list = [il.replace("/", "\\") for il in image_list]
        self.assertEqual(sorted(image_list), sorted(self.imglist))

        # with text file
        image_list = utils.find_images(self.txtfile, True, '.tif')
        # if windows, convert slashes so lists are comparable
        if platform.system() == "Windows":
            image_list = [il.replace("/", "\\") for il in image_list]
        self.assertEqual(sorted(image_list), sorted(self.imglist))

    def test_find_images_with_exclude_list(self):
        # without text files
        image_list = utils.find_images_with_exclude_list(self.srcdir_ndvi, False, '.tif', self.excllist)
        # if windows, convert slashes so lists are comparable
        if platform.system() == "Windows":
            image_list = [il.replace("/", "\\") for il in image_list]
        self.assertEqual(sorted(image_list), sorted([x for x in self.imglist if x != self.excllist[0]]))

        # with text files
        image_list = utils.find_images_with_exclude_list(self.txtfile, True, '.tif', self.exclfile)
        # if windows, convert slashes so lists are comparable
        if platform.system() == "Windows":
            image_list = [il.replace("/", "\\") for il in image_list]
        self.assertEqual(sorted(image_list), sorted([x for x in self.imglist if x != self.excllist[0]]))

    def test_delete_temp_files(self):
        utils.delete_temp_files(self.dummyfns)
        self.assertFalse(os.path.isfile(self.dummyfns[0]))
        self.assertFalse(os.path.isfile(self.dummyfns[1]))
        self.assertFalse(os.path.isfile(self.dummyfns[2]))

    '''
    NOTE: not testing get_source_names(); should add test gdb later
    '''

    def test_does_cross_180(self):
        self.assertFalse(utils.doesCross180(ogr.CreateGeometryFromWkt(self.poly_no180)))
        self.assertTrue(utils.doesCross180(ogr.CreateGeometryFromWkt(self.poly_yes180)))

    def test_get_wrapped_geometry(self):
        self.assertTrue(isinstance(utils.getWrappedGeometry(ogr.CreateGeometryFromWkt(self.poly_yes180)),
                                   osgeo.ogr.Geometry))

        '''
        Cannot test for calc_y_intersection_with_180 ZeroDivisionError; code before it prevents this from happening
        '''

    def tearDown(self):
        if os.path.isdir(self.dummydir):
            shutil.rmtree(self.dummydir)
        if os.path.isfile(self.txtfile):
            os.remove(self.txtfile)
        if os.path.isfile(self.exclfile):
            os.remove(self.exclfile)


if __name__ == '__main__':

    test_cases = [
        TestUtils
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
