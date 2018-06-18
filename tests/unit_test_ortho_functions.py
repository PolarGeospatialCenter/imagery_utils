import unittest, os, sys, glob, argparse, logging
import gdal, ogr
import xml
import numpy as np

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

from lib import ortho_functions, utils

#logger = logging.getLogger("logger")
# lso = logging.StreamHandler()
# lso.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
# lso.setFormatter(formatter)
# logger.addHandler(lso)


class TestReadMetadata(unittest.TestCase):
    
    def setUp(self):
        self.stretch = 'rf'
        self.rd_stretch = 'rd'
        self.srcdir = os.path.join(test_dir, 'metadata_files')
    
    #@unittest.skip("skipping")
    def test_parse_DG_md_files(self):
        
        dg_files = (
            ##(file name, if passes)
            ('10APR23190859-M2AS-052462689010_01_P001.XML', True), ## 2A unrenamed
            ('12JUL19233558-M1BS-052754253040_01_P001.XML', True), ## 1B unrenamed
            ('12AUG27132242-M1BS-500122876080_01_P006.XML', False), ## 1B unrenamed truncated xml
            ('GE01_11OCT122053047-P1BS-10504100009FD100.xml', True), #### GE01 image wth abscalfact in W/m2/um
            ('GE01_14APR022119147-M1BS-1050410010473600.xml', True), #### GE01 image wth abscalfact in W/cm2/nm
            ('GE01_20140402211914_1050410010473600_14APR02211914-M1BS-053720734020_01_P003.XML', True), ##GE01 pgctools3 name
            ('QB02_02OCT092117107-M2AS_R1C1-101001000153C800.xml', True), #2A tiled pgctools2 renamed
            ('QB02_12AUG271322429-M1BS-10100100101AD000.xml', True),  #1B pgctools2 renamed
            ('QB02_20021009211710_101001000153C800_02OCT09211710-M2AS_R1C1-052075481010_01_P001.xml', True),
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.XML', True),
            ('WV01_09OCT042222158-P1BS-1020010009B33500.xml', True),
            ('WV01_12MAR262229422-P1BS-102001001B02FA00.xml', True),
            ('WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.xml', True),
            ('WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007.XML', True),
            ('WV02_10APR231908590-M2AS_R1C1-1030010005C7AF00.xml', True),
            ('WV02_10APR231908590-M2AS_R2C3-1030010005C7AF00.xml', True),
            ('WV02_12JUL192335585-M1BS-103001001B998D00.xml', True),
            ('WV02_13OCT050528024-P1BS-10300100278D8500.xml', True),
            ('WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004.XML', True),
            ('WV03_14SEP192129471-M1BS-104001000227BF00.xml', True),
            ('WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.XML', True),
        )
        
        for mdf, result in dg_files:  ### test reflectance
            metapath = os.path.join(self.srcdir, mdf)
            try:
                calib_dict = ortho_functions.getDGXmlData(metapath, self.stretch)
            except xml.parsers.expat.ExpatError:
                calib_dict = False
            
            self.assertEqual(bool(calib_dict), result)
            if calib_dict:
                if 'BAND_P' in calib_dict:
                    self.assertGreater(calib_dict['BAND_P'][0], 0.0005)
                    self.assertLess(calib_dict['BAND_P'][0], 0.0015)
                if 'BAND_B' in calib_dict:
                    self.assertGreater(calib_dict['BAND_B'][0], 0.000447)
                    self.assertLess(calib_dict['BAND_B'][0], 0.0012)
                if 'BAND_P' in calib_dict:
                    self.assertGreater(calib_dict['BAND_P'][1], -0.029)
                    self.assertLess(calib_dict['BAND_P'][1], -0.0098)
                if 'BAND_B' in calib_dict:
                    self.assertGreater(calib_dict['BAND_B'][1], -0.1307)
                    self.assertLess(calib_dict['BAND_B'][1], -0.0084)
                if 'BAND_B' in calib_dict and 'BAND_G' in calib_dict: ### check bands are not equal
                    self.assertNotEqual(calib_dict['BAND_B'][0], calib_dict['BAND_G'][0])
                    
        for mdf, result in dg_files:   ### test radiance
            metapath = os.path.join(self.srcdir, mdf)
            calib_dict = ortho_functions.getDGXmlData(metapath, self.rd_stretch)
            self.assertEqual(bool(calib_dict), result)
            if calib_dict:
                #print calib_dict
                if 'BAND_P' in calib_dict:
                    self.assertGreater(calib_dict['BAND_P'][0], 0.08)
                    self.assertLess(calib_dict['BAND_P'][0], 0.15)
                if 'BAND_B' in calib_dict:
                    self.assertGreater(calib_dict['BAND_B'][0], 0.17)
                    self.assertLess(calib_dict['BAND_B'][0], 0.33)
                if 'BAND_P' in calib_dict:
                    self.assertGreater(calib_dict['BAND_P'][1], -4.5)
                    self.assertLess(calib_dict['BAND_P'][1], -1.4)
                if 'BAND_B' in calib_dict:
                    self.assertGreater(calib_dict['BAND_B'][1], -9.7)
                    self.assertLess(calib_dict['BAND_B'][1], -2.8)
                if 'BAND_B' in calib_dict and 'BAND_G' in calib_dict: ### check bands are not equal
                    self.assertNotEqual(calib_dict['BAND_B'][0], calib_dict['BAND_G'][0])
                           
    #@unittest.skip("skipping")     
    def test_parse_GE_md_files(self):
        ge_files = (
            ('GE01_110108M0010160234A222000100252M_000500940.txt', True),
        )
        
        for mdf, result in ge_files: ### test reflectance
            metapath = os.path.join(self.srcdir, mdf)
            calib_dict = ortho_functions.GetGEcalibDict(metapath, self.stretch)
            self.assertEqual(bool(calib_dict), result)
            if calib_dict:
                
                if 5 in calib_dict:  # pan band 
                    self.assertGreater(calib_dict[5][0], 0.0002)
                    self.assertLess(calib_dict[5][0], 0.0008)
                if 1 in calib_dict:  # blue band
                    self.assertGreater(calib_dict[1][0], 0.0002)
                    self.assertLess(calib_dict[1][0], 0.0008)
                if 5 in calib_dict:  # pan band bias
                    self.assertEqual(calib_dict[5][1], 0)
                if 1 in calib_dict:  # blue band bias
                    self.assertEqual(calib_dict[1][1], 0)
                if 1 in calib_dict and 2 in calib_dict: ### check bands are not equal
                    self.assertNotEqual(calib_dict[1][0], calib_dict[2][0])
                    
        for mdf, result in ge_files:  ### test radiance
            metapath = os.path.join(self.srcdir, mdf)
            calib_dict = ortho_functions.GetGEcalibDict(metapath, self.rd_stretch)
            self.assertEqual(bool(calib_dict), result)
            if calib_dict:
                
                if 5 in calib_dict:  # pan band
                    self.assertGreater(calib_dict[5][0], 0.1)
                    self.assertLess(calib_dict[5][0], 0.2)
                if 1 in calib_dict:  # blue band
                    self.assertGreater(calib_dict[1][0], 0.1)
                    self.assertLess(calib_dict[1][0], 0.2)
                if 5 in calib_dict:  # pan band bias
                    self.assertEqual(calib_dict[5][1], 0)
                if 1 in calib_dict:  # blue band bias
                    self.assertEqual(calib_dict[1][1], 0)
                if 1 in calib_dict and 2 in calib_dict: ### check bands are not equal
                    self.assertNotEqual(calib_dict[1][0], calib_dict[2][0])
        
    def test_parse_IK_md_files(self):
        
        ik_files = (
            ('IK01_20010602215300_2001060221531300000010031227_po_387877_metadata.txt', True), ## test IK metadata file with multiple source IDs
            ('IK01_19991222080400_1999122208040550000011606084_po_82037_metadata.txt', True),  ## test pgctools3 name
            ('IK01_20050319201700_2005031920171340000011627450_po_333838_metadata.txt', True), ## test pgctools3 name
            ('IK01_1999122208040550000011606084_pan_1569N.txt', True), ## test pgctools2 name
            ('IK01_2005031920171340000011627450_rgb_5817N.txt', True), ## test pgctools2 name
        )
        
        for mdf, result in ik_files:  ### test reflectance
            metapath = os.path.join(self.srcdir, mdf)
            calib_dict = ortho_functions.GetIKcalibDict(metapath, self.stretch)
            self.assertEqual(bool(calib_dict), result)
            if calib_dict:
                #print(mdf)
                #print(calib_dict)
                if 4 in calib_dict:  # pan band
                    self.assertGreater(calib_dict[4][0], 0.0004)
                    self.assertLess(calib_dict[4][0], 0.0007)
                if 0 in calib_dict:  # blue band
                    self.assertGreater(calib_dict[0][0], 0.0003)
                    self.assertLess(calib_dict[0][0], 0.0007)
                if 4 in calib_dict:  # pan band bias
                    self.assertEqual(calib_dict[4][1], 0)
                if 0 in calib_dict:  # blue band bias
                    self.assertEqual(calib_dict[0][1], 0)
                if 0 in calib_dict and 1 in calib_dict: ### check bands are not equal
                    self.assertNotEqual(calib_dict[0][0], calib_dict[1][0])
                    
        for mdf, result in ik_files:    ### test radiance
            metapath = os.path.join(self.srcdir, mdf)
            calib_dict = ortho_functions.GetIKcalibDict(metapath, self.rd_stretch)
            self.assertEqual(bool(calib_dict), result)
            if calib_dict:
                
                if 4 in calib_dict:  # pan band
                    self.assertGreater(calib_dict[4][0], 0.1)
                    self.assertLess(calib_dict[4][0], 0.16)
                if 0 in calib_dict:  # blue band
                    self.assertGreater(calib_dict[0][0], 0.15)
                    self.assertLess(calib_dict[0][0], 0.25)
                if 5 in calib_dict:  # pan band bias
                    self.assertEqual(calib_dict[4][1], 0)
                if 1 in calib_dict:  # blue band bias
                    self.assertEqual(calib_dict[0][1], 0)
                if 0 in calib_dict and 1 in calib_dict: ### check bands are not equal
                    self.assertNotEqual(calib_dict[0][0], calib_dict[1])


class TestWriteMetadata(unittest.TestCase):
    
    def setUp(self):
        self.fn = 'WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.ntf'
        self.mf = os.path.join(os.path.join(test_dir, 'output', 'WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.xml'))
        
        self.test_lines = [
            '<BITDEPTH>Byte</BITDEPTH>',
            '<COMPRESSION>lzw</COMPRESSION>',
            '<EPSG_CODE>4326</EPSG_CODE>',
            '<FORMAT>GTiff</FORMAT>',
            '<STRETCH>rf</STRETCH>',
            '<VERSION>imagery_utils v{}</VERSION>'.format(utils.package_version),
            '<ORTHO_HEIGHT>2568.0</ORTHO_HEIGHT>',
            '<RESAMPLEMETHOD>near</RESAMPLEMETHOD>'
        ]
    
    #@unittest.skip("skipping")
    def test_parse_DG_md_file(self):
        args = ProcessArgs(4326,'rf')
        info = ImageInfo(self.fn, 'DigitalGlobe', 'WV01')
        metafile = ortho_functions.GetDGMetadataPath(info.localsrc)
        info.metapath = metafile
        rc = ortho_functions.WriteOutputMetadata(args, info)
        ## read meta and check content
        f = open(self.mf)
        contents = f.read()
        f.close()
        for test_line in self.test_lines:
            self.assertTrue(test_line in contents)
    
    def tearDown(self):
        os.remove(self.mf)
        
        
                    
class TestCollectFiles(unittest.TestCase):
    
    def test_gather_metadata_file(self):
        
        rm_files = [
                '01JAN08QB020800008JAN01102125-P1BS-005590467020_01_P001_________AAE_0AAAAABAABA0.xml'
            ]
        
        for root, dirs, files in os.walk(os.path.join(test_dir, 'ortho')):
            for f in files:
                if f.lower().endswith(".ntf") or f.lower().endswith(".tif"):
                    #print(f)
                    #### Find metadata file
                    srcfp = os.path.join(root, f)
                    
                    metafile = ortho_functions.GetDGMetadataPath(srcfp)
                    if metafile is None:
                        metafile = ortho_functions.ExtractDGMetadataFile(srcfp, root)
                    if metafile is None:
                        metafile = ortho_functions.GetIKMetadataPath(srcfp)
                    if metafile is None:
                        metafile = ortho_functions.GetGEMetadataPath(srcfp)
                    self.assertIsNotNone(metafile)
                    
                    if metafile and os.path.basename(metafile) in rm_files:
                        os.remove(metafile)


class TestDEMOverlap(unittest.TestCase):
    
    def setUp(self):
        self.dem = os.path.join(os.path.join(test_dir, 'dem', 'ramp_lowres.tif'))
        self.srs = utils.SpatialRef(4326)
    
    def test_dem_overlap(self):
        image_geom_wkts = [
            ('POLYGON ((-52.23 70.843333,-51.735 70.844444,-51.736667 70.760556,-52.23 70.759722,-52.23 70.843333))',
             False),  # False
            (
            'POLYGON ((-64.23 -70.843333,-63.735 -70.844444,-63.736667 -70.760556,-64.23 -70.759722,-64.23 -70.843333))',
            True),  # True
            (
            'POLYGON ((-52.23 -50.843333,-51.735 -50.844444,-51.736667 -50.760556,-52.23 -50.759722,-52.23 -50.843333))',
            False)  # False
        ]
        
        for wkt, result in image_geom_wkts:
            test_result = ortho_functions.overlap_check(wkt, self.srs, self.dem)
            self.assertEqual(test_result, result)


class TestTargetExtent(unittest.TestCase):
        
    def test_target_extent(self):
        wkt = 'POLYGON ((810287 2505832,811661 2487415,807201 2487233,805772 2505802,810287 2505832))'
        fn = 'GE01_20110307105821_1050410001518E00_11MAR07105821-M1BS-500657359080_01_P008.ntf'
        target_extent_geom = ogr.CreateGeometryFromWkt(wkt)
        args = ProcessArgs(32629, 'rf')
        info = ImageInfo(fn)
        rc = ortho_functions.GetImageStats(args, info, target_extent_geom)
        self.assertEqual(info.extent,
                         '-te 805772.000000000000 2487233.000000000000 811661.000000000000 2505832.000000000000 ')


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

class TestOutputDataValues(unittest.TestCase):

    def setUp(self):
        image_new = os.path.join(test_dir, 'output')
        image_old = os.path.join(test_dir, 'output_static')

        # find images
        self.new_imgs = sorted(glob.glob(os.path.join(image_new, "*.tif")))
        self.old_imgs = sorted(glob.glob(os.path.join(image_old, "*.tif")))

        # if no images found, explain why
        if not self.new_imgs:
            raise Exception("No images in self.new_imgs; run 'func_test_ortho.py' to generate images")
        if not self.old_imgs:
            raise Exception("No images in self.old_imgs; create or populate 'output_static' directory with mosaics "
                            "using previous version of the codebase")

    # GE01
    def test_GE01_rf_image_equivalence(self):
        # select images
        target_image = 'GE01_11OCT122053047-P1BS-10504100009FD100_u08rf3031.tif'
        new = get_images(self.new_imgs, target_image)
        old = get_images(self.old_imgs, target_image)

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # IK01
    def test_IK01_rf_image_equivalence(self):
        # select images
        target_image = 'IK01_20050319201700_2005031920171340000011627450_po_333838_msi_0000000_u08rf3413.tif'
        new = get_images(self.new_imgs, target_image)
        old = get_images(self.old_imgs, target_image)

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # QB02
    def test_QB02_rf_image_equivalence(self):
        # select images
        target_image = 'QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006_u08rf3413.tif'
        new = get_images(self.new_imgs, target_image)
        old = get_images(self.old_imgs, target_image)

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # WV01
    def test_WV01_rf_image_equivalence(self):
        # select images
        target_image = 'WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007_u08rf3413.tif'
        new = get_images(self.new_imgs, target_image)
        old = get_images(self.old_imgs, target_image)

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # WV02
    def test_WV02_rf_image_equivalence(self):
        # select images
        target_image = \
            'WV02_20100423190859_1030010005C7AF00_10APR23190859-M2AS_R1C1-052462689010_01_P001_u08rf26910.tif'
        new = get_images(self.new_imgs, target_image)
        old = get_images(self.old_imgs, target_image)

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))

    # WV03
    def test_WV03_rf_image_equivalence(self):
        # select images
        target_image = 'WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002_u08rf3413.tif'
        new = get_images(self.new_imgs, target_image)
        old = get_images(self.old_imgs, target_image)

        self.assertEqual(True, np.all(img_as_array(old) == img_as_array(new)))


class TestOverlapCheck(unittest.TestCase):
    def setUp(self):
        self.srcdir = os.path.join(test_dir, 'output')
        self.spatial_ref = utils.SpatialRef(3031)
        self.dem = os.path.join(test_dir, 'dem', 'ramp_lowres.tif')
        self.dem_none = None
        self.ortho_height = None
        self.wd = None
        self.skip_dem_overlap_check = True
        self.resolution = None
        self.rgb = False
        self.bgrn = False
        self.stretch = 'rf'

    def test_overlap_check(self):
        info = ortho_functions.ImageInfo()
        info.srcfn = 'WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004_u08rf3031.tif'
        info.localsrc = os.path.join(self.srcdir, info.srcfn)
        info, rc = ortho_functions.GetImageStats(self, info)

        overlap = ortho_functions.overlap_check(info.geometry_wkt, self.spatial_ref, self.dem)
        self.assertTrue(overlap)

    def test_overlap_check_no_dem(self):
        info = ortho_functions.ImageInfo()
        info.srcfn = 'WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004_u08rf3031.tif'
        info.localsrc = os.path.join(self.srcdir, info.srcfn)
        info, rc = ortho_functions.GetImageStats(self, info)

        overlap = ortho_functions.overlap_check(info.geometry_wkt, self.spatial_ref, self.dem_none)
        self.assertFalse(overlap)


class TestCalcEarthSunDist(unittest.TestCase):
    def setUp(self):
        self.year = 2010
        self.month = 10
        self.day = 20
        self.hour = 10
        self.minute = 20
        self.second = 10

    def test_calc_esd(self):
        esd = ortho_functions.calcEarthSunDist(self)
        self.assertEqual(esd, 0.9957508611980816)


class TestRPCHeight(unittest.TestCase):
    def setUp(self):
        self.srcdir = os.path.join(test_dir, 'ortho')

    def test_rpc_height_wv01(self):
        info = ortho_functions.ImageInfo()
        info.localsrc = os.path.join(self.srcdir,
                                     'WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.ntf')
        h = ortho_functions.get_rpc_height(info)
        self.assertEqual(h, 2568.0)

    def test_rpc_height_wv02(self):
        info = ortho_functions.ImageInfo()
        info.localsrc = os.path.join(self.srcdir,
                                     'WV02_20120719233558_103001001B998D00_12JUL19233558-M1BS-052754253040_01_P001.TIF')
        h = ortho_functions.get_rpc_height(info)
        self.assertEqual(h, 75.0)

    def test_rpc_height_wv03(self):
        info = ortho_functions.ImageInfo()
        info.localsrc = os.path.join(self.srcdir,
                                     'WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.NTF')
        h = ortho_functions.get_rpc_height(info)
        self.assertEqual(h, 149.0)

    def test_rpc_height_ge01(self):
        info = ortho_functions.ImageInfo()
        info.localsrc = os.path.join(self.srcdir,
                                     'GE01_20110307105821_1050410001518E00_11MAR07105821-P1BS-500657359080_01_P008.ntf')
        h = ortho_functions.get_rpc_height(info)
        self.assertEqual(h, 334.0)

    def test_rpc_height_ik01(self):
        info = ortho_functions.ImageInfo()
        info.localsrc = os.path.join(self.srcdir,
                                     'IK01_20050319201700_2005031920171340000011627450_po_333838_blu_0000000.ntf')
        h = ortho_functions.get_rpc_height(info)
        self.assertEqual(h, 520.0)

    def test_rpc_height_qb02(self):
        info = ortho_functions.ImageInfo()
        info.localsrc = os.path.join(self.srcdir,
                                     'QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF')
        h = ortho_functions.get_rpc_height(info)
        self.assertEqual(h, 45.0)

    def test_rpc_height_None(self):
        info = ortho_functions.ImageInfo()
        info.localsrc = os.path.join(self.srcdir,
                                     'QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF')
        info.localsrc = None
        h = ortho_functions.get_rpc_height(info)
        self.assertEqual(h, 0)


class ProcessArgs(object):
    def __init__(self,epsg,stretch):
        self.epsg = epsg
        self.resolution = None
        self.rgb = False
        self.bgrn = False
        self.skip_warp = False
        self.dem = None
        self.ortho_height = None
        self.resample = 'near'
        self.outtype = 'Byte'
        self.format = "GTiff"
        self.gtiff_compression = "lzw"
        self.stretch = stretch
        self.spatial_ref = utils.SpatialRef(self.epsg)


class ImageInfo(object):
    def __init__(self, fn, vendor=None, sat=None):
        self.srcfn = fn
        self.localsrc = os.path.join(os.path.join(test_dir, 'ortho', self.srcfn))
        self.localdst = os.path.join(os.path.join(test_dir, 'output', self.srcfn))
        self.vendor = vendor
        self.sat = sat


if __name__ == '__main__':
    
    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Test imagery_utils ortho_functions package"
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
        TestReadMetadata,
        TestWriteMetadata,
        TestCollectFiles,
        TestDEMOverlap,
        TestTargetExtent,
        TestOutputDataValues,
        TestRPCHeight,
        TestCalcEarthSunDist,
        TestOverlapCheck
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
