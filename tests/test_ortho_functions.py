import platform
import unittest, os, sys
from osgeo import gdal, ogr
from collections import namedtuple

__test_dir__ = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(__test_dir__))
testdata_dir = os.path.join(__test_dir__, 'testdata')

from lib import ortho_functions, utils
from lib import VERSION

Band_data_range = namedtuple('Band_data_range', ['factor_min', 'factor_max', 'offset_min', 'offset_max'])


class TestReadMetadata(unittest.TestCase):
    
    def setUp(self):
        self.stretch = 'rf'
        self.rd_stretch = 'rd'
        self.srcdir = os.path.join(testdata_dir, 'metadata_files')
    
    # @unittest.skip("skipping")
    def test_parse_DG_md_files(self):
        
        dg_files = (
            ##(file name, is readable, is usable)
            ('10APR23190859-M2AS-052462689010_01_P001.xml', True, True), ## 2A unrenamed
            ('12JUL19233558-M1BS-052754253040_01_P001.xml', True, True), ## 1B unrenamed
            ('12AUG27132242-M1BS-500122876080_01_P006.xml', False, False), ## 1B unrenamed truncated xml
            ('GE01_11OCT122053047-P1BS-10504100009FD100.xml', True, True), #### GE01 image wth abscalfact in W/m2/um
            ('GE01_14APR022119147-M1BS-1050410010473600.xml', True, True), #### GE01 image wth abscalfact in W/cm2/nm
            ('GE01_20140402211914_1050410010473600_14APR02211914-M1BS-053720734020_01_P003.xml', True, True), ##GE01 pgctools3 name
            ('QB02_02OCT092117107-M2AS_R1C1-101001000153C800.xml', True, True), #2A tiled pgctools2 renamed
            ('QB02_12AUG271322429-M1BS-10100100101AD000.xml', True, True),  #1B pgctools2 renamed
            ('QB02_20021009211710_101001000153C800_02OCT09211710-M2AS_R1C1-052075481010_01_P001.xml', True, True),
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.xml', True, True),
            ('WV01_09OCT042222158-P1BS-1020010009B33500.xml', True, True),
            ('WV01_12MAR262229422-P1BS-102001001B02FA00.xml', True, True),
            ('WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.xml', True, True),
            ('WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007.xml', True, True),
            ('WV02_10APR231908590-M2AS_R1C1-1030010005C7AF00.xml', True, True),
            ('WV02_10APR231908590-M2AS_R2C3-1030010005C7AF00.xml', True, True),
            ('WV02_12JUL192335585-M1BS-103001001B998D00.xml', True, True),
            ('WV02_13OCT050528024-P1BS-10300100278D8500.xml', True, True),
            ('WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004.xml', True, True),
            ('WV03_14SEP192129471-M1BS-104001000227BF00.xml', True, True),
            ('WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.xml', True, True),
            ('QB02_20050623212833_1010010004535800_05JUN23212833-P2AS-005511498020_01_P001.xml', True, False), # uses EARLIESTACQTIME instead of FIRSTLINETIME, but has no EFFECTIVEBANDWIDTH
            ('WV03_20150411220541_104A01000A704D00_15APR11220541-A1BS-500802194040_01_P001.xml', True, True), # SWIR
            ('WV03_20150526221639_104A01000C51A100_15MAY26221639-A1BS-500802200030_01_P001.xml', True, True), # SWIR
            ('WV03_20190114103353_104C0100462B2500_19JAN14103353-C1BA-502817502010_01_P001.xml', True, True), # CAVIS A
            ('WV03_20190114103355_104C0100462B2500_19JAN14103355-C1BB-502817502010_01_P001.xml', True, True), # CAVIS B
        )

        dg_valid_data_range = {
            'rf': {
                'BAND_P': (Band_data_range(0.0005, 0.0015, -0.036, 0)),  # pan
                'BAND_B': (Band_data_range(0.000447, 0.0012, -0.1307, 0.0012)),  # blue
                'BAND_S1': (Band_data_range(0.00005, 0.0015, 0, 0)),  # 1st SWIR band
            },
            'rd': {
                'BAND_P': (Band_data_range(0.08, 0.15, -5.55, 0)),
                'BAND_B': (Band_data_range(0.17, 0.33,  -9.84, 0)),
                'BAND_S1': (Band_data_range(0.0045, 0.15, 0, 0)),
            }
        }

        dg_test_bands = ['BAND_P', 'BAND_B', 'BAND_S1', 'BAND_DC', 'BAND_A1']

        for stretch in [self.stretch, self.rd_stretch]:
            stretch_params_method(self, dg_files, stretch, dg_valid_data_range, dg_test_bands,
                                  utils.get_dg_metadata_as_xml, ortho_functions.get_dg_calib_dict, )

    def test_parse_GE_md_files(self):
        ge_files = (
            ('GE01_110108M0010160234A222000100252M_000500940.txt', True, True),
        )

        ge_valid_data_range = {
            'rf': {
                5: (Band_data_range(0.0002, 0.0008, 0, 0)),  # pan
                1: (Band_data_range(0.0002, 0.0008, 0, 0)),  # blue
            },
            'rd': {
                5: (Band_data_range(0.08, 0.18, 0, 0)),
                1: (Band_data_range(0.14, 0.33, 0, 0)),
            }
        }

        ge_test_bands = None

        for stretch in [self.stretch, self.rd_stretch]:
            stretch_params_method(self, ge_files, stretch, ge_valid_data_range, ge_test_bands,
                                  utils.get_ge_metadata_as_xml, ortho_functions.get_ge_calib_dict)

    def test_parse_IK_md_files(self):
        
        ik_files = (
            ('IK01_20010602215300_2001060221531300000010031227_po_387877_metadata.txt', True, True), ## test IK metadata file with multiple source IDs
            ('IK01_19991222080400_1999122208040550000011606084_po_82037_metadata.txt', True, True),  ## test pgctools3 name
            ('IK01_20050319201700_2005031920171340000011627450_po_333838_metadata.txt', True, True), ## test pgctools3 name
            ('IK01_1999122208040550000011606084_pan_1569N.txt', True, True), ## test pgctools2 name
            ('IK01_2005031920171340000011627450_rgb_5817N.txt', True, True), ## test pgctools2 name
        )

        ik_valid_data_range = {
            'rf': {
                4: (Band_data_range(0.0004, 0.0007, 0, 0)),  # pan
                0: (Band_data_range(0.0003, 0.0007, 0, 0)),  # blue
            },
            'rd': {
                4: (Band_data_range(0.1, 0.16, 0, 0)),
                0: (Band_data_range(0.15, 0.25, 0, 0)),
            }
        }

        ik_test_bands = None

        for stretch in [self.stretch, self.rd_stretch]:
            stretch_params_method(self, ik_files, stretch, ik_valid_data_range, ik_test_bands,
                                  utils.get_ik_metadata_as_xml, ortho_functions.get_ik_calib_dict)


def stretch_params_method(test_obj, file_list, stretch, valid_data_range, test_bands, meta_function, calib_function):
    # Test stretch factor and offset
    metadata_files = [(os.path.join(test_obj.srcdir, m), r1, r2) for m, r1, r2 in file_list]
    for mdf, is_readable, is_usable in metadata_files:
        # print(f'{mdf}: {is_readable} {is_usable}')
        metad = None
        calib_dict = {}
        img_name = os.path.basename(mdf).replace('metadata.txt', 'blu_0000000.ntf')
        _, _, _, _, _, regex = utils.get_sensor(img_name)
        try:
            metad = meta_function(mdf)
        except utils.InvalidMetadataError as e:
            pass
        if metad:
            try:
                if calib_function == ortho_functions.get_ik_calib_dict:
                    calib_dict = calib_function(metad, mdf, regex, stretch)
                else:
                    calib_dict = calib_function(metad, stretch)
            except utils.InvalidMetadataError as e:
                pass
        test_obj.assertEqual(bool(metad), is_readable)
        test_obj.assertEqual(bool(calib_dict), is_usable)
        if calib_dict:
            if test_bands:
                # For DG, exactly one of the listed bands should be present
                t = [b for b in test_bands if b in calib_dict]
                test_obj.assertEqual(len(t), 1)
            # Check band values are not equal
            all_band_factors = [f for f, b in calib_dict.values()]
            test_obj.assertEqual(len(all_band_factors), len(set(all_band_factors)))
            # Check stretch values are within reasonable limits
            bdr_dict = valid_data_range[stretch]
            for band, bdr in bdr_dict.items():
                if band in calib_dict:
                    test_obj.assertGreaterEqual(calib_dict[band][0], bdr.factor_min)
                    test_obj.assertLessEqual(calib_dict[band][0], bdr.factor_max)
                    test_obj.assertGreaterEqual(calib_dict[band][1], bdr.offset_min)
                    test_obj.assertLessEqual(calib_dict[band][1], bdr.offset_max)


class TestWriteMetadata(unittest.TestCase):

    def setUp(self):
        self.epsg = '4326'
        self.stretch = 'rf'
        self.srcfn = 'WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.ntf'
        self.srcfp = os.path.join(testdata_dir, 'ortho', self.srcfn)
        self.dstdir = os.path.join(__test_dir__, 'tmp_output')
        self.dstfp = os.path.join(self.dstdir, '{}_u08{}{}.tif'.format(
            os.path.splitext(self.srcfn)[0],
            self.stretch,
            self.epsg))
        self.mf = f'{os.path.splitext(self.dstfp)[0]}.xml'

        self.test_lines = [
            f'<BITDEPTH>Byte</BITDEPTH>',
            f'<COMPRESSION>lzw</COMPRESSION>',
            f'<EPSG_CODE>{self.epsg}</EPSG_CODE>',
            f'<FORMAT>GTiff</FORMAT>',
            f'<STRETCH>{self.stretch}</STRETCH>',
            f'<VERSION>imagery_utils v{VERSION}</VERSION>',
            f'<RESAMPLEMETHOD>near</RESAMPLEMETHOD>'
        ]
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    def test_write_DG_md_file(self):
        test_args = ProcessArgs(self.epsg, self.stretch)
        info = ortho_functions.ImageInfo(self.srcfp, self.dstdir, self.dstdir, test_args)
        rc = ortho_functions.write_output_metadata(test_args, info)
        ## read meta and check content
        f = open(self.mf)
        contents = f.read()
        f.close()
        for test_line in self.test_lines:
            self.assertTrue(test_line in contents)
    
    def tearDown(self):
        if os.path.isfile(self.mf):
            os.remove(self.mf)


class TestCollectFiles(unittest.TestCase):

    def test_gather_metadata_file(self):

        rm_files = [
                '01JAN08QB020800008JAN01102125-P1BS-005590467020_01_P001_________AAE_0AAAAABAABA0.xml'
            ]
        skip_list = [
                '01JAN08QB020800008JAN01102125-P1BS-005590467020_01_P001_________AAE_0AAAAABAABA0.ntf' # tar has an issue
            ]

        for root, dirs, files in os.walk(os.path.join(testdata_dir, 'ortho')):
            for f in files:
                if (f.lower().endswith(".ntf") or f.lower().endswith(".tif")) and f not in skip_list:
                    #### Find metadata file
                    stretch = 'rf'
                    epsg = '4326'
                    srcfp = os.path.join(root, f)
                    dstdir = os.path.join(__test_dir__, 'tmp_output')
                    dstfp = os.path.join(dstdir, '{}_u08{}{}.tif'.format(
                        os.path.splitext(f)[0],
                        stretch,
                        epsg))
                    test_args = ProcessArgs(epsg, stretch)
                    info = ortho_functions.ImageInfo(srcfp, dstfp, dstdir, test_args)
                    self.assertIsNotNone(info.metapath)

                    if info.metapath and os.path.basename(info.metapath) in rm_files:
                        os.remove(info.metapath)


class TestDEMOverlap(unittest.TestCase):
    
    def setUp(self):
        self.dem = os.path.join(os.path.join(testdata_dir, 'dem', 'grimp_200m.tif')) # dem for greenland
        self.srs = utils.SpatialRef(4326)
    
    def test_dem_overlap(self):
        image_geom_wkts = [
            ('POLYGON ((-52.23 -80.843333, -51.735 -80.844444, -51.736667 -80.760556, -52.23 -80.759722, -52.23 -80.843333))',
             False),  # False
            ('POLYGON ((-52.23 70.843333, -51.735 70.844444, -51.736667 70.760556, -52.23 70.759722, -52.23 70.843333))',
            True),  # True
            ('POLYGON ((-52.23 -50.843333, -51.735 -50.844444, -51.736667 -50.760556, -52.23 -50.759722, -52.23 -50.843333))',
            False)  # False
        ]
        
        for wkt, result in image_geom_wkts:
            test_result = ortho_functions.overlap_check(wkt, self.srs, self.dem)
            self.assertEqual(test_result, result)


class TestAutoDEMOverlap(unittest.TestCase):

    def setUp(self):
        self.gpkg = os.path.join(os.path.join(testdata_dir, 'auto_dem', 'dems_list.gpkg'))
        self.srs = utils.SpatialRef(4326)

    def test_auto_dem_overlap(self):
        image_geom_wkts = [
            # polygon, linux path
            ('POLYGON ((-52.23 70.843333, -51.735 70.844444, -51.736667 70.760556, -52.23 70.759722, -52.23 70.843333))',
             '/mnt/pgc/data/elev/dem/gimp/GrIMPv2/data/grimp_v02.0_30m_dem.tif'), #greenland
            ('POLYGON ((-52.23 -80.843333, -51.735 -80.844444, -51.736667 -80.760556, -52.23 -80.759722, -52.23 -80.843333))',
             '/mnt/pgc/data/elev/dem/tandem-x/90m/mosaic/TanDEM-X_Antarctica_90m/TanDEMX_PolarDEM_90m.tif'), #antarctic
            ('POLYGON ((-52.23 -50.843333, -51.735 -50.844444, -51.736667 -50.760556, -52.23 -50.759722, -52.23 -50.843333))',
             None), # ocean
            ('POLYGON ((-52.3475 84.515555,-50.882 84.53222,-50.89833 84.40166,-52.330833 84.3844,-52.3475 84.515555))',
             None), # ocean
            ('POLYGON ((-49.23 61.910556, -47.735 58.844444, -47.735 61.910556, -49.23 58.844444,-49.23 61.910556))',
             None), # greenland centroid
            ('POLYGON ((11 -68.5, 11 -70, 12 -70, 12 -68.5, 11 -68.5))',
             None), # antarctic centroid
            ('POLYGON ((-49.23 59.7, -48.23 59.7,-47.23 58.7, -49.23 58.7,-49.23 59.7))',
             None), # greenland, but not contained
            ('POLYGON ((-56 -61, -55 -61, -55 -60,-56 -60, -56 -61))',
             None), # antarctic, but not contained
            ('POLYGON ((-89.43 81.53, -88.94 81.53, -88.94 81.33, -89.43 81.33, -89.43 81.53))',
             '/mnt/pgc/data/elev/dem/copernicus-dem-30m/mosaic/global/cop30_tiles_global_wgs84-height_nunatak.vrt'), #Centroid in copernicus layer which overlaps greenland layer
        ]

        windows_paths = {
            # linux path: windows path
            '/mnt/pgc/data/elev/dem/gimp/GrIMPv2/data/grimp_v02.0_30m_dem.tif':
                r'\\ad.umn.edu\geo\pgc\data\elev\dem\gimp\GrIMPv2\data\grimp_v02.0_30m_dem.tif',
            '/mnt/pgc/data/elev/dem/tandem-x/90m/mosaic/TanDEM-X_Antarctica_90m/TanDEMX_PolarDEM_90m.tif':
                r'\\ad.umn.edu\geo\pgc\data\elev\dem\tandem-x\90m\mosaic\TanDEM-X_Antarctica_90m\TanDEMX_PolarDEM_90m.tif',
            '/mnt/pgc/data/elev/dem/copernicus-dem-30m/mosaic/global/cop30_tiles_global_wgs84-height_nunatak.vrt':
                r'\\ad.umn.edu\geo\pgc\data\elev\dem\copernicus-dem-30m\mosaic\global\cop30_tiles_global_wgs84-height_windows.vrt'
        }

        for wkt, result in image_geom_wkts:
            test_result = ortho_functions.check_image_auto_dem(wkt, self.srs, self.gpkg)
            if result is not None and platform.system() == 'Windows':
                result = windows_paths[result]
            print(wkt, test_result, result)
            self.assertEqual(test_result, result)

    def test_auto_dem_invalid_gpkgs(self):
        image_geom_wkt = 'POLYGON ((-52.23 70.843333, -51.735 70.844444, -51.736667 70.760556, -52.23 70.759722, -52.23 70.843333))'

        gpkgs = [
            os.path.join(testdata_dir, 'auto_dem', 'invalid.gpkg'),
            os.path.join(testdata_dir, 'auto_dem', 'no_dempath.gpkg')
        ]

        for gpkg in gpkgs:
            assert(os.path.isfile(gpkg))
            with self.assertRaises(RuntimeError):
                ortho_functions.check_image_auto_dem(image_geom_wkt, self.srs, gpkg)


class TestTargetExtent(unittest.TestCase):
        
    def test_target_extent(self):
        epsg = '32629'
        stretch = 'rf'
        wkt = 'POLYGON ((810287 2505832,811661 2487415,807201 2487233,805772 2505802,810287 2505832))'
        fn = 'GE01_20110307105821_1050410001518E00_11MAR07105821-M1BS-500657359080_01_P008.ntf'
        srcfp = os.path.join(testdata_dir, 'ortho', fn)
        dstdir = os.path.join(__test_dir__, 'tmp_output')
        target_extent_geom = ogr.CreateGeometryFromWkt(wkt)
        test_args = ProcessArgs(epsg, stretch)
        info = ortho_functions.ImageInfo(srcfp, dstdir, dstdir, test_args)
        rc = info.get_image_stats(test_args)
        rc = info.set_extent_geom(target_extent_geom)
        self.assertEqual(info.extent,
                     '-te 805772.000000000000 2487233.000000000000 811661.000000000000 2505832.000000000000 ')


class TestAutoStretchAndEpsg(unittest.TestCase):

    def test_auto_stretch_and_epsg(self):
        test_files = (
            # file name, expected stretch, expected epsg
            ('WV03_20190114103353_104C0100462B2500_19JAN14103353-C1BA-502817502010_01_P001.ntf', 'rf', 3031),  # CAVIS
            ('WV03_20190114103355_104C0100462B2500_19JAN14103355-C1BB-502817502010_01_P001.ntf', 'rf', 3031),  # CAVIS
            ('WV03_20150526221639_104A01000C51A100_15MAY26221639-A1BS-500802200030_01_P001.ntf', 'rf', 3413),  # SWIR
            ('GE01_20110307105821_1050410001518E00_11MAR07105821-M1BS-500657359080_01_P008.ntf', 'mr', 32630),  # Nonpolar
            ('WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.ntf', 'mr', 3413),  # Arctic
            ('WV03_20181226170822_10400100468EFA00_18DEC26170822-M1BS-502826003080_01_P009.ntf', 'rf', 3031),  # Antarctic
        )

        for fn, out_stretch, out_epsg in test_files:
            in_epsg = 'auto'
            in_stretch = 'au'
            srcfp = os.path.join(testdata_dir, 'ortho', fn)
            dstdir = os.path.join(__test_dir__, 'tmp_output')
            test_args = ProcessArgs(in_epsg, in_stretch)
            info = ortho_functions.ImageInfo(srcfp, dstdir, dstdir, test_args)
            self.assertEqual(info.stretch, out_stretch)
            self.assertEqual(info.epsg, out_epsg)


class TestCalcEarthSunDist(unittest.TestCase):
    def setUp(self):
        self.year = 2010
        self.month = 10
        self.day = 20
        self.hour = 10
        self.minute = 20
        self.second = 10

    def test_calc_esd(self):
        esd = ortho_functions.calc_earth_sun_dist(self)
        self.assertEqual(esd, 0.9957508611980816)


class TestRPCHeight(unittest.TestCase):
    def setUp(self):
        self.epsg = 4326  # also test epsg as an integer
        self.stretch = 'rf'
        self.srcdir = os.path.join(testdata_dir, 'ortho')
        self.dstdir = os.path.join(__test_dir__, 'tmp_output')
        self.test_args = ProcessArgs(self.epsg, self.stretch)
        self.srcfns = [
            ('WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.ntf', 2568.0),
            ('WV02_20120719233558_103001001B998D00_12JUL19233558-M1BS-052754253040_01_P001.tif', 75.0),
            ('WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.ntf', 149.0),
            ('GE01_20110307105821_1050410001518E00_11MAR07105821-P1BS-500657359080_01_P008.ntf', 334.0),
            ('IK01_20050319201700_2005031920171340000011627450_po_333838_blu_0000000.ntf', 520.0),
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.ntf', 45.0)
        ]

    def test_rpc_height(self):
        for srcfn, test_h in self.srcfns:
            srcfp = os.path.join(self.srcdir, srcfn)
            info = ortho_functions.ImageInfo(srcfp, self.dstdir, self.dstdir, self.test_args)
            h = ortho_functions.get_rpc_height(info)
            self.assertEqual(h, test_h)


class ProcessArgs(object):
    def __init__(self, epsg='4326', stretch='rf'):
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
        self.tap = False
        self.wd = None
        self.epsg_utm_nad83 = False


if __name__ == '__main__':

    test_cases = [
        TestReadMetadata,
        TestWriteMetadata,
        TestCollectFiles,
        TestDEMOverlap,
        TestAutoDEMOverlap,
        TestTargetExtent,
        TestAutoStretchAndEpsg,
        TestRPCHeight,
        TestCalcEarthSunDist,
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
