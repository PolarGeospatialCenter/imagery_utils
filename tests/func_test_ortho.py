"""Runs pgc_ortho with a variety of images and input parameters to achieve test coverage."""
import shutil
import unittest, os, sys, argparse, logging, subprocess
import platform

from setuptools import glob

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

logger = logging.getLogger("logger")
# lso = logging.StreamHandler()
# lso.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
# lso.setFormatter(formatter)
# logger.addHandler(lso)


class TestOrthoFunc(unittest.TestCase):

    def setUp(self):
        self.srcdir = os.path.join(os.path.join(test_dir, 'ortho'))
        self.scriptpath = os.path.join(root_dir, "pgc_ortho.py")
        self.dstdir = os.path.join(script_dir, 'testdata', 'output')

        if platform.system() == 'Windows':
            self.gimpdem = r'V:\pgc\data\elev\dem\gimp\GIMPv1\gimpdem_v1_30m.tif'
            self.rampdem = r'V:\pgc\data\elev\dem\ramp\RAMPv2_wgs84_200m.tif'
        else:
            self.gimpdem = '/mnt/pgc/data/elev/dem/gimp/GIMPv1/gimpdem_v1_30m.tif'
            self.rampdem = '/mnt/pgc/data/elev/dem/ramp/RAMPv2_wgs84_200m.tif'

        # if os.path.isdir(self.dstdir):
        #     shutil.rmtree(self.dstdir)
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    # @unittest.skip("skipping")
    def test_image_types(self):
        
        test_images = [
            # (image_path, egsg, result)
            ('GE01_11OCT122053047-P1BS-10504100009FD100.ntf', 3031, True), #### GE01 image wth abscalfact in W/m2/um
            ('GE01_14APR022119147-M1BS-1050410010473600.ntf', 3413, True), #### GE01 image wth abscalfact in W/cm2/nm
            ('GE01_20110108171314_1016023_5V110108M0010160234A222000100252M_000500940.ntf', 26914, True),
            ('GE01_20140402211914_1050410010473600_14APR02211914-M1BS-053720734020_01_P003.ntf', 3413, True),
            ('IK01_19991222080400_1999122208040550000011606084_po_82037_pan_0000000.tif', 32636, False),  # Corrupt
            ('IK01_20050319201700_2005031920171340000011627450_po_333838_blu_0000000.ntf', 3413, False),  # Corrupt
            ('QB02_20021009211710_101001000153C800_02OCT09211710-M2AS_R1C1-052075481010_01_P001.tif', 3413, False),
            ('QB02_20070918204906_10100100072E5100_07SEP18204906-M3AS_R1C1-005656156020_01_P001.ntf', 3413, False),
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.ntf', 3413, True),
            ('WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.ntf', 3031, True),
            ('WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007.ntf', 3413, True),
            ('WV02_20100423190859_1030010005C7AF00_10APR23190859-M2AS_R1C1-052462689010_01_P001.ntf', 26910, True),
            ('WV02_20100804230742_1030010006A15800_10AUG04230742-M3DM_R1C3-052672098020_01_P001.tif', 3413, False),
            ('WV02_20120719233558_103001001B998D00_12JUL19233558-M1BS-052754253040_01_P001.tif', 3413, True),
            ('WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004.ntf', 3031, True),
            ('WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.ntf', 3413, True),
            ('WV03_20190114103353_104C0100462B2500_19JAN14103353-C1BA-502817502010_01_P001.ntf', 3031, True)
        ]
        
        for test_image, epsg, result in test_images:
            
            srcfp = os.path.join(self.srcdir, test_image)
            dstfp = os.path.join(self.dstdir, '{}_u08rf{}.tif'.format(
                os.path.splitext(test_image)[0], epsg))
            print(srcfp)
            cmd = r"""python "{}" -r 10 -p {} "{}" "{}" """.format(
                self.scriptpath, epsg, srcfp, self.dstdir)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            se, so = p.communicate()
            # print(so)
            # print(se)
            self.assertTrue(os.path.isfile(dstfp) == result)

    def test_input_parameters(self):
        
        cmds = [
            # (file name, arg string, should succeed, output extension)
            # epsg: 3413
            # stretch: mr
            # resample: cubic
            # outtype: Byte
            # gtiff compression: jpeg95
            # dem: gimpdem_v2_30m.tif
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.ntf',
             f'-r 10 --epsg 3413 --stretch mr --resample cubic --format GTiff --outtype Byte --gtiff-compression jpeg95 --dem {self.gimpdem}',
             True,
             '.tif'),

            # --rgb with Geoeye image
            # epsg: 3413
            # stretch: rf
            # outtype: Byte
            # gtiff compression: jpeg95
            ('GE01_20110108171314_1016023_5V110108M0010160234A222000100252M_000500940.ntf',
             '-r 10 --epsg 3413 --stretch mr --rgb --format GTiff --outtype Byte --gtiff-compression jpeg95',
             True,
             '.tif'),

            # ns, rgb, and Byte with CAVIS image
            # epsg: auto
            # stretch: ns
            # outtype: Byte
            # gtiff compression: jpeg95
            ('WV03_20190114103353_104C0100462B2500_19JAN14103353-C1BA-502817502010_01_P001.ntf',
             '-r 10 --epsg auto --stretch ns --rgb --format GTiff --outtype Byte --gtiff-compression jpeg95',
             True,
             '.tif'),

            # --rgb with SWIR image
            # epsg: 3413
            # stretch: auto
            ('WV03_20150712212305_104A01000E7C1F00_15JUL12212305-A1BS-500802261010_01_P001.ntf',
             '-r 10 --epsg 3413 --stretch au --rgb --format GTiff --outtype Byte --gtiff-compression jpeg95',
             True,
             '.tif'),

            # epsg: 3413
            # stretch: rf
            # resample: near
            # format: ENVI
            # outtype: Byte
            # gtiff compression: lzw
            # dem: gimpdem_v2_30m.tif
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.ntf',
             f'-r 10 --epsg 3413 --stretch rf --resample near --format ENVI --outtype Byte --gtiff-compression lzw --dem {self.gimpdem}',
             True,
             '.envi'),

            # epsg: 3413
            # stretch: rf
            # resample: near
            # format: .img
            # outtype: Float32
            # gtiff compression: lzw
            # dem: Y:/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.ntf',
             f'-r 10 --epsg 3413 --stretch rf --resample near --format HFA --outtype Float32 --gtiff-compression lzw --dem {self.gimpdem}',
             True,
             '.img'),

            # epsg: 3413
            # stretch: rd
            # outtype: UInt16
            # format: .jp2
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.ntf',
             '-r 10 --epsg 3413 --stretch rd --resample near --format JP2OpenJPEG --outtype UInt16 --gtiff-compression lzw',
             True,
             '.jp2'),

            # dem: Y:/private/elevation/dem/RAMP/RAMPv2/RAMPv2_wgs84_200m.tif
            # should fail: the image is not contained within the DEM
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.ntf',
             f'-r 10 --epsg 3413 --dem {self.rampdem}',
             False,
             '.tif'),

            # stretch, dem, and epsg: auto
            ('WV02_20120719233558_103001001B998D00_12JUL19233558-M1BS-052754253040_01_P001.tif',
             f'-r 10 --skip-cmd-txt --epsg auto --stretch au --dem auto',
             True,
             '.tif'),
        ]

        i = 0
        for fn, test_args, succeeded, ext in cmds:
            i += 1
            _dstdir = os.path.join(self.dstdir, str(i))
            os.mkdir(_dstdir)
            print(fn)
            cmd = f'python "{self.scriptpath}" {test_args} {self.srcdir}/{fn} {_dstdir}'
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            se, so = p.communicate()
            # print(so)
            # print(se)
            output_files = glob.glob(os.path.join(_dstdir, f'{os.path.splitext(fn)[0]}*{ext}'))
            self.assertEqual(len(output_files) > 0, succeeded)

    def tearDown(self):
        shutil.rmtree(self.dstdir)


if __name__ == '__main__':
    
    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Test imagery_utils ortho package"
        )

    parser.add_argument('--testdata',
                        help="test data directory (default is testdata folder within script directory)")

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
        TestOrthoFunc
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
