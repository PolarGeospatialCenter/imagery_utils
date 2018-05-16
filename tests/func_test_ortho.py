"""Runs pgc_ortho with a variety of images and input parameters to achieve test coverage."""
import unittest, os, sys, glob, shutil, argparse, logging, subprocess
import gdal, ogr, osr, gdalconst

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

from lib import ortho_functions

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
        # if os.path.isdir(self.dstdir):
        #     shutil.rmtree(self.dstdir)
        if not os.path.isdir(self.dstdir):
            os.makedirs(self.dstdir)

    #@unittest.skip("skipping")
    def test_image_types(self):
        """
        Runs the ortho script on most types of images, including images from
        all vendors, images with different band numbers, at different locations, etc.
        """
        
        test_images = [
            #(image_path, egsg)
            ('WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007.NTF',3413),
            ('WV02_20120719233558_103001001B998D00_12JUL19233558-M1BS-052754253040_01_P001.TIF',3413),
            ('WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004.NTF',3031),
            ('GE01_20110108171314_1016023_5V110108M0010160234A222000100252M_000500940.ntf',26914),
            ('WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.NTF',3413),
            ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF',3413),
            ('GE01_20140402211914_1050410010473600_14APR02211914-M1BS-053720734020_01_P003.NTF',3413),
            ('WV02_20100423190859_1030010005C7AF00_10APR23190859-M2AS_R1C1-052462689010_01_P001.NTF',26910),
            ('IK01_19991222080400_1999122208040550000011606084_po_82037_pan_0000000.tif',32636),
            ('IK01_20050319201700_2005031920171340000011627450_po_333838_blu_0000000.ntf',3413),
            ('QB02_20021009211710_101001000153C800_02OCT09211710-M2AS_R1C1-052075481010_01_P001.tif',3413),
            ('WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.ntf',3031),
            ('QB02_20070918204906_10100100072E5100_07SEP18204906-M3AS_R1C1-005656156020_01_P001.ntf',3413),
            ('WV02_20100804230742_1030010006A15800_10AUG04230742-M3DM_R1C3-052672098020_01_P001.tif',3413),
            ('GE01_11OCT122053047-P1BS-10504100009FD100.ntf',3031), #### GE01 image wth abscalfact in W/m2/um
            ('GE01_14APR022119147-M1BS-1050410010473600.ntf',3413), #### GE01 image wth abscalfact in W/cm2/nm            
        ]
        
        for test_image, epsg in test_images:
            
            srcfp = os.path.join(self.srcdir, test_image)
            cmd = r"""python "{}" --wd /local -r 10 -p {} "{}" "{}" """.format(self.scriptpath, epsg, srcfp,
                                                                               self.dstdir)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            se, so = p.communicate()
            print(so)
            print(se)
            
            
    def test_input_parameters(self):
        """
        Runs the ortho script on a single multispectral DigitalGlobe image with
        several combinations of input parameters. The pgctools2 scene_id of the
        image being tested is QB02_12AUG271322429-M1BS-10100100101AD000
        """
        
        cmds = [
            # epsg: 3413
            # stretch: ns
            # resample: cubic
            # format: GTiff
            # outtype: Byte
            # gtiff compression: jpeg95
            # dem: Y:/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif
            r"""python "{}" -r 10 --epsg 3413 --stretch ns --resample cubic --format GTiff --outtype Byte --gtiff-compression jpeg95 --dem /mnt/agic/storage00/agic/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif {}/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF {}"""
            .format(self.scriptpath, self.srcdir, self.dstdir),
           
            # epsg: 3413
            # stretch: rf
            # resample: near
            # format: ENVI
            # outtype: Byte
            # gtiff compression: lzw
            # dem: Y:/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif
            r"""python "{}" -r 10 --epsg 3413 --stretch rf --resample near --format ENVI --outtype Byte --gtiff-compression lzw --dem /mnt/agic/storage00/agic/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif {}/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF {}"""
            .format(self.scriptpath, self.srcdir, self.dstdir),
    
            # epsg: 3413
            # stretch: mr
            # resample: near
            # format: HFA
            # outtype: Float32
            # gtiff compression: lzw
            # dem: Y:/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif
            r"""python "{}" -r 10 --epsg 3413 --stretch mr --resample near --format HFA --outtype Float32 --gtiff-compression lzw --dem /mnt/agic/storage00/agic/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif {}/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF {}"""
            .format(self.scriptpath, self.srcdir, self.dstdir),
    
            # epsg: 3413
            # stretch: rd
            # resample: near
            # format: GTiff
            # outtype: UInt16
            # gtiff compression: lzw
            # dem: None
            r"""python "{}" -r 10 --epsg 3413 --stretch rd --resample near --format GTiff --outtype UInt16 --gtiff-compression lzw {}/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF {}"""
            .format(self.scriptpath, self.srcdir, self.dstdir),
        
            # dem: Y:/private/elevation/dem/RAMP/RAMPv2/ RAMPv2_wgs84_200m.tif
            # should fail: the image is not contained within the DEM
            r"""python "{}" -r 10 --epsg 3413 --dem /mnt/agic/storage00/agic/private/elevation/dem/RAMP/RAMPv2/RAMPv2_wgs84_200m.tif {}/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF {}"""
            .format(self.scriptpath, self.srcdir, self.dstdir)
        ]
        
        for cmd in cmds:
            p = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
            se,so = p.communicate()
            print so
            print se
            
    # def tearDown(self):
    #     shutil.rmtree(self.dstdir)


if __name__ == '__main__':
    
    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Test imagery_utils ortho package"
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
        TestOrthoFunc
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
