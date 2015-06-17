import unittest, os, sys, glob, shutil, argparse, logging
import gdal, ogr, osr, gdalconst

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

from lib import ortho_utils

logger = logging.getLogger("logger")
#lso = logging.StreamHandler()
#lso.setLevel(logging.DEBUG)
#formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
#lso.setFormatter(formatter)
#logger.addHandler(lso)

class TestMetadata(unittest.TestCase):
    
    def setUp(self):
	self.stretch = 'rf'
	self.rd_stretch = 'rd'
    
    #@unittest.skip("skipping")
    def test_parse_DG_md_files(self):
	
	dg_files = (
	    ##(file name, if passes)
	    ('10APR23190859-M2AS-052462689010_01_P001.XML',True), ## 2A unrenamed
	    ('12JUL19233558-M1BS-052754253040_01_P001.XML',True), ## 1B unrenamed
	    ('12AUG27132242-M1BS-500122876080_01_P006.XML',False), ## 1B unrenamed truncated xml
	    ('GE01_11OCT122053047-P1BS-10504100009FD100.xml',True), #### GE01 image wth abscalfact in W/m2/um
	    ('GE01_14APR022119147-M1BS-1050410010473600.xml',True), #### GE01 image wth abscalfact in W/cm2/nm
	    ('GE01_20140402211914_1050410010473600_14APR02211914-M1BS-053720734020_01_P003.XML',True), ##GE01 pgctools3 name
	    ('QB02_02OCT092117107-M2AS_R1C1-101001000153C800.xml',True), #2A tiled pgctools2 renamed
	    ('QB02_12AUG271322429-M1BS-10100100101AD000.xml',True),  #1B pgctools2 renamed
	    ('QB02_20021009211710_101001000153C800_02OCT09211710-M2AS_R1C1-052075481010_01_P001.xml',True),
	    ('QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.XML',True),
	    ('WV01_09OCT042222158-P1BS-1020010009B33500.xml',True),
	    ('WV01_12MAR262229422-P1BS-102001001B02FA00.xml',True),
	    ('WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.xml',True),
	    ('WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007.XML',True),
	    ('WV02_10APR231908590-M2AS_R1C1-1030010005C7AF00.xml',True),
	    ('WV02_10APR231908590-M2AS_R2C3-1030010005C7AF00.xml',True),
	    ('WV02_12JUL192335585-M1BS-103001001B998D00.xml',True),
	    ('WV02_13OCT050528024-P1BS-10300100278D8500.xml',True),
	    ('WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004.XML',True),
	    ('WV03_14SEP192129471-M1BS-104001000227BF00.xml',True),
	    ('WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.XML',True),
	)
	
	for mdf, result in dg_files:  ### test reflectance
	    metapath = os.path.join(test_dir,'metadata_files',mdf)
	    calib_dict = ortho_utils.getDGXmlData(metapath,self.stretch)
	    self.assertEqual(bool(calib_dict),result)
	    if calib_dict:
		if 'BAND_P' in calib_dict:
		    self.assertGreater(calib_dict['BAND_P'],0.0005)
		    self.assertLess(calib_dict['BAND_P'],0.0015)
		if 'BAND_B' in calib_dict:
		    self.assertGreater(calib_dict['BAND_B'],0.00045)
		    self.assertLess(calib_dict['BAND_B'],0.001)
		if 'BAND_B' in calib_dict and 'BAND_G' in calib_dict: ### check bands are not equal
		    self.assertNotEqual(calib_dict['BAND_B'],calib_dict['BAND_G'])
		    
	for mdf, result in dg_files:   ### test radiance
	    metapath = os.path.join(test_dir,'metadata_files',mdf)
	    calib_dict = ortho_utils.getDGXmlData(metapath,self.rd_stretch)
	    self.assertEqual(bool(calib_dict),result)
	    if calib_dict:
		#print calib_dict
		if 'BAND_P' in calib_dict:
		    self.assertGreater(calib_dict['BAND_P'],0.08)
		    self.assertLess(calib_dict['BAND_P'],0.15)
		if 'BAND_B' in calib_dict:
		    self.assertGreater(calib_dict['BAND_B'],0.17)
		    self.assertLess(calib_dict['BAND_B'],0.33)
		if 'BAND_B' in calib_dict and 'BAND_G' in calib_dict: ### check bands are not equal
		    self.assertNotEqual(calib_dict['BAND_B'],calib_dict['BAND_G'])
		    
	    
    #@unittest.skip("skipping")	    
    def test_parse_GE_md_files(self):
	ge_files = (
	    ('GE01_110108M0010160234A222000100252M_000500940.txt',True),
	)
	
	for mdf, result in ge_files: ### test reflectance
	    metapath = os.path.join(test_dir,'metadata_files',mdf)
	    calib_dict = ortho_utils.GetGEcalibDict(metapath,self.stretch)
	    self.assertEqual(bool(calib_dict),result)
	    if calib_dict:
		
		if 5 in calib_dict:  # pan band
		    self.assertGreater(calib_dict[5],0.0002)
		    self.assertLess(calib_dict[5],0.0008)
		if 1 in calib_dict:  # blue band
		    self.assertGreater(calib_dict[1],0.0002)
		    self.assertLess(calib_dict[1],0.0008)
		if 1 in calib_dict and 2 in calib_dict: ### check bands are not equal
		    self.assertNotEqual(calib_dict[1],calib_dict[2])
		    
	for mdf, result in ge_files:  ### test radiance
	    metapath = os.path.join(test_dir,'metadata_files',mdf)
	    calib_dict = ortho_utils.GetGEcalibDict(metapath,self.rd_stretch)
	    self.assertEqual(bool(calib_dict),result)
	    if calib_dict:
		
		if 5 in calib_dict:  # pan band
		    self.assertGreater(calib_dict[5],0.01)
		    self.assertLess(calib_dict[5],0.02)
		if 1 in calib_dict:  # blue band
		    self.assertGreater(calib_dict[1],0.01)
		    self.assertLess(calib_dict[1],0.02)
		if 1 in calib_dict and 2 in calib_dict: ### check bands are not equal
		    self.assertNotEqual(calib_dict[1],calib_dict[2])
	
    def test_parse_IK_md_files(self):
	
	ik_files = (
	    ('IK01_20010602215300_2001060221531300000010031227_po_387877_metadata.txt',True), ## test IK metadata file with multiple source IDs
	    ('IK01_19991222080400_1999122208040550000011606084_po_82037_metadata.txt',True),  ## test pgctools3 name
	    ('IK01_20050319201700_2005031920171340000011627450_po_333838_metadata.txt',True), ## test pgctools3 name
	    ('IK01_1999122208040550000011606084_pan_1569N.txt',True), ## test pgctools2 name
	    ('IK01_2005031920171340000011627450_rgb_5817N.txt',True), ## test pgctools2 name
	)
	
	for mdf, result in ik_files:  ### test reflectance
	    metapath = os.path.join(test_dir,'metadata_files',mdf)
	    calib_dict = ortho_utils.GetIKcalibDict(metapath,self.stretch)
	    self.assertEqual(bool(calib_dict),result)
	    if calib_dict:
		#print mdf
		#print calib_dict
		if 4 in calib_dict:  # pan band
		    self.assertGreater(calib_dict[4],0.0004)
		    self.assertLess(calib_dict[4],0.0006)
		if 0 in calib_dict:  # blue band
		    self.assertGreater(calib_dict[0],0.0003)
		    self.assertLess(calib_dict[0],0.0006)
		if 0 in calib_dict and 1 in calib_dict: ### check bands are not equal
		    self.assertNotEqual(calib_dict[0],calib_dict[1])
		    
	for mdf, result in ik_files:    ### test radiance
	    metapath = os.path.join(test_dir,'metadata_files',mdf)
	    calib_dict = ortho_utils.GetIKcalibDict(metapath,self.rd_stretch)
	    self.assertEqual(bool(calib_dict),result)
	    if calib_dict:
		
		if 4 in calib_dict:  # pan band
		    self.assertGreater(calib_dict[4],0.1)
		    self.assertLess(calib_dict[4],0.16)
		if 0 in calib_dict:  # blue band
		    self.assertGreater(calib_dict[0],0.15)
		    self.assertLess(calib_dict[0],0.25)
		if 0 in calib_dict and 1 in calib_dict: ### check bands are not equal
		    self.assertNotEqual(calib_dict[0],calib_dict[1])
		    
class TestCollectFiles(unittest.TestCase):
    
    def test_gather_metadata_file(self):

	for root, dirs, files in os.walk(test_dir):
	    for f in files:
		if f.lower().endswith(".ntf") or f.lower().endswith(".tif"):
		    #print f
		    #### Find metadata file
		    srcfp = os.path.join(root,f)
		    
		    metafile = ortho_utils.GetDGMetadataPath(srcfp)
		    if metafile is None:
			metafile = ortho_utils.GetIKMetadataPath(srcfp)
		    if metafile is None:
			metafile = ortho_utils.GetGEMetadataPath(srcfp)
		    self.assertIsNotNone(metafile)
			
    
if __name__ == '__main__':
    
    
    #### Set Up Arguments
    parser = argparse.ArgumentParser(
	description="Test stereo_utils package"
	)

    parser.add_argument('--testdata', help="test data directory (default is testdata folder within script directory)")

    #### Parse Arguments
    args = parser.parse_args()
    global test_dir
    
    if args.testdata:
	test_dir = os.path.abspath(args.testdata)
    else:
	test_dir = os.path.join(script_dir,'testdata')
    
    if not os.path.isdir(test_dir):
	parser.error("Test data folder does not exist: %s" %test_dir)
	
    test_cases = [
	TestMetadata,
	TestCollectFiles
	
    ]
    
    suites = []
    for test_case in test_cases:
	suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
	suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)