"""Runs pgc_ortho with a variety of images and input parameters to achieve test coverage."""
import subprocess
import sys
import os

def image_type_tests(pgc_ortho_script_path, test_imagery_directory, output_dir):
    """
    Runs the ortho script on most types of images, including images from
    all vendors, images with different band numbers, at different locations, etc.
    """
    # test info: WV01, 1 band, Arctic
    # pgctools2 name: WV01_12MAR262229422-P1BS-102001001B02FA00
    
    test_images = [
        #(image_path, egsg)
        ('renamed_pgctools3/IK01_2001060221531300000010031227_P2AS_387877/IK01_20010602215300_2001060221531300000010031227_po_387877_blu_0020000.ntf',3338), # tests ikonos metadata with multiple source ids
        ('renamed_pgctools3/WV01_102001001B02FA00_P1BS_052596100010_03/WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007.NTF',3413),
        ('renamed_pgctools3/WV02_103001001B998D00_M1BS_052754253040_01/WV02_20120719233558_103001001B998D00_12JUL19233558-M1BS-052754253040_01_P001.TIF',3413),
        ('renamed_pgctools3/WV02_10300100278D8500_P1BS_500099283010_01/WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004.NTF',3031),
        ('renamed_pgctools3/GE01_1016023_M4AM_000500940/GE01_20110108171314_1016023_5V110108M0010160234A222000100252M_000500940.ntf',4326),
        ('renamed_pgctools3/WV03_104001000227BF00_M1BS_500191821040_01/WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.NTF',3413),
        ('renamed_pgctools3/QB02_10100100101AD000_M1BS_500122876080_01/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF',3413),
        ('renamed_pgctools3/GE01_1050410010473600_M1BS_053720734020_01/GE01_20140402211914_1050410010473600_14APR02211914-M1BS-053720734020_01_P003.NTF',4326),
        ('renamed_pgctools3/WV02_1030010005C7AF00_M2AS_052462689010_01/WV02_20100423190859_1030010005C7AF00_10APR23190859-M2AS_R1C1-052462689010_01_P001.NTF',4326),
        ('renamed_pgctools3/IK01_1999122208040550000011606084_P1BS_82037/IK01_19991222080400_1999122208040550000011606084_po_82037_pan_0000000.tif',4326),
        ('renamed_pgctools3/IK01_2005031920171340000011627450_M1BS_333838/IK01_20050319201700_2005031920171340000011627450_po_333838_blu_0000000.ntf',3413),
        ('renamed_pgctools3/QB02_101001000153C800_M2AS_052075481010_01/QB02_20021009211710_101001000153C800_02OCT09211710-M2AS_R1C1-052075481010_01_P001.tif',3413),
        ('renamed_pgctools3/WV01_1020010009B33500_P1BS_052532098020_01/WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.ntf',3031),
        ('renamed_pgctools3/QB02_10100100072E5100_M3AS_005656156020_01/QB02_20070918204906_10100100072E5100_07SEP18204906-M3AS_R1C1-005656156020_01_P001.ntf',3413),
        ('renamed_pgctools3/WV02_1030010006A15800_M3DM_052672098020_01/WV02_20100804230742_1030010006A15800_10AUG04230742-M3DM_R1C3-052672098020_01_P001.tif',3413),
        ('renamed_pgctools2/GE01_11OCT122053047-P1BS-10504100009FD100.ntf',3413), #### GE01 image wth abscalfact in W/m2/um
        ('renamed_pgctools2/GE01_14APR022119147-M1BS-1050410010473600.ntf',3413), #### GE01 image wth abscalfact in W/cm2/nm
        
    ]
    
    for test_image,epsg in test_images:
        
        test_image_path = os.path.join(test_imagery_directory,test_image)
        command = r"""python "%s" --wd /local -p %d "%s" "%s" """ %(pgc_ortho_script_path, epsg, test_image_path, output_dir)
        print command
        subprocess.call(command, shell=True)



def input_parameter_tests_dg(pgc_ortho_script_path, test_imagery_directory, output_dir):
    """
    Runs the ortho script on a single multispectral DigitalGlobe image with
    several combinations of input parameters. The pgctools2 scene_id of the
    image being tested is QB02_12AUG271322429-M1BS-10100100101AD000
    """

    # epsg: 3413
    # stretch: ns
    # resample: cubic
    # format: GTiff
    # outtype: Byte
    # gtiff compression: jpeg95
    # dem: Y:/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif
    command = r"""python "%s" --epsg 3413 --stretch ns --resample cubic --format GTiff --outtype Byte --gtiff_compression jpeg95 --dem /mnt/agic/storage00/agic/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif %s/renamed_pgctools3/QB02_10100100101AD000_M1BS_500122876080_01/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    print command
    subprocess.call(command, shell=True)

    # epsg: 3413
    # stretch: rf
    # resample: near
    # format: ENVI
    # outtype: Byte
    # gtiff compression: lzw
    # dem: Y:/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif
    command = r"""python "%s" --epsg 3413 --stretch rf --resample near --format ENVI --outtype Byte --gtiff_compression lzw --dem /mnt/agic/storage00/agic/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif %s/renamed_pgctools3/QB02_10100100101AD000_M1BS_500122876080_01/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    print command
    subprocess.call(command, shell=True)

    # epsg: 3413
    # stretch: mr
    # resample: near
    # format: HFA
    # outtype: Float32
    # gtiff compression: lzw
    # dem: Y:/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif
    command = r"""python "%s" --epsg 3413 --stretch mr --resample near --format HFA --outtype Float32 --gtiff_compression lzw --dem /mnt/agic/storage00/agic/private/elevation/dem/GIMP/GIMPv2/gimpdem_v2_30m.tif %s/renamed_pgctools3/QB02_10100100101AD000_M1BS_500122876080_01/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    print command
    subprocess.call(command, shell=True)

    # epsg: 3413
    # stretch: rd
    # resample: near
    # format: GTiff
    # outtype: UInt16
    # gtiff compression: lzw
    # dem: None
    command = r"""python "%s" --epsg 3413 --stretch rd --resample near --format GTiff --outtype UInt16 --gtiff_compression lzw %s/renamed_pgctools3/QB02_10100100101AD000_M1BS_500122876080_01/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    print command
    subprocess.call(command, shell=True)

    # dem: Y:/private/elevation/dem/RAMP/RAMPv2/ RAMPv2_wgs84_200m.tif
    # should fail: the image is not contained within the DEM
    command = r"""python "%s" --epsg 3413 --dem /mnt/agic/storage00/agic/private/elevation/dem/RAMP/RAMPv2/RAMPv2_wgs84_200m.tif %s/renamed_pgctools3/QB02_10100100101AD000_M1BS_500122876080_01/QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    print command
    subprocess.call(command, shell=True)


if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    pgc_ortho_script_path = os.path.join(os.path.dirname(script_dir),'pgc_ortho.py')
    test_imagery_directory = os.path.join(script_dir,'testdata')
    output_directory = os.path.join(script_dir,'testdata','output')
    
    image_type_tests(pgc_ortho_script_path, test_imagery_directory, output_directory)
    input_parameter_tests_dg(pgc_ortho_script_path, test_imagery_directory, output_directory)
