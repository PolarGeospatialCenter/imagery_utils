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
    command = r"""python "%s" -p 3413 %s\WV01_102001001B02FA00_P1BS_052596100010_03\WV01_20120326222942_102001001B02FA00_12MAR26222942-P1BS-052596100010_03_P007.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: WV02, 8 band, Arctic, .tif
    # pgctools2 name: WV02_12JUL192335585-M1BS-103001001B998D00
    command = r"""python "%s" -p 3413 %s\WV02_103001001B998D00_M1BS_052754253040_01\WV02_20120719233558_103001001B998D00_12JUL19233558-M1BS-052754253040_01_P001.TIF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: WV02, 1 band, Antarctic
    # pgctools2 name: WV02_13OCT050528024-P1BS-10300100278D8500
    command = r"""python "%s" -p 3031 %s\WV02_10300100278D8500_P1BS_500099283010_01\WV02_20131005052802_10300100278D8500_13OCT05052802-P1BS-500099283010_01_P004.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: GE01/Geoeye, 4 band, Non-Polar
    # pgctools2 name: GE01_110108M0010160234A222000100252M_000500940
    command = r"""python "%s" -p 4326 %s\GE01_1016023_M4AM_000500940\GE01_20110108171314_1016023_5V110108M0010160234A222000100252M_000500940.ntf %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: WV03, 8 band, Arctic
    # pgctools2 name: WV03_14SEP192129471-M1BS-104001000227BF00
    command = r"""python "%s" -p 3413 %s\WV03_104001000227BF00_M1BS_500191821040_01\WV03_20140919212947_104001000227BF00_14SEP19212947-M1BS-500191821040_01_P002.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: QB02, 4 band, Arctic
    # pgctools2 name: QB02_12AUG271322429-M1BS-10100100101AD000
    command = r"""python "%s" -p 3413 %s\QB02_10100100101AD000_M1BS_500122876080_01\QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: GE01/DigitalGlobe, 4 band, Arctic
    # pgctools2 name: GE01_14APR022119147-M1BS-1050410010473600
    command = r"""python "%s" -p 3413 %s\GE01_1050410010473600_M1BS_053720734020_01\GE01_20140402211914_1050410010473600_14APR02211914-M1BS-053720734020_01_P003.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: WV02, Level 2A, 8 band, Non-Polar
    # pgctools2 name: WV02_10APR231908590-M2AS_R1C1-1030010005C7AF00
    command = r"""python "%s" -p 4326 %s\WV02_1030010005C7AF00_M2AS_052462689010_01\WV02_20100423190859_1030010005C7AF00_10APR23190859-M2AS_R1C1-052462689010_01_P001.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: IK01, 1 band, Non-Polar, .tif
    # pgctools2 name: IK01_1999122208040550000011606084_pan_1569N
    # should fail:  it's an Ikonos tif, and we can't get rpc's for those.
    command = r"""python "%s" -p 4326 %s\IK01_1999122208040550000011606084_P1BS_82037\IK01_19991222080400_1999122208040550000011606084_po_82037_pan_0000000.tif %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: IK01, 4 band, Arctic
    # pgctools2 name: IK01_2005031920171340000011627450_blu_5817N
    command = r"""python "%s" -p 3413 %s\IK01_2005031920171340000011627450_M1BS_333838\IK01_20050319201700_2005031920171340000011627450_po_333838_blu_0000000.ntf %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: QB02, 4 band, Arctic, Level 2A, .tif, tiled, fail block
    # ptctools2 name: QB02_02OCT092117100-M2AS_R1C1-101001000153C800
    # should fail: can't process 2A tiled tif's
    command = r"""python "%s" -p 3413 %s\QB02_101001000153C800_M2AS_052075481010_01\QB02_20021009211710_101001000153C800_02OCT09211710-M2AS_R1C1-052075481010_01_P001.tif %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: WV01, 1 band, Antarctic, crossing 180 longitude
    # ptctools2 name: WV01_09OCT042222158-P1BS-1020010009B33500
    command = r"""python "%s" -p 3031 %s\WV01_1020010009B33500_P1BS_052532098020_01\WV01_20091004222215_1020010009B33500_09OCT04222215-P1BS-052532098020_01_P019.ntf %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: QB02, 4 band, Arctic, product level 3*, fail block
    # ptctools2 name: QB02_07SEP182049060-M3AS_R1C1-10100100072E5100
    # should fail: can't process 3* products
    command = r"""python "%s" -p 3413 %s\QB02_10100100072E5100_M3AS_005656156020_01\QB02_20070918204906_10100100072E5100_07SEP18204906-M3AS_R1C1-005656156020_01_P001.ntf %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)

    # test info: WV02, 8 band, Arctic, mosaic, fail block
    # ptctools2 name: WV02_10AUG042307420-M3DM_R1C3-1030010006A15800
    # should fail: can't process mosaic products
    command = r"""python "%s" -p 3413 %s\WV02_1030010006A15800_M3DM_052672098020_01\WV02_20100804230742_1030010006A15800_10AUG04230742-M3DM_R1C3-052672098020_01_P001.tif %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir)
    subprocess.call(command, shell=True)


def input_parameter_tests_dg(pgc_ortho_script_path, test_imagery_directory, output_dir):
    """
    Runs the ortho script on a single multispectral DigitalGlobe image with
    several combinations of input parameters. The pgctools2 scene_id of the
    image being tested is QB02_12AUG271322429-M1BS-10100100101AD000
    """
    if not os.path.isdir(output_dir + r"\dg_input_params"):
        os.makedirs(output_dir + r"\dg_input_params")

    # epsg: 3413
    # stretch: ns
    # resample: cubic
    # format: GTiff
    # outtype: Byte
    # gtiff compression: jpeg95
    # dem: Y:\private\elevation\dem\GIMP\GIMPv2\gimpdem_v2_30m.tif
    command = r"""python "%s" --epsg 3413 --stretch ns --resample cubic --format GTiff --outtype Byte --gtiff_compression jpeg95 --dem Y:\private\elevation\dem\GIMP\GIMPv2\gimpdem_v2_30m.tif %s\QB02_10100100101AD000_M1BS_500122876080_01\QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir + r"\dg_input_params")
    subprocess.call(command, shell=True)

    # epsg: 3413
    # stretch: rf
    # resample: near
    # format: ENVI
    # outtype: Byte
    # gtiff compression: lzw
    # dem: Y:\private\elevation\dem\GIMP\GIMPv2\gimpdem_v2_30m.tif
    command = r"""python "%s" --epsg 3413 --stretch rf --resample near --format ENVI --outtype Byte --gtiff_compression lzw --dem Y:\private\elevation\dem\GIMP\GIMPv2\gimpdem_v2_30m.tif %s\QB02_10100100101AD000_M1BS_500122876080_01\QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir + r"\dg_input_params")
    subprocess.call(command, shell=True)

    # epsg: 3413
    # stretch: mr
    # resample: near
    # format: HFA
    # outtype: Float32
    # gtiff compression: lzw
    # dem: Y:\private\elevation\dem\GIMP\GIMPv2\gimpdem_v2_30m.tif
    command = r"""python "%s" --epsg 3413 --stretch mr --resample near --format HFA --outtype Float32 --gtiff_compression lzw --dem Y:\private\elevation\dem\GIMP\GIMPv2\gimpdem_v2_30m.tif %s\QB02_10100100101AD000_M1BS_500122876080_01\QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir + r"\dg_input_params")
    subprocess.call(command, shell=True)

    # epsg: 3413
    # stretch: rd
    # resample: near
    # format: GTiff
    # outtype: UInt16
    # gtiff compression: lzw
    # dem: None
    command = r"""python "%s" --epsg 3413 --stretch rd --resample near --format GTiff --outtype UInt16 --gtiff_compression lzw %s\QB02_10100100101AD000_M1BS_500122876080_01\QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir + r"\dg_input_params")
    subprocess.call(command, shell=True)

    # dem: Y:\private\elevation\dem\RAMP\RAMPv2\ RAMPv2_wgs84_200m.tif
    # should fail: the image is not contained within the DEM
    command = r"""python "%s" --epsg 3413 --dem Y:\private\elevation\dem\RAMP\RAMPv2\RAMPv2_wgs84_200m.tif %s\QB02_10100100101AD000_M1BS_500122876080_01\QB02_20120827132242_10100100101AD000_12AUG27132242-M1BS-500122876080_01_P006.NTF %s""" % (pgc_ortho_script_path, test_imagery_directory, output_dir + r"\dg_input_params")
    subprocess.call(command, shell=True)


if __name__ == '__main__':
    pgc_ortho_script_path = sys.argv[1]
    output_directory = sys.argv[2]

    pgc_ortho_script_dir = os.path.dirname(pgc_ortho_script_path)
    test_imagery_directory = os.path.join(pgc_ortho_script_dir, r"tests\ortho_test_data\renamed_pgctools3")

    image_type_tests(pgc_ortho_script_path, test_imagery_directory, output_directory)
    input_parameter_tests_dg(pgc_ortho_script_path, test_imagery_directory, output_directory)
