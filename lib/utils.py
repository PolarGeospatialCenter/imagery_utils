
import contextlib
import copy
import glob
import logging
import math
import os
import re
import sys
import traceback
from datetime import datetime
from io import StringIO
from xml.etree import cElementTree as ET

import numpy as np
from osgeo import gdal, ogr, osr

try:
    import collections.abc as collectionsAbc
except ImportError:
    import collections as collectionsAbc

gdal.SetConfigOption('GDAL_PAM_ENABLED', 'NO')

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

@contextlib.contextmanager
def capture_stdout_stderr():
    oldout, olderr = sys.stdout, sys.stderr
    out = [StringIO(), StringIO()]
    try:
        sys.stdout, sys.stderr = out
        yield out
    finally:
        sys.stdout, sys.stderr = oldout, olderr
        out[0] = out[0].getvalue()
        out[1] = out[1].getvalue()


def capture_error_trace():
    with capture_stdout_stderr() as out:
        traceback.print_exc()
    caught_out, caught_err = out
    return caught_err


class InvalidArgumentError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)

class InvalidMetadataError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)

class SpatialRef(object):

    def __init__(self, epsg):
        srs = osr_srs_preserve_axis_order(osr.SpatialReference())

        try:
            epsgcode = int(epsg)
        except ValueError:
            raise RuntimeError("EPSG value must be an integer: {}".format(epsg))
        else:
            # test epsg code
            err = srs.SetFromUserInput("EPSG:{}".format(epsgcode))
            if err != 0:
                # test esri code
                err = srs.SetFromUserInput("ESRI:{}".format(epsgcode))
                if err != 0:
                    raise RuntimeError("Invalid EPSG/ERSI code: {}".format(epsgcode))
                else:
                    proj4_string = srs.ExportToProj4()
            else:
                proj4_string = srs.ExportToProj4()

        proj4_patterns = {
            "+ellps=GRS80 +towgs84=0,0,0,0,0,0,0": "+datum=NAD83",
            "+ellps=WGS84 +towgs84=0,0,0,0,0,0,0": "+datum=WGS84",
        }

        for pattern, replacement in proj4_patterns.items():
            if proj4_string.find(pattern) != -1:
                proj4_string = proj4_string.replace(pattern, replacement)

        self.srs = srs
        self.proj4 = proj4_string
        self.epsg = epsgcode


def osr_srs_preserve_axis_order(osr_srs):
    try:
        # revert to GDAL 2.x axis conventions to maintain consistent results if GDAL 3+ used
        osr_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    except AttributeError:
        pass
    return osr_srs


def get_bit_depth(outtype):
    if outtype == "Byte":
        bitdepth = 'u08'
    elif outtype == "UInt16":
        bitdepth = "u16"
    elif outtype == "Float32":
        bitdepth = "f32"
    else:
        logger.error("Invalid bit depth '%s' supplied; must be 'Byte', 'UInt16', or 'Float32'.", outtype)
        return None

    return bitdepth


def get_sensor(srcfn):

    ### Regex signatures to identify file vendor and sensor
    RAW_DG = "(?P<ts>\d\d[a-z]{3}\d{8})-(?P<prod>\w{4})?(?P<tile>\w+)?-(?P<oid>\d{12}_\d\d)_(?P<pnum>p\d{3})"

    RENAMED_DG = "(?P<snsr>\w\w\d\d)_(?P<ts>\d\d[a-z]{3}\d{9})-(?P<prod>\w{4})?(?P<tile>\w+)?-(?P<catid>[a-z0-9]+)"

    RENAMED_DG2 = "(?P<snsr>\w\w\d\d)_(?P<ts>\d{14})_(?P<catid>[a-z0-9]{16})"

    RAW_GE = "(?P<snsr>\d[a-z])(?P<ts>\d{6})(?P<band>[a-z])(?P<said>\d{9})(?P<prod>\d[a-z])(?P<pid>\d{3})(?P<siid>\d{8})(?P<ver>\d)(?P<mono>[a-z0-9])_(?P<pnum>\d{8,9})"

    RENAMED_GE = "(?P<snsr>\w\w\d\d)_(?P<ts>\d{6})(?P<band>\w)(?P<said>\d{9})(?P<prod>\d\w)(?P<pid>\d{3})(?P<siid>\d{8})(?P<ver>\d)(?P<mono>\w)_(?P<pnum>\d{8,9})"

    RAW_IK = "po_(?P<po>\d{5,7})_(?P<band>[a-z]+)_(?P<cmp>\d+)"

    RENAMED_IK = "(?P<snsr>[a-z]{2}\d\d)_(?P<ts>\d{12})(?P<siid>\d+)_(?P<band>[a-z]+)_(?P<lat>\d{4}[ns])"

    sat = None
    vendor = None

    DG_patterns = [RAW_DG, RENAMED_DG, RENAMED_DG2]
    GE_patterns = [RAW_GE, RENAMED_GE]
    IK_patterns = [RAW_IK, RENAMED_IK]

    for pattern in DG_patterns:
        p = re.compile(pattern)
        m = p.search(srcfn.lower())
        if m is not None:
            vendor = "DigitalGlobe"
            gd = m.groupdict()
            if 'snsr' in gd:
                sat = gd['snsr']

    for pattern in GE_patterns:
        p = re.compile(pattern)
        m = p.search(srcfn.lower())
        if m is not None:
            vendor = "GeoEye"
            sat = "GE01"

    for pattern in IK_patterns:
        p = re.compile(pattern)
        m = p.search(srcfn.lower())
        if m is not None:
            vendor = "GeoEye"
            sat = "IK01"

    return vendor, sat.upper()


def find_images(inpath, is_filelist, target_exts):

    image_list = []
    if is_filelist:
        if type(inpath) is str:
            with open(inpath, 'r') as textfile_fp:
                inpath = textfile_fp.read().splitlines()
        for image in inpath:
            if os.path.isfile(image) and os.path.splitext(image)[1].lower() in target_exts:
                image_list.append(image)
            else:
                logger.debug("File in textfile does not exist or has an invalid extension: %s", image)

    else:
        for root, dirs, files in os.walk(inpath):
            for f in files:
                if os.path.splitext(f)[1].lower() in target_exts:
                    image_path = os.path.join(root, f)
                    image_path = image_path.replace('\\', '/')
                    image_list.append(image_path)

    return image_list


def find_images_with_exclude_list(inpath, is_filelist, target_exts, exclude_list):

    image_list = []

    if is_filelist is True:
        if type(inpath) is str:
            with open(inpath, 'r') as textfile_fp:
                inpath = textfile_fp.read().splitlines()
        for line in inpath:
            image = line.rstrip('\n').rstrip('\r')
            if os.path.isfile(image) and os.path.splitext(image)[1].lower() in target_exts:
                image_list.append(image)
            else:
                logger.info("File in textfile does not exist or has an invalid extension: %s", image)

    else:
        for root, dirs, files in os.walk(inpath):
            for f in files:
                if os.path.splitext(f)[1].lower() in target_exts:
                    image_path = os.path.join(root, f)
                    image_path = image_path.replace('\\', '/')
                    image_list.append(image_path)

    #print(len(exclude_list))
    if len(exclude_list) > 0:

        image_list2 = []
        for image in image_list:
            exclude = [pattern for pattern in exclude_list if image in pattern]
            if exclude:
                logger.debug("Scene ID matches pattern in exclude_list: %s", image)
            else:
                image_list2.append(image)

        return image_list2

    else:
        return image_list


def delete_temp_files(names):

    for name in names:
        deleteList = glob.glob(os.path.splitext(name)[0] + '.*')
        for f in deleteList:
            if "log" not in os.path.basename(f):
                try:
                    os.remove(f)
                except Exception as e:
                    logger.error(capture_error_trace())
                    logger.warning('Could not remove %s: %s', os.path.basename(f), e)


def get_dg_metadata_as_xml(metafile):
    if os.path.isfile(metafile):
        try:
            metaf = open(metafile, "r")
        except IOError as err:
            raise InvalidMetadataError(f"Cannot open metadata file: {metafile}: {err}")
    else:
        raise InvalidMetadataError(f"Metadata file does not exist: {metafile}")


def get_ge_metadata_as_xml(metafile):
    if os.path.isfile(metafile):
        try:
            metaf = open(metafile, "r")
        except IOError as err:
            raise InvalidMetadataError(f"Cannot open metadata file: {metafile}: {err}")
    else:
        raise InvalidMetadataError(f"Metadata file does not exist: {metafile}")

    # Patterns to extract tag/value pairs and BEGIN/END group tags
    gepat1 = re.compile(r'(?P<tag>\w+) = "?(?P<data>.*?)"?;', re.I)
    gepat2 = re.compile(r"(?P<tag>\w+) = ", re.I)

    # These tags use the following tag/value as an attribute of the group rather than
    # a standalone node
    group_tags = {"aoiGeoCoordinate": "coordinateNumber",
                  "aoiMapCoordinate": "coordinateNumber",
                  "bandSpecificInformation": "bandNumber"}

    # Start processing
    root = ET.Element("root")
    parent = None
    current = root
    node_stack = []
    mlstr = False  # multi-line string flag

    for line in metaf:
        # mlstr will be true when working on a multi-line string
        if mlstr:
            if not line.strip() == ");":
                data += line.strip()
            else:
                data += line.strip()
                child = ET.SubElement(current, tag)
                child.text = data
                mlstr = False

        # Handle tag/value pairs and groups
        mat1 = gepat1.search(line)
        if mat1:
            tag = mat1.group("tag").strip()
            data = mat1.group("data").strip()

            if tag == "BEGIN_GROUP":
                if data is None or data == "":
                    child = ET.SubElement(current, "group")
                else:
                    child = ET.SubElement(current, data)
                if parent:
                    node_stack.append(parent)
                parent = current
                current = child
            elif tag == "END_GROUP":
                current = parent if parent else root
                parent = node_stack.pop() if node_stack else None
            else:
                if current.tag in group_tags and tag == group_tags[current.tag]:
                    current.set(tag, data)
                else:
                    child = ET.SubElement(current, tag)
                    child.text = data
        else:
            mat2 = gepat2.search(line)
            if mat2:
                tag = mat2.group("tag").strip()
                data = ""
                mlstr = True

    metaf.close()
    return ET.ElementTree(root)


def getIKMetadataAsXml(metafile):
    """
    Given IKONOS metadata file, returns all the key/pair values as a
    searchable XML tree
    """
    if os.path.isfile(metafile):
        try:
            metaf = open(metafile, "r")
        except IOError as err:
            raise InvalidMetadataError(f"Cannot open metadata file: {metafile}: {err}")
    else:
        raise InvalidMetadataError(f"Metadata file does not exist: {metafile}")

    # Patterns to identify tag/value pairs and group tags
    ikpat1 = re.compile(r"(?P<tag>.+?): (?P<data>.+)?", re.I)
    ikpat2 = re.compile(r"(?P<tag>[a-zA-Z ()]+)", re.I)

    # Lists of tags known to be at a certain depth of the tree, to be used as
    # attributes rather than nodes or ignored altogether
    tags_1L = ["Product_Order_Metadata", "Source_Image_Metadata", "Product_Space_Metadata",
               "Product_Component_Metadata"]
    tags_2L = ["Source_Image_ID", "Component_ID"]
    tags_coords = [
        "Latitude",
        "Longitude",
        "Map_X_Easting",
        "Map_Y_Northing",
        "UL_Map_X_Easting",
        "UL_Map_Y_Northing",
        "Pan_Cross_Scan",
        "Pan_Along_Scan",
        "MS_Cross_Scan",
        "MS_Along_Scan",
        "Hemisphere",
        "Zone_Number",
    ]
    ignores = [
        "Company Information",
        "Address",
        "GeoEye",
        "12076 Grant Street",
        "Thornton, Colorado 80241",
        "U.S.A.",
        "Contact Information",
        "On the Web: http://www.geoeye.com",
        "Customer Service Phone (U.S.A.): 1.800.232.9037",
        "Customer Service Phone (World Wide): 1.703.480.5670",
        "Customer Service Fax (World Wide): 1.703.450.9570",
        "Customer Service Email: info@geoeye.com",
        "Customer Service Center hours of operation:",
        "Monday - Friday, 8:00 - 20:00 Eastern Standard Time"
    ]

    # Start processing
    root = ET.Element("root")
    parent = None
    current = root
    node_stack = []

    for line in metaf:
        item = line.strip()
        if item in ignores:
            continue  # Skip this stuff

        # Can't have spaces or slashes in node tags
        item = item.replace(" ", "_").replace("/", "_")

        # If we've found a top-level group name, handle it here
        if item in tags_1L:
            child = ET.SubElement(root, item)
            node_stack = []  # top-level nodes are children of root so reset
            parent = root
            current = child

        # Everything else
        else:
            mat1 = ikpat1.search(line)
            mat2 = ikpat2.search(line) if not mat1 else None

            # Tag/value pair
            if mat1:
                tag = mat1.group("tag").strip().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                if mat1.group("data"):
                    data = mat1.group("data").strip()
                else:
                    data = ""

                # Second-level groups define major blocks
                if tag in tags_2L:
                    # We may have been working on a different second-level tag, so
                    # reset the stack and pointers as needed
                    while current.tag not in tags_1L and current.tag != "root":
                        current = parent
                        parent = node_stack.pop()

                    # Now add the new child node
                    child = ET.SubElement(current, tag)
                    child.set("id", data)  # Currently, all 2L tags are IDs
                    node_stack.append(parent)
                    parent = current
                    current = child

                # Handle 'Coordinate' tags as a special case
                elif tag == "Coordinate":
                    # If we were working on a Coordinate, back up a level
                    if current.tag == "Coordinate":
                        child = ET.SubElement(parent, tag)
                        child.set("id", data)
                        current = child
                    else:
                        child = ET.SubElement(current, tag)
                        child.set("id", data)
                        node_stack.append(parent)
                        parent = current
                        current = child

                # Vanilla tag/value pair
                else:
                    # Adjust depth if we just finished a Coordinate block
                    if tag not in tags_coords and current.tag in ["Coordinate",
                                                                  "Component_Map_Coordinates_in_Map_Units",
                                                                  "Acquired_Nominal_GSD",
                                                                  "UTM_Specific_Parameters"]:
                        while current.tag not in tags_2L and current.tag not in tags_1L and current.tag != "root":
                            current = parent
                            parent = node_stack.pop()

                    # Add a standard node
                    child = ET.SubElement(current, tag)
                    child.text = data

            # Handle new group names
            elif mat2:
                tag = mat2.group("tag").strip().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")

                # Except for Coordinates there aren't really any 4th level tags we care about, so we always
                # back up until current points at a second or top-level node
                while current.tag not in tags_2L and current.tag not in tags_1L and current.tag != "root":
                    current = parent
                    parent = node_stack.pop()

                # Now add the new group node
                child = ET.SubElement(current, tag)
                node_stack.append(parent)
                parent = current
                current = child

    return ET.ElementTree(root)


def get_source_names(src_fp):
    """Get the source footprint name and layer name, if provided"""

    if src_fp.lower().endswith(".shp"):
        src_dsp = src_fp
        src_lyr = os.path.splitext(os.path.basename(src_fp))[0]
    elif ".gdb" in src_fp.lower() and not src_fp.lower().endswith(".gdb"):
        src_dsp, src_lyr = re.split(r"(?<=\.gdb)/", src_fp, re.I)
    else:
        msg = "The source {} does not appear to be a shapefile or File GDB".format(src_fp)
        raise RuntimeError(msg)

    return src_dsp, src_lyr


def doesCross180(geom):
    """
    Returns true if the geometry's polygon crosses 180 longitude

    :param geom: <osgeo.ogr.Geometry>
    :return: <bool>
    """
    if geom.GetGeometryName() == "MULTIPOLYGON":
        err = "Function does not support testing MULTIPOLYGON geometry"
        raise RuntimeError(err)

    result = False

    # Get an array of the longitudes of all the points of all the rings in the polygon
    x_coords = []
    for ring in geom:
        for pt in range(0, ring.GetPointCount()):
            x_coords.append(ring.GetX(pt))
    if x_coords:
        result = (max(x_coords) - min(x_coords)) > 180.0

    return result


def getWrappedGeometry(src_geom):
    """
    Change a single-polygon extent to multipart if it crosses 180 latitude
    Author: Claire Porter

    :param src_geom: <osgeo.ogr.Geometry>
    :return: <osgeo.ogr.Geometry> type wkbMultiPolygon
    """

    def calc_y_intersection_with_180(pt1, pt2):
        """
        Find y where x is 180 longitude

        :param pt1: <list> coordinate pair, as int or float
        :param pt2: <list> coordinate pair, int or float
        :return: <float>
        """
        # Add 360 to negative x coordinates
        pt1_x = pt1[0] + 360.0 if pt1[0] < 0.0 else pt1[0]
        pt2_x = pt2[0] + 360.0 if pt2[0] < 0.0 else pt2[0]

        rise = pt2[1] - pt1[1]      # Difference in y
        run = pt2_x - pt1_x         # Difference in x
        run_prime = 180.0 - pt1_x   # Difference in x to 180

        try:
            pt3_y = ((run_prime * rise) / run) + pt1[1]
        except ZeroDivisionError as err:
            raise RuntimeError(err)

        return pt3_y

    # Points lists for west and east components
    west_points = []
    east_points = []

    # Assume a single polygon, deconstruct to points, skipping last one
    ring_geom = src_geom.GetGeometryRef(0)
    for i in range(0, ring_geom.GetPointCount() - 1):
        pt1 = ring_geom.GetPoint(i)
        pt2 = ring_geom.GetPoint(i + 1)

        # Add point to appropriate bin (points on 0.0 go to east)
        if pt1[0] < 0.0:
            west_points.append(pt1)
        else:
            east_points.append(pt1)

        # Test if segment to next point crosses 180 (x is opposite sign)
        if (pt1[0] > 0) - (pt1[0] < 0) != (pt2[0] > 0) - (pt2[0] < 0):

            # If segment crosses, calculate y for the intersection point
            pt3_y = calc_y_intersection_with_180(pt1, pt2)

            # Add the intersection point to both bins (change 180 to -180 for west)
            west_points.append((-180.0, pt3_y))
            east_points.append((180.0, pt3_y))

    # Build a multipart polygon from the new point sets (repeat first point to close polygon)
    mp_geometry = ogr.Geometry(ogr.wkbMultiPolygon)

    for ring_points in (west_points, east_points):

        if len(ring_points) > 0:

            # Create the basic objects
            poly = ogr.Geometry(ogr.wkbPolygon)
            ring = ogr.Geometry(ogr.wkbLinearRing)

            # Add the points to the ring
            for pt in ring_points:
                ring.AddPoint(pt[0], pt[1])

            # Repeat the first point to close the ring
            ring.AddPoint(ring_points[0][0], ring_points[0][1])

            # Add the ring to the polygon and the polygon to the geometry
            poly.AddGeometry(ring)
            mp_geometry.AddGeometry(poly)

            # Clean up memory
            del poly
            del ring

    return mp_geometry


def write_task_bundles(task_list, tasks_per_bundle, dstdir, bundle_prefix,
                       header_list=None, task_delim=',', bundle_ext='txt'):

    jobnum_total = int(math.ceil(len(task_list) / float(tasks_per_bundle)))
    jobnum_fmt = '{:0>'+str(len(str(jobnum_total)))+'}'
    join_task_items = type(task_list[0]) in (tuple, list, np.ndarray)

    bundle_prefix = os.path.join(
        dstdir,
        '{}_{}_{}'.format(
            bundle_prefix, datetime.now().strftime("%Y%m%d%H%M%S"), os.getpid()
        )
    )
    bundle_file_list = []

    header_line = task_delim.join(header_list) if header_list is not None else None

    print("Writing task bundle text files in directory: {}".format(dstdir))
    for jobnum, tasknum in enumerate(range(0, len(task_list), tasks_per_bundle)):
        bundle_file = '{}_{}.{}'.format(bundle_prefix, jobnum_fmt.format(jobnum+1), bundle_ext)
        task_bundle = task_list[tasknum:tasknum+tasks_per_bundle]
        with open(bundle_file, 'w') as bundle_file_fp:
            if header_line is not None:
                bundle_file_fp.write(header_line+'\n')
            for task in task_bundle:
                task_line = str(task) if not join_task_items else task_delim.join([str(arg) for arg in task])
                bundle_file_fp.write(task_line+'\n')
        bundle_file_list.append(bundle_file)

    return bundle_file_list


def yield_task_args(task_list, script_args,
                    argname_1D=None,
                    argname_2D_list=None):
    """
    Takes a 1D or 2D list of ArgumentParser script argument values,
    each row of the list corresponding to a separate "task", and applies
    argument values to the provided ArgumentParser "args" namespace,
    yielding a copy of the namespace for each task where the argument
    values for that particular task are set in the namespace.
    Script argument values that are not modified as part of this operation
    remain unmodified in the yielded ArgumentParser namespaces.

    Parameters
    ----------
    task_list : collection, 1D or 2D
        Collection of script argument values for a list of tasks.
        If 2D, the outmost iterable (i.e. row) designates the task,
        while the innermost iterable (i.e. column) contains argument
        values corresponding to a single task.
    script_args : ArgumentParser argument namespace object
        ArgumentParser argument namespace object for the main script.
    argname_1D : string
        The name of the script argument to which the argument values
        in a 1D `task_list` correspond. If `task_list` is 2D, the value
        of this option is ignored. See info on the `argname_2D_list`
        option for more information.
    argname_2D_list : list of strings
        The names of script arguments, corresponding to the "columns"
        of the `task_list` argument values. Script argument names must be
        formatted as you would normally access them from the ArgumentParser
        namespace (no leading dashes, and dashes within the argument name
        should be converted to underscores).

    Yields
    ------
    task_args : ArgumentParser argument namespace object
        Clone of the `script_args` ArgumentParser argument namespace
        object, yielded for each task in `task_list`.
    """
    if len(task_list) == 0:
        return

    test_task = task_list[0]
    if isinstance(test_task, collectionsAbc.Iterable) and not isinstance(test_task, str):
        test_task_nargs = len(test_task)
    else:
        test_task_nargs = 1
        # If tasks have a CSV file path as the single argument and argname_1D
        # is provided, assume the CSV files themselves can be provided as the
        # argname_1D script argument.
        if (    argname_2D_list is not None and len(argname_2D_list) != 1
            and test_task.endswith('.csv') and argname_1D is not None):
            argname_2D_list = [argname_1D]

    if argname_2D_list is None:
        if argname_1D is None:
            raise InvalidArgumentError(
                "One of the following arguments must be provided: {}".format(
                    ','.join(["`{}`".format(arg) for arg in [
                        'argname_1D',
                        'argname_2D_list'
                    ]])
                ))
        else:
            argname_2D_list = [argname_1D]

    # Verify that the number of script argument names provided in argname_2D_list
    # (or single argument from argname_1D) matches the number of argument values in
    # each row of the task_list.
    if len(argname_2D_list) != test_task_nargs:
        raise InvalidArgumentError(
            "Number of expected arguments in task list ({}) does not match "
            "number of argument values found in task list ({})".format(
                len(argname_2D_list), test_task_nargs
            )
        )
    del test_task, test_task_nargs

    for task in task_list:
        # The script_args object from ArgumentParser is mutable,
        # so we must copy it before modifying.
        task_args = copy.copy(script_args)

        # Task list could be a 1D list of argument values (likely strings)
        # or could be a 2D list or NumPy array of argument values.
        # Convert 1D single-argument task to the multiple-argument
        # structure (a list with a single argument) for code simplicity.
        if isinstance(task, collectionsAbc.Iterable) and not isinstance(task, str):
            task_arg_list = task
        else:
            task_arg_list = [task]

        for i, argval in enumerate(task_arg_list):
            argname = argname_2D_list[i]

            if argval is None:
                argval = None
            elif isinstance(argval, str) and argval.lower() in ('none', ''):
                if argval == '':
                    continue
                else:
                    argval = None
            else:
                try:
                    argval = float(argval)
                    if int(argval) == argval:
                        argval = int(argval)
                except ValueError:
                    argval = 'r"{}"'.format(argval)

            exec_statement = 'task_args.{} = {}'.format(argname, argval)
            # print(exec_statement)
            exec(exec_statement)

        yield task_args


def subset_vrt_dem(csv_arg_data, csv_header_argname_list, script_args):
    """
    If source CSV argument list has tasks listed multiple times but with
    different argument DEM values, and the script DEM argument is a large
    VRT file composed of tiled DEMs with locations matching those specified
    at the task-level argument DEMs, create new VRT files from subsets of
    the larger script DEM VRT and have the tasks use those instead.
    This can significantly speed up GDAL operations over using a single,
    large (~20,000-element) VRT DEM.

    Subset VRT files are created in script argument 'scratch' directory.

    Parameters
    ----------
    csv_arg_data : NumPy array, 2D
        Source CSV argument list in NumPy array format, with header row removed.
    csv_header_argname_list : list of strings
        Header row of source CSV argument list, containing script argument names
        in ArgumentParser namescape variable format.
    script_args : ArgumentParser argument namespace object
        ArgumentParser argument namespace object for the main script.

    Returns
    -------
    csv_arg_data_trimmed : NumPy array, 2D
        Source CSV argument list in NumPy array format, with header row and
        duplicate task rows replaced with single tasks having updated DEM
        argument value to use new subset VRT files.
    """
    csv_col_idx_src = csv_header_argname_list.index('src')
    csv_col_idx_dem = csv_header_argname_list.index('dem')

    script_arg_vrt_dem = script_args.dem
    if not script_arg_vrt_dem.endswith('.vrt'):
        raise InvalidArgumentError(
            "Script DEM argument does not end with expected .vrt suffix: {}".format(script_arg_vrt_dem)
        )

    # Get the longest common prefix of all component DEMs
    # that make up the script argument VRT file.
    main_vrt_component_dem_prefix = []
    tree = ET.parse(script_arg_vrt_dem)
    root = tree.getroot()
    for sourceFilename in root.iter('SourceFilename'):
        dem_filename = sourceFilename.text
        main_vrt_component_dem_prefix.append(dem_filename)
        main_vrt_component_dem_prefix = [os.path.commonprefix(main_vrt_component_dem_prefix)]
    if len(main_vrt_component_dem_prefix) == 0:
        raise InvalidArgumentError(
            "Cannot find 'SourceFilename' elements in script DEM argument VRT file: {}".format(script_arg_vrt_dem)
        )
    else:
        main_vrt_component_dem_prefix = main_vrt_component_dem_prefix[0]

    # Parse CSV task argument values.
    # Each src can be listed multiple times, once for each DEM
    # it intersects with.
    vrt_src_set = set()
    vrt_dem_set = set()
    nonvrt_src_set = set()
    csv_src_keeprownum_dict = dict()
    vrt_src_dem_dict = dict()
    for rownum, task in enumerate(csv_arg_data):
        task_src = task[csv_col_idx_src]
        task_dem = task[csv_col_idx_dem]

        # Quick check if task DEM could be a component DEM of the main VRT,
        # and would be applicable for subsetting.
        if not task_dem.startswith(main_vrt_component_dem_prefix):
            nonvrt_src_set.add(task_src)
            csv_src_keeprownum_dict[task_src] = rownum
            continue

        vrt_src_set.add(task_src)
        vrt_dem_set.add(task_dem)
        if task_src not in vrt_src_dem_dict:
            vrt_src_dem_dict[task_src] = []
        vrt_src_dem_dict[task_src].append(task_dem)
        if task_src not in csv_src_keeprownum_dict:
            csv_src_keeprownum_dict[task_src] = rownum
        else:
            task_first_src = csv_arg_data[csv_src_keeprownum_dict[task_src]]
            task_first_src[csv_col_idx_dem] = task_dem
            if task_first_src.tolist() != task.tolist():
                raise InvalidArgumentError(
                    "Source CSV argument list rows {} and {} differ more than allowed "
                    "for VRT DEM subsetting".format(csv_src_keeprownum_dict[task_src], rownum)
                )

    invalid_src_set = set.intersection(vrt_src_set, nonvrt_src_set)
    if len(invalid_src_set) > 0:
        print("Source CSV argument list invalid 'src':\n{}".format('\n'.join(list(invalid_src_set))))
        raise InvalidArgumentError(
            "Source CSV argument list has duplicate 'src' tasks listed with DEMs "
            "not allowed for VRT DEM subsetting"
        )

    if len(vrt_src_set) == 0:
        return csv_arg_data

    # Trim CSV data array to first occurrence of unique 'src' arguments,
    # and tasks with DEM argument that is not part of a VRT subset.
    keep_rows_idx = sorted(list(csv_src_keeprownum_dict.values()))
    csv_arg_data_trimmed = csv_arg_data[np.asarray(keep_rows_idx)]
    vrt_dem_list = sorted(list(vrt_dem_set))

    subset_vrts_all = set()

    # It's likely that the same combination of DEMs is required
    # by multiple src images.
    # For each task src:
    #  - Derive a subset VRT filename for the particular combination of DEMs.
    #  - Change the DEM argument value for the task to the VRT filename.
    #  - For each DEM in the subset, add the VRT filename to a set of
    #    VRTs that need information for that DEM.
    process_time = datetime.now().strftime("%Y%m%d%H%M%S")
    process_pid = os.getpid()
    dem_vrts_dict = dict()
    for task in csv_arg_data_trimmed:
        task_src = task[csv_col_idx_src]
        if task_src in nonvrt_src_set:
            continue
        task_subset_dems = vrt_src_dem_dict[task_src]
        demgroupid = '-'.join([str(vrt_dem_list.index(dem)) for dem in sorted(task_subset_dems)])
        task_subset_vrt = os.path.join(
            script_args.scratch,
            'Or_dem_{}_{}_{}.vrt'.format(
                process_time, process_pid, demgroupid
            )
        )
        subset_vrts_all.add(task_subset_vrt)
        task[csv_col_idx_dem] = task_subset_vrt  # modifies mutable csv_arg_data
        for dem in task_subset_dems:
            if dem not in dem_vrts_dict:
                dem_vrts_dict[dem] = set()
            dem_vrts_dict[dem].add(task_subset_vrt)

    # Get all information subset VRT files need to contain beyond
    # the 'SimpleSource' information for each DEM in the subset.
    vrt_contents_prefix = ''
    with open(script_arg_vrt_dem, 'r') as main_vrt_fp:
        for line in main_vrt_fp.readlines():
            if '<SimpleSource>' in line:
                vrt_contents_prefix += "    "
                break
            vrt_contents_prefix += line
    vrt_contents_suffix = "</VRTRasterBand>\n</VRTDataset>\n"

    print("Writing subsets of VRT DEM in directory: {}".format(script_args.scratch))
    subset_vrts_started = set()

    # Loop through all 'SimpleSource' items in the main script VRT DEM.
    # If the 'SourceFilename' of the SimpleSource matches a DEM filename
    # needed by any of the subset VRT files, append its SimpleSource info
    # to all subset VRT files that DEM is part of.
    # Assume main VRT was built from a sorted text file list of component
    # DEM filenames, so that we only need to scan through the main VRT once.
    tree = ET.parse(script_arg_vrt_dem)
    root = tree.getroot()
    current_vrt_dem_idx = 0
    for simpleSource in root.iter('SimpleSource'):
        sourceFilename = simpleSource.find('SourceFilename')
        if sourceFilename is None:
            raise InvalidArgumentError(
                "Source CSV argument list 'SimpleSource' missing 'SourceFilename'"
            )
        dem_filename = sourceFilename.text
        if dem_filename == vrt_dem_list[current_vrt_dem_idx]:
            for vrt in dem_vrts_dict[dem_filename]:
                if vrt not in subset_vrts_started:
                    subset_vrts_started.add(vrt)
                    with open(vrt, 'a') as subset_vrt_fp:
                        subset_vrt_fp.write(vrt_contents_prefix)
                with open(vrt, 'a') as subset_vrt_fp:
                    subset_vrt_fp.write(ET.tostring(simpleSource, encoding='unicode', method='xml'))
            current_vrt_dem_idx += 1
            if current_vrt_dem_idx == len(vrt_dem_list):
                break

    if current_vrt_dem_idx != len(vrt_dem_list):
        raise InvalidArgumentError(
            "Could not find CSV DEM filename '{}' in scan of main VRT DEM (script argument) "
            "'SimpleSource/SourceFilename' elements. Make sure CSV DEM filenames match exactly "
            "the elements in the main VRT DEM, and that the VRT DEM was built from a SORTED "
            "text file list of component DEM filenames.".format(vrt_dem_list[current_vrt_dem_idx])
        )

    subset_vrts_not_started = subset_vrts_all - subset_vrts_started
    if len(subset_vrts_not_started) > 0:
        raise InvalidArgumentError(
            "For source CSV argument list, {} of {} subset VRT DEMs were not written. "
            "Make sure main VRT DEM (script argument) was built from a SORTED text file list "
            "of component DEM filenames.".format(len(subset_vrts_not_started), len(subset_vrts_all))
        )

    # Finish writing all subset VRT files
    for vrt in subset_vrts_started:
        with open(vrt, 'a') as subset_vrt_fp:
            subset_vrt_fp.write(vrt_contents_suffix)

    return csv_arg_data_trimmed

def write_input_command_txt(arg_str, dst_dir):
    logger.info("Processing command: {}".format(arg_str))
    now = datetime.now()
    base_cmd = os.path.splitext(os.path.basename(arg_str.split()[0]))[0]
    txt_fn = "{}_command_{}.txt".format(base_cmd, now.strftime("%Y%m%d_%H%M%S"))
    txt_fp = os.path.join(os.path.abspath(os.path.join(dst_dir, os.pardir)),txt_fn)
    try:
        with open(txt_fp, 'w') as f:
            f.write(arg_str)
    except:
        logger.error("Could not write command reference text file")
