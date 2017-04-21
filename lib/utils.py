import os, string, sys, shutil, math, glob, re, tarfile, logging, platform, argparse, subprocess
from datetime import datetime, timedelta

import multiprocessing as mp
from xml.dom import minidom
from xml.etree import cElementTree as ET
import gdal, ogr, osr, gdalconst

gdal.SetConfigOption('GDAL_PAM_ENABLED', 'NO')

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

## Copy DEM global vars
deliv_suffixes = (
### ASP
'-DEM.prj',
'-DEM.tif',
'-DRG.tif',
'-IntersectionErr.tif',
'-GoodPixelMap.tif',
'-stereo.default',
'-PC.laz',
'-PC.las',
'.geojson',

### SETSM
'_dem.tif',
'_ortho.tif',
'_matchtag.tif',
'_meta.txt'
)

archive_suffix = ".tar"

shp_suffixes = (
                  '.shp',
                  '.shx',
                  '.prj',
                  '.dbf'
)

pc_suffixes = (
                  '-PC.tif',
                  '-PC-center.txt'
)

fltr_suffixes = (
                  '_fltr-DEM.tif',
                  '_fltr-DEM.prj'
)


log_suffixes = (
                  '-log-point2dem',
                  '-log-stereo_corr',
                   '-log-stereo_pprc',
                  '-log-stereo_fltr',
                  '-log-stereo_rfne',
                  '-log-stereo_tri'
)


class Task(object):

    def __init__(self, task_name, task_abrv, task_exe, task_cmd, task_method=None, task_method_arg_list=None):
        self.name = task_name
        self.abrv = task_abrv
        self.exe = task_exe
        self.cmd = task_cmd
        self.method = task_method
        self.method_arg_list = task_method_arg_list


class PBSTaskHandler(object):

    def __init__(self, qsubscript, qsub_args=""):

        ####  verify PBS is present by calling pbsnodes cmd
        try:
            cmd = "pbsnodes"
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            so, se = p.communicate()
        except OSError,e:
            raise RuntimeError("PBS job submission is not available on this system")

        self.qsubscript = qsubscript
        if not qsubscript:
            raise RuntimeError("PBS job submission requires a valid qsub script")
        elif not os.path.isfile(qsubscript):
            raise RuntimeError("Qsub script does not exist: {}".format(qsubscript))

        self.qsub_args = qsub_args

    def run_tasks(self, tasks):

        for task in tasks:
            cmd = r'qsub {} -N {} -v p1="{}" "{}"'.format(
                self.qsub_args,
                task.abrv,
                task.cmd,
                self.qsubscript
            )
            subprocess.call(cmd, shell=True)
            

class SLURMTaskHandler(object):

    def __init__(self, qsubscript, qsub_args=""):

        ####  verify PBS is present by calling pbsnodes cmd
        try:
            cmd = "sinfo"
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            so, se = p.communicate()
        except OSError,e:
            raise RuntimeError("SLURM job submission is not available on this system")

        self.qsubscript = qsubscript
        if not qsubscript:
            raise RuntimeError("SLURM job submission requires a valid qsub script")
        elif not os.path.isfile(qsubscript):
            raise RuntimeError("Qsub script does not exist: {}".format(qsubscript))

        self.qsub_args = qsub_args

    def run_tasks(self, tasks):

        for task in tasks:
            cmd = r'sbatch -J {} --export=p1="{}" "{}"'.format(
                task.abrv,
                task.cmd,
                self.qsubscript
            )
            subprocess.call(cmd, shell=True)


class ParallelTaskHandler(object):

    def __init__(self, num_processes=1):
        self.num_processes = num_processes
        if mp.cpu_count() < num_processes:
            raise RuntimeError("Specified number of processes ({0}) is higher than the system cpu count ({1})".format(num_proceses,mp.count_cpu()))
        elif num_processes < 1:
            raise RuntimeError("Specified number of processes ({0}) must be greater than 0, using default".format(num_proceses,mp.count_cpu()))

    def run_tasks(self, tasks):

        task_queue = [[task.name, self._format_task(task)] for task in tasks]
        pool = mp.Pool(self.num_processes)
        try:
            pool.map(exec_cmd_mp,task_queue,1)
        except KeyboardInterrupt:
            pool.terminate()
            raise RuntimeError("Processes terminated without file cleanup")

    def _format_task(self, task):
        _cmd = r'{} {}'.format(
            task.exe,
            task.cmd,
        )
        return _cmd


class SpatialRef(object):

    def __init__(self,epsg):
        srs = osr.SpatialReference()
        try:
            epsgcode = int(epsg)
        except ValueError, e:
            raise RuntimeError("EPSG value must be an integer: %s" %epsg)
        else:
            err = srs.ImportFromEPSG(epsgcode)
            if err == 7:
                raise RuntimeError("Invalid EPSG code: %d" %epsgcode)
            else:
                proj4_string = srs.ExportToProj4()

        proj4_patterns = {
            "+ellps=GRS80 +towgs84=0,0,0,0,0,0,0":"+datum=NAD83",
            "+ellps=WGS84 +towgs84=0,0,0,0,0,0,0":"+datum=WGS84",
        }

        for pattern, replacement in proj4_patterns.iteritems():
            if proj4_string.find(pattern) <> -1:
                proj4_string = proj4_string.replace(pattern,replacement)

        self.srs = srs
        self.proj4 = proj4_string
        self.epsg = epsgcode



def get_bit_depth(outtype):
    if outtype == "Byte":
        bitdepth = 'u08'
    elif outtype == "UInt16":
        bitdepth = "u16"
    elif outtype == "Float32":
        bitdepth = "f32"

    return bitdepth


def get_sensor(srcfn):

    ### Regex signatures to identify file vendor, mode, kind, and create the name_dict
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

    return vendor, sat


def exec_cmd(cmd):
    logger.info(cmd)

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (so,se) = p.communicate()
    rc = p.wait()
    err = 0

    if rc != 0:
        logger.error("Error found - Return Code = %s:  %s" %(rc,cmd))
        err = 1
    else:
        logger.debug("Return Code = %s:  %s" %(rc,cmd))

    logger.debug("STDOUT:  "+so)
    logger.debug("STDERR:  "+se)
    return (err,so,se)


def exec_cmd_mp(job):
    job_name, cmd = job
    logger.info('Running job: {0}'.format(job_name))
    logger.debug('Cmd: {0}'.format(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
    try:
        (so,se) = p.communicate()
    except KeyboardInterrupt:
        os.killpg(p.pid, signal.SIGTERM)

    else:
        logger.debug(so)
        logger.debug(se)


def find_images(inpath, is_textfile, target_exts):

    image_list = []
    if is_textfile:
        t = open(inpath,'r')
        for line in t.readlines():
            image = line.rstrip('\n').rstrip('\r')
            if os.path.isfile(image) and os.path.splitext(image)[1].lower() in target_exts:
                image_list.append(image)
            else:
                logger.debug("File in textfile does not exist or has an invalid extension: %s" %image)
        t.close()

    else:
        for root,dirs,files in os.walk(inpath):
            for f in  files:
                if os.path.splitext(f)[1].lower() in target_exts:
                    image_path = os.path.join(root,f)
                    image_path = string.replace(image_path,'\\','/')
                    image_list.append(image_path)

    return image_list


def find_images_with_exclude_list(inpath, is_textfile, target_exts, exclude_list):

    image_list = []

    if is_textfile is True:
        t = open(inpath,'r')
        for line in t.readlines():
            image = line.rstrip('\n').rstrip('\r')
            if os.path.isfile(image) and os.path.splitext(image)[1].lower() in target_exts:
                image_list.append(image)
            else:
                logger.info("File in textfile does not exist or has an invalid extension: %s" %image)
        t.close()

    else:
        for root,dirs,files in os.walk(inpath):
            for f in  files:
                if os.path.splitext(f)[1].lower() in target_exts:
                    image_path = os.path.join(root,f)
                    image_path = string.replace(image_path,'\\','/')
                    image_list.append(image_path)

    #print len(exclude_list)
    if len(exclude_list) > 0:

        image_list2 = []
        for image in image_list:
            include=True
            for pattern in exclude_list:
                if pattern in image:
                    include=False

            if include==False:
                logger.debug("Scene ID is matches pattern in exclude_list: %s" %image)
            else:
                image_list2.append(image)

        return image_list2

    else:
        return image_list


def convert_optional_args_to_string(args, positional_arg_keys, arg_keys_to_remove):

    args_dict = vars(args)
    arg_list = []

    ## Add optional args to arg_list
    for k,v in args_dict.iteritems():
        if k not in positional_arg_keys and k not in arg_keys_to_remove and v is not None:
            k = k.replace('_','-')
            if isinstance(v,list) or isinstance(v,tuple):
                arg_list.append("--{} {}".format(k,' '.join([str(item) for item in v])))
            elif isinstance(v,bool):
                if v is True:
                    arg_list.append("--{}".format(k))
            else:
                arg_list.append("--{} {}".format(k,str(v)))

    arg_str_base = " ".join(arg_list)
    return arg_str_base


def check_file_inclusion(f, pairname, overlap_prefix, args):
    move_file = False

    #### determine if file is part of overlap
    if overlap_prefix in f:

        if f.endswith(deliv_suffixes):
            move_file = True
        if f.endswith(fltr_suffixes):
            move_file = False

        if args.include_pc is True:
            if f.endswith(pc_suffixes):
                move_file = True

        if args.include_logs is True:
            if f.endswith(log_suffixes):
                move_file = True

        if args.include_fltr is True:
            if f.endswith(fltr_suffixes):
                move_file = True

        if args.exclude_drg is True:
            if f.endswith(('-DRG.tif','_ortho.tif')):
                move_file = False

        if args.exclude_err is True:
            if f.endswith('-IntersectionErr.tif'):
                move_file = False

        if args.dems_only is True:
            move_file = False
            if f.endswith(("-DEM.tif",'-DEM.prj','.geojson','_dem.tif','_meta.txt')):
                move_file = True
            if f.endswith(("_fltr-DEM.tif",'_fltr-DEM.prj')):
                if args.include_fltr:
                    move_file = True
                else:
                    move_file = False

        if args.tar_only is True:
            move_file = False
            if f.endswith(".tar"):
                move_file = True

    #### determine if file is in pair shp
    if (f.endswith(shp_suffixes) and pairname in f and not '-DEM' in f):
        if not args.dems_only:
            move_file = True

    return move_file


def delete_temp_files(names):

    for name in names:
        deleteList = glob.glob(os.path.splitext(name)[0]+'.*')
        for f in deleteList:
            if not "log" in os.path.basename(f):
                try:
                    os.remove(f)
                except Exception, e:
                    logger.warning('Could not remove %s: %s' %(os.path.basename(f),e))


def getGEMetadataAsXml(metafile):
    if os.path.isfile(metafile):
        try:
            metaf = open(metafile, "r")
        except IOError, err:
            logger.error("Could not open metadata file %s because %s" % (metafile, err))
            raise
    else:
        logger.error("Metadata file %s not found" % metafile)
        return None

    # Patterns to extract tag/value pairs and BEGIN/END group tags
    gepat1 = re.compile(r'(?P<tag>\w+) = "?(?P<data>.*?)"?;', re.I)
    gepat2 = re.compile(r"(?P<tag>\w+) = ", re.I)

    # These tags use the following tag/value as an attribute of the group rather than
    # a standalone node
    group_tags = {"aoiGeoCoordinate":"coordinateNumber",
                  "aoiMapCoordinate":"coordinateNumber",
                  "bandSpecificInformation":"bandNumber"}

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
    #print ET.ElementTree(root)
    return ET.ElementTree(root)


def getIKMetadataAsXml(metafile):
    """
    Given the text of an IKONOS metadata file, returns all the key/pair values as a
    searchable XML tree
    """
    if not metafile:
        return ET.Element("root")  # No metadata provided, return an empty tree

    # If metafile is a file, open it and read from it, otherwise assume a list of strings
    if os.path.isfile(metafile) and os.path.getsize(metafile) > 0:
        try:
            metaf = open(metafile, "r")
        except IOError, err:
            logger.error( "Could not open metadata file %s because %s" % (metafile, err))
            raise
    else:
        metaf = metafile

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
                    if tag not in tags_coords and current.tag in ["Coordinate","Component_Map_Coordinates_in_Map_Units","Acquired_Nominal_GSD"]:
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



