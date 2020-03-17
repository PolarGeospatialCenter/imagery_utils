import os, sys, shutil, math, glob, re, tarfile, argparse, subprocess, logging, platform
from datetime import datetime, timedelta
import gdal, ogr, osr, gdalconst

from lib import ortho_functions, utils, taskhandler

#### Create Loggers
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

#TODO: build image pair class
# compare mul and pan extent to find smaller extent and pass in to ortho_functions.process() as a parameter

#### Reg Exs

# WV02_12FEB061315046-P1BS-10300100106FC100.ntf
WV02p = re.compile("WV02_\w+-M")

# WV03_12FEB061315046-P1BS-10300100106FC100.ntf
WV03p = re.compile("WV03_\w+-M")

# QB02_12FEB061315046-P1BS-10300100106FC100.ntf
QB02p = re.compile("QB02_\w+-M")

# GE01_12FEB061315046-P1BS-10300100106FC100.ntf
GE01p_dg = re.compile("GE01_\w+-M")

# GE01_111211P0011184144A222000100082M_000754776.ntf
GE01p = re.compile("GE01_\w+M0")

# IK01_2009121113234710000011610960_pan_6516S.ntf
IK01p = re.compile("IK01_\w+(blu|msi|bgrn)")

dRegExs = {
    WV02p:("WV02"),
    GE01p_dg:("GE01"),
    WV03p:("WV03"),
    QB02p:("QB02"),
    GE01p:("GE01"),
    IK01p:("IK01")
}

class ImagePair(object):
    
    def __init__(self, mul_srcfp, spatial_ref):
        self.mul_srcfp = mul_srcfp
        self.srcdir, self.mul_srcfn = os.path.split(mul_srcfp)
        
        ####  Identify name pattern
        self.sensor = None
        for regex in dRegExs:
            match = regex.match(self.mul_srcfn)
            if match is not None:
                self.sensor = dRegExs[regex]
                break
        if self.sensor:
            
            self.pan_srcfn = self._get_panchromatic_name()
            self.pan_srcfp = os.path.join(self.srcdir, self.pan_srcfn)
            if not os.path.isfile(self.pan_srcfp):
                raise RuntimeError("Corresponding panchromatic image not found: {}".format(self.mul_srcfp))
            else:
            ## get extent info for both images and calc intersect
                mul_extent = self._get_image_info(self.mul_srcfp, spatial_ref)
                pan_extent = self._get_image_info(self.pan_srcfp, spatial_ref)
                self.intersection_geom = mul_extent.Intersection(pan_extent)
                # print(mul_extent)
                # print(mul_extent.Contains(pan_extent))
                # print(pan_extent)
                # print(pan_extent.Contains(mul_extent))
                # print(self.intersection_geom)
                
        else:
            raise RuntimeError("Image does not match multispectral name pattern: {}".format(self.mul_srcfn))
                
    def _get_panchromatic_name(self):
    
        ####  check for pan version
        if self.sensor in ["WV02", "WV03", "QB02"]:
            pan_name = self.mul_srcfn.replace("-M", "-P")
        elif self.sensor == "GE01":
            if "_5V" in self.mul_srcfn:
                
                pan_name_base = self.mul_srcfp[:-24].replace("M0", "P0")
                candidates = glob.glob(pan_name_base + "*")
                candidates2 = [f for f in candidates if f.endswith(('.ntf', '.NTF', '.tif', '.TIF'))]
                if len(candidates2) == 0:
                    pan_name = ''
                elif len(candidates2) == 1:
                    pan_name = os.path.basename(candidates2[0])
                else: #raise error for now. TODO: iterate through candidates for greatest overlap
                    pan_name = ''
                    logger.error('%i panchromatic images match the multispectral image name %s', len(candidates2),
                                 self.mul_srcfn)
            else:
                pan_name = self.mul_srcfn.replace("-M", "-P")
        elif self.sensor == "IK01":
            pan_name = re.sub("blu|msi|bgrn", "pan", self.mul_srcfn)

        return pan_name
    
    def _get_image_info(self, src_image, spatial_ref):

        if self.sensor == 'IK01' and "_msi_" in src_image:
            src_image_name = src_image("_msi_", "_blu_")
            src_image = os.path.join(self.srcdir, src_image_name)
    
        ds = gdal.Open(src_image, gdalconst.GA_ReadOnly)
        if ds is not None:
    
            ####  Get extent from GCPs
            num_gcps = ds.GetGCPCount()
    
            if num_gcps == 4:
                gcps = ds.GetGCPs()
                proj = ds.GetGCPProjection()
    
                gcp_dict = {}
                id_dict = {"UpperLeft": 1,
                           "1": 1,
                           "UpperRight": 2,
                           "2": 2,
                           "LowerLeft": 4,
                           "4": 4,
                           "LowerRight": 3,
                           "3": 3}
    
                for gcp in gcps:
                    gcp_dict[id_dict[gcp.Id]] = [float(gcp.GCPPixel), float(gcp.GCPLine), float(gcp.GCPX),
                                                 float(gcp.GCPY), float(gcp.GCPZ)]
                ulx = gcp_dict[1][2]
                uly = gcp_dict[1][3]
                urx = gcp_dict[2][2]
                ury = gcp_dict[2][3]
                llx = gcp_dict[4][2]
                lly = gcp_dict[4][3]
                lrx = gcp_dict[3][2]
                lry = gcp_dict[3][3]
    
                xsize = gcp_dict[1][0] - gcp_dict[2][0]
                ysize = gcp_dict[1][1] - gcp_dict[4][1]
    
            else:
                xsize = ds.RasterXSize
                ysize = ds.RasterYSize
                proj = ds.GetProjectionRef()
                gtf = ds.GetGeoTransform()
                print(gtf)
    
                ulx = gtf[0] + 0 * gtf[1] + 0 * gtf[2]
                uly = gtf[3] + 0 * gtf[4] + 0 * gtf[5]
                urx = gtf[0] + xsize * gtf[1] + 0 * gtf[2]
                ury = gtf[3] + xsize * gtf[4] + 0 * gtf[5]
                llx = gtf[0] + 0 * gtf[1] + ysize * gtf[2]
                lly = gtf[3] + 0 * gtf[4] + ysize * gtf[5]
                lrx = gtf[0] + xsize * gtf[1] + ysize* gtf[2]
                lry = gtf[3] + xsize * gtf[4] + ysize * gtf[5]
    
            ds = None
    
            ####  Create geometry objects
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint(ulx, uly)
            ring.AddPoint(urx, ury)
            ring.AddPoint(lrx, lry)
            ring.AddPoint(llx, lly)
            ring.AddPoint(ulx, uly)

            extent_geom = ogr.Geometry(ogr.wkbPolygon)
            extent_geom.AddGeometry(ring)
    
            #### Create srs objects
            s_srs = utils.osr_srs_preserve_axis_order(osr.SpatialReference(proj))
            t_srs = spatial_ref.srs
            st_ct = osr.CoordinateTransformation(s_srs, t_srs)
    
            #### Transform geoms to target srs
            if not s_srs.IsSame(t_srs):
                extent_geom.Transform(st_ct)
            #logger.info("Projected extent: %s", str(extent_geom))
            return extent_geom
                   
        else:
            logger.error("Cannot open dataset: %s", src_image)
            return None


def main():

    #### Set Up Arguments
    parent_parser, pos_arg_keys = ortho_functions.buildParentArgumentParser()
    parser = argparse.ArgumentParser(
        parents=[parent_parser],
        description="Run/Submit batch pansharpening in parallel"
    )

    parser.add_argument("--pbs", action='store_true', default=False,
                        help="submit tasks to PBS")
    parser.add_argument("--slurm", action='store_true', default=False,
                        help="submit tasks to SLURM")
    parser.add_argument("--parallel-processes", type=int, default=1,
                        help="number of parallel processes to spawn (default 1)")
    parser.add_argument("--qsubscript",
                        help="submission script to use in PBS/SLURM submission (PBS default is qsub_pansharpen.sh, "
                             "SLURM default is slurm_pansharpen.py, in script root folder)")
    parser.add_argument("-l",
                        help="PBS resources requested (mimicks qsub syntax, PBS only)")
    parser.add_argument("--dryrun", action="store_true", default=False,
                        help="print actions without executing")

    #### Parse Arguments
    args = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(args.src)
    dstdir = os.path.abspath(args.dst)

    #### Validate Required Arguments
    if os.path.isdir(src):
        srctype = 'dir'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() == '.txt':
        srctype = 'textfile'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() in ortho_functions.exts:
        srctype = 'image'
    elif os.path.isfile(src.replace('msi', 'blu')) and os.path.splitext(src)[1].lower() in ortho_functions.exts:
        srctype = 'image'
    else:
        parser.error("Error arg1 is not a recognized file path or file type: {}".format(src))

    if not os.path.isdir(dstdir):
        parser.error("Error arg2 is not a valid file path: {}".format(dstdir))

    # Verify qsubscript
    if args.pbs or args.slurm:
        if args.qsubscript is None:
            if args.pbs:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'qsub_pansharpen.sh')
            if args.slurm:
                qsubpath = os.path.join(os.path.dirname(scriptpath), 'slurm_pansharpen.sh')
        else:
            qsubpath = os.path.abspath(args.qsubscript)
        if not os.path.isfile(qsubpath):
            parser.error("qsub script path is not valid: {}".format(qsubpath))

    ### Verify processing options do not conflict
    if args.pbs and args.slurm:
        parser.error("Options --pbs and --slurm are mutually exclusive")
    if (args.pbs or args.slurm) and args.parallel_processes > 1:
        parser.error("HPC Options (--pbs or --slurm) and --parallel-processes > 1 are mutually exclusive")

    #### Verify EPSG
    try:
        spatial_ref = utils.SpatialRef(args.epsg)
    except RuntimeError as e:
        parser.error(e)
        
    ## Check GDAL version (2.1.0 minimum)
    gdal_version = gdal.VersionInfo()
    try:
        if int(gdal_version) < 2010000:
            parser.error("gdal_pansharpen requires GDAL version 2.1.0 or higher")
    except ValueError:
        parser.error("Cannot parse GDAL version: {}".format(gdal_version))

    #### Set up console logging handler
    lso = logging.StreamHandler()
    lso.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)
    
    #### Get args ready to pass to task handler
    arg_keys_to_remove = ('l', 'qsubscript', 'dryrun', 'pbs', 'slurm', 'parallel_processes')
    arg_str_base = taskhandler.convert_optional_args_to_string(args, pos_arg_keys, arg_keys_to_remove)
    
    ## Identify source images
    if srctype == 'dir':
        image_list1 = utils.find_images(src, False, ortho_functions.exts)
    elif srctype == 'textfile':
        image_list1 = utils.find_images(src, True, ortho_functions.exts)
    else:
        image_list1 = [src]

    pair_list = []
    for srcfp in image_list1:
        #print(srcfp)
        try:
            image_pair = ImagePair(srcfp, spatial_ref)
        except RuntimeError as e:
            logger.error(e)
        else:
            logger.info("Image: %s, Sensor: %s", image_pair.mul_srcfn, image_pair.sensor)
            pair_list.append(image_pair)
                
    logger.info('Number of src image pairs: %i', len(pair_list))
    
    ## Build task queue
    i = 0
    task_queue = []
    for image_pair in pair_list:
        
        bittype = utils.get_bit_depth(args.outtype)
        pansh_dstfp = os.path.join(dstdir, "{}_{}{}{}_pansh.tif".format(os.path.splitext(image_pair.mul_srcfn)[0],
                                                                        bittype, args.stretch, args.epsg))
        
        if not os.path.isfile(pansh_dstfp):
            i += 1
            task = taskhandler.Task(
                image_pair.mul_srcfn,
                'Psh{:04g}'.format(i),
                'python',
                '{} {} {} {}'.format(scriptpath, arg_str_base, image_pair.mul_srcfp, dstdir),
                exec_pansharpen,
                [image_pair, pansh_dstfp, args]
            )
            task_queue.append(task)
            
    logger.info('Number of incomplete tasks: %i', i)

    ## Run tasks
    if len(task_queue) > 0:
        logger.info("Submitting Tasks")
        if args.pbs:
            l = "-l {}".format(args.l) if args.l else ""
            try:
                task_handler = taskhandler.PBSTaskHandler(qsubpath, l)
            except RuntimeError as e:
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)
                
        elif args.slurm:
            try:
                task_handler = taskhandler.SLURMTaskHandler(qsubpath)
            except RuntimeError as e:
                logger.error(e)
            else:
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)
            
        elif args.parallel_processes > 1:
            try:
                task_handler = taskhandler.ParallelTaskHandler(args.parallel_processes)
            except RuntimeError as e:
                logger.error(e)
            else:
                logger.info("Number of child processes to spawn: %i", task_handler.num_processes)
                if not args.dryrun:
                    task_handler.run_tasks(task_queue)
    
        else:
            results = {}
            lfh = None
            for task in task_queue:
                           
                src, dstfp, task_arg_obj = task.method_arg_list
                
                #### Set up processing log handler
                logfile = os.path.splitext(dstfp)[0] + ".log"
                lfh = logging.FileHandler(logfile)
                lfh.setLevel(logging.DEBUG)
                formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
                lfh.setFormatter(formatter)
                logger.addHandler(lfh)
                
                if not args.dryrun:
                    results[task.name] = task.method(src, dstfp, task_arg_obj)
                    
                #### remove existing file handler
                logger.removeHandler(lfh)
            
                #### remove existing file handler
                logger.removeHandler(lfh)
                
            #### Print Images with Errors    
            for k, v in results.items():
                if v != 0:
                    logger.warning("Failed Image: %s", k)
        
        logger.info("Done")
        
    else:
        logger.info("No images found to process")


def exec_pansharpen(image_pair, pansh_dstfp, args):

    dstdir = os.path.dirname(pansh_dstfp)

    #### Get working dir
    if args.wd is not None:
        wd = args.wd
    else:
        wd = dstdir
    if not os.path.isdir(wd):
        try:
            os.makedirs(wd)
        except OSError:
            pass
    logger.info("Working Dir: %s", wd)

    ####  Identify name pattern
    print("Multispectral image: {}".format(image_pair.mul_srcfp))
    print("Panchromatic image: {}".format(image_pair.pan_srcfp))

    if args.dem is not None:
        dem_arg = '-d "{}" '.format(args.dem)
    else:
        dem_arg = ""

    bittype = utils.get_bit_depth(args.outtype)
    pan_basename = os.path.splitext(image_pair.pan_srcfn)[0]
    mul_basename = os.path.splitext(image_pair.mul_srcfn)[0]
    pan_local_dstfp = os.path.join(wd, "{}_{}{}{}.tif".format(pan_basename, bittype, args.stretch, args.epsg))
    mul_local_dstfp = os.path.join(wd, "{}_{}{}{}.tif".format(mul_basename, bittype, args.stretch, args.epsg))
    pan_dstfp = os.path.join(dstdir, "{}_{}{}{}.tif".format(pan_basename, bittype, args.stretch, args.epsg))
    mul_dstfp = os.path.join(dstdir, "{}_{}{}{}.tif".format(mul_basename, bittype, args.stretch, args.epsg))
    pansh_tempfp = os.path.join(wd, "{}_{}{}{}_pansh_temp.tif".format(mul_basename, bittype, args.stretch, args.epsg))
    pansh_local_dstfp = os.path.join(wd, "{}_{}{}{}_pansh.tif".format(mul_basename, bittype, args.stretch, args.epsg))
    pansh_xmlfp = os.path.join(dstdir, "{}_{}{}{}_pansh.xml".format(mul_basename, bittype, args.stretch, args.epsg))
    mul_xmlfp = os.path.join(dstdir, "{}_{}{}{}.xml".format(mul_basename, bittype, args.stretch, args.epsg))
    
    if not os.path.isdir(wd):
        os.makedirs(wd)

    ####  Ortho pan
    logger.info("Orthorectifying panchromatic image")
    if not os.path.isfile(pan_dstfp) and not os.path.isfile(pan_local_dstfp):
        ortho_functions.process_image(image_pair.pan_srcfp, pan_dstfp, args, image_pair.intersection_geom)

    if not os.path.isfile(pan_local_dstfp) and os.path.isfile(pan_dstfp):
        shutil.copy2(pan_dstfp, pan_local_dstfp)

    logger.info("Orthorectifying multispectral image")
    ####  Ortho multi
    if not os.path.isfile(mul_dstfp) and not os.path.isfile(mul_local_dstfp):
        ## If resolution is specified in the command line, assume it's intended for the pansharpened image
        ##    and multiply the multi by 4
        if args.resolution:
            args.resolution = args.resolution * 4.0
        ortho_functions.process_image(image_pair.mul_srcfp, mul_dstfp, args, image_pair.intersection_geom)

    if not os.path.isfile(mul_local_dstfp) and os.path.isfile(mul_dstfp):
        shutil.copy2(mul_dstfp, mul_local_dstfp)

    ####  Pansharpen
    ## get system info for program extension
    if platform.system() == 'Windows':
        py_ext = ''
    else:
        py_ext = '.py'
    
    logger.info("Pansharpening multispectral image")
    if os.path.isfile(pan_local_dstfp) and os.path.isfile(mul_local_dstfp):
        if not os.path.isfile(pansh_local_dstfp):
            cmd = 'gdal_pansharpen{} -co BIGTIFF=IF_SAFER -co COMPRESS=LZW -co TILED=YES "{}" "{}" "{}"'.\
                format(py_ext, pan_local_dstfp, mul_local_dstfp, pansh_local_dstfp)
            taskhandler.exec_cmd(cmd)
    else:
        print("Pan or Multi warped image does not exist\n\t{}\n\t{}").format(pan_local_dstfp, mul_local_dstfp)

    #### Make pyramids
    if os.path.isfile(pansh_local_dstfp):
        cmd = 'gdaladdo -r {} "{}" 2 4 8 16'.format(args.pyramid_type, pansh_local_dstfp)
        taskhandler.exec_cmd(cmd)
       
    ## Copy warped multispectral xml to pansharpened output
    shutil.copy2(mul_xmlfp, pansh_xmlfp)

    #### Copy pansharpened output
    if wd != dstdir:
        for local_path, dst_path in [(pansh_local_dstfp, pansh_dstfp), (pan_local_dstfp, pan_dstfp),
                                     (mul_local_dstfp, mul_dstfp)]:
            if os.path.isfile(local_path) and not os.path.isfile(dst_path):
                shutil.copy2(local_path, dst_path)

    #### Delete Temp Files
    wd_files = [
        pansh_local_dstfp,
        pan_local_dstfp,
        mul_local_dstfp
    ]

    if not args.save_temps:
        if wd != dstdir:
            for f in wd_files:
                try:
                    os.remove(f)
                except Exception as e:
                    logger.warning('Could not remove %s: %s', os.path.basename(f), e)
    
    if os.path.isfile(pansh_dstfp):
        return 0
    else:
        return 0
        

if __name__ == '__main__':
    main()
