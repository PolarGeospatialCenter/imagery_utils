import os, string, sys, shutil, math, glob, re, tarfile, argparse
from datetime import datetime, timedelta

from subprocess import *
from lib.ortho_utils import *

import gdal, ogr,osr, gdalconst


#### Reg Exs

# WV02_12FEB061315046-P1BS-10300100106FC100.ntf
WV02p = re.compile("WV02_\w+-P")

# WV03_12FEB061315046-P1BS-10300100106FC100.ntf
WV03p = re.compile("WV03_\w+-P")

# QB02_12FEB061315046-P1BS-10300100106FC100.ntf
QB02p = re.compile("QB02_\w+-P")

# GE01_12FEB061315046-P1BS-10300100106FC100.ntf
GE01p_dg = re.compile("GE01_\w+-P")

# GE01_111211P0011184144A222000100082M_000754776.ntf
GE01p = re.compile("GE01_\w+P0")

# IK01_2009121113234710000011610960_pan_6516S.ntf
IK01p = re.compile("IK01_\w+pan")

dRegExs = {
    WV02p:("WV02",0.5),
    GE01p_dg:("GE01",0.5),
    WV02p:("WV03",0.3),
    QB02p:("QB02",0.6),
    GE01p:("GE01",0.5),
    IK01p:("IK01",1)
        }

def get_multispectral_name(sensor,pan_name):
    ####  Identify name pattern, get res
            
    ####  check for multi version
    if sensor in ["WV02","WV03","QB02"]:
        mul_name = pan_name.replace("-P","-M")
    elif sensor == "GE01":
        mul_name = pan_name.replace("P0","M0")
        mul_name = mul_name.replace("-P","-M")
    elif sensor == "IK01":
        mul_name = pan_name.replace("blu","pan")
    
    return mul_name

def main():
    
    
    #### Set Up Arguments 
    parent_parser, pos_arg_keys = buildParentArgumentParser()
    parser = argparse.ArgumentParser(
	parents=[parent_parser],
	description="Run/Submit batch pansharpening in parallel"
	)
    
    parser.add_argument("-l", help="PBS resources requested (mimicks qsub syntax)")
    parser.add_argument("--qsubscript",
                      help="qsub script to use in cluster job submission (default is qsub_pansharpen.sh in script root folder)")
    parser.add_argument("--dryrun", action="store_true", default=False,
                    help="print actions without executing")
    
    #### Parse Arguments
    opt = parser.parse_args()
    scriptpath = os.path.abspath(sys.argv[0])
    src = os.path.abspath(opt.src)
    dstdir = os.path.abspath(opt.dst)
    
    #### Validate Required Arguments
    if os.path.isdir(src):
        srctype = 'dir'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() == '.txt':
        srctype = 'textfile'
    elif os.path.isfile(src) and os.path.splitext(src)[1].lower() in exts:
        srctype = 'image'
    elif os.path.isfile(src.replace('msi','blu')) and os.path.splitext(src)[1].lower() in exts:
        srctype = 'image'
    else:
        parser.error("Error arg1 is not a recognized file path or file type: %s" %(src))
    
    if not os.path.isdir(dstdir):
        parser.error("Error arg2 is not a valid file path: %s" %(dstdir))

    
    if opt.qsubscript is None: 
        qsubpath = os.path.join(os.path.dirname(scriptpath),'qsub_pansharpen.sh')
    else:
        qsubpath = os.path.abspath(opt.qsubscript)
        
    if not os.path.isfile(qsubpath):
        parser.error("qsub script path is not valid: %s" %qsubpath)
    
    #### Verify EPSG
    try:
        spatial_ref = SpatialRef(opt.epsg)
    except RuntimeError, e:
	parser.error(e)
    
    #### Set Up Logging Handlers
    lso = logging.StreamHandler()
    lso.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
    lso.setFormatter(formatter)
    logger.addHandler(lso)
    
        
    ###############################
    ####  Submission logic
    ################################
    if srctype in ['dir','textfile']:
    
    
        #### Get args ready to pass through
        #### Get -l args and make a var
        l = ("-l %s" %opt.l) if opt.l is not None else ""
    
        args_dict = vars(opt)
        arg_list = []
        arg_keys_to_remove = ('l','qsubscript')
        
        ## Add optional args to arg_list
        for k,v in args_dict.iteritems():
            if k not in pos_arg_keys and k not in arg_keys_to_remove and v is not None:
                if isinstance(v,list) or isinstance(v,tuple):
                    arg_list.append("--%s %s" %(k,' '.join([str(item) for item in v])))
                elif isinstance(v,bool):
                    if v is True:
                        arg_list.append("--%s" %(k))
                else:
                    arg_list.append("--%s %s" %(k,str(v)))
        
        arg_str = " ".join(arg_list)
        
    
        #### find images
        if srctype == 'dir':
            image_list = FindImages(src,exts)
        elif srctype == 'textfile':
            t = open(src,'r')
            image_list = []
            for line in t.readlines():
                if os.path.isfile(line.rstrip()):
                    image_list.append(line.rstrip())
                else:
                    logger.warning('Src image does not exist: %s' %line.rstrip())
            t.close()
            
        print 'Number of images to process: %i' %len(image_list)
        
        #### Loop over images
        i = 0
        for image in image_list:
    
            #print  image
            srcdir,srcname = os.path.split(image)
    
            ####  Identify name pattern, get res
            sensor = None
            for regex in dRegExs:
                match = regex.match(srcname)
                if match is not None:
                    sensor,res = dRegExs[regex]
                    print "Image: %s, Sensor: %s, Res: %f" %(srcname,sensor,res)

            if sensor is None:
                print "Image did not match any panchromatic image pattern: %s" %srcname
 
            if sensor is not None:
               
                mul_name = get_multispectral_name(sensor,srcname)
                mulp = os.path.join(srcdir,mul_name)
                bittype = getBitdepth(opt.outtype)
   
                if opt.dem is not None:
                    dem_str = "ortho"
                    
                else:
                    dem_str = ""
                    
                panshp = os.path.join(dstdir,"%s%s_%s%s%s_pansh.tif"%(dem_str,os.path.splitext(srcname)[0],bittype,opt.stretch,opt.epsg))
                
                if os.path.isfile(mulp):
                    if not os.path.isfile(panshp):
                        
                        cmd = r'qsub %s -N Pansh%04i -v p1="%s %s %s %s" "%s"' %(l,i,scriptpath,arg_str,image,dstdir,qsubpath)
                        print i, cmd
                        if not opt.dryrun:
                            p = Popen(cmd,shell=True)
                            p.wait()
                        i+=1
                    
                else:
                    print "Error: Multispectral image not found: %s" %(mulp)
                    
    
    ###############################
    ####  Execution logic
    ################################
    
    elif srctype == 'image':
    
        srcdir,srcname = os.path.split(src)
        
        #### Get working dir
        if opt.wd is not None:
            wd = opt.wd
        else:
            wd = dstdir
        if not os.path.isdir(wd):
            try:
                os.makedirs(wd)
            except OSError:
                pass
        logger.info("Working Dir: %s" %wd)
        
        print "Pan image: %s" %srcname
        
        ####  Identify name pattern, get res
        sensor = None
        for regex in dRegExs:
            match = regex.search(srcname)
            if match is not None:
                sensor,res = dRegExs[regex]
                print "Sensor: %s, Res: %f" %(sensor,res)
            
        if sensor is None:
            print "Error: Cannot match file name to sensor pattern: %s" %srcname
        
        else:
            mul_name = get_multispectral_name(sensor,srcname)
            
            mulp = os.path.join(srcdir,mul_name)
            if os.path.isfile(mulp) is not True:
                print "Error: Multispectral image not found: %s" %(mulp)
            
            else:
            
                if opt.dem is not None:
                    dem_arg = '-d "%s" ' %opt.dem
                    dem_str = "ortho"
                    
                else:
                    dem_arg = ""
                    dem_str = ""
                
                bittype = getBitdepth(opt.outtype)
                
                p = os.path.splitext(srcname)[0]
                m = os.path.splitext(mul_name)[0]
                panolp = os.path.join(wd,"%s%s_%s%s%s.tif"%(dem_str,p,bittype,opt.stretch,opt.epsg))
                mulolp = os.path.join(wd,"%s%s_%s%s%s.tif"%(dem_str,m,bittype,opt.stretch,opt.epsg))
                panop = os.path.join(dstdir,"%s%s_%s%s%s.tif"%(dem_str,p,bittype,opt.stretch,opt.epsg))
                mulop = os.path.join(dstdir,"%s%s_%s%s%s.tif"%(dem_str,m,bittype,opt.stretch,opt.epsg))
                panshtp = os.path.join(wd,"%s%s_%s%s%s_pansh_temp.tif"%(dem_str,p,bittype,opt.stretch,opt.epsg))
                panshlp = os.path.join(wd,"%s%s_%s%s%s_pansh.tif"%(dem_str,p,bittype,opt.stretch,opt.epsg))
                panshp = os.path.join(dstdir,"%s%s_%s%s%s_pansh.tif"%(dem_str,p,bittype,opt.stretch,opt.epsg))    
                
                #### Check if pansh is already present
                if not os.path.isfile(panshp) and not opt.dryrun:
                    
                    if not os.path.isdir(wd):
                        os.makedirs(wd)
                        
                    ####  Ortho pan
                    if not os.path.isfile(panop) and not os.path.isfile(panolp):
                        opt.resolution = str(res)
                        rc = processImage(src,panop,opt)
                    
                    if not os.path.isfile(panolp) and os.path.isfile(panop):
                        shutil.copy2(panop,panolp)
                        
                    
                    ####  Ortho multi - call python script: also copies to output, add do not delete temp files flag
                    if not os.path.isfile(mulop) and not os.path.isfile(mulolp):
                        opt.resolution = str(res*4.0)
                        rc = processImage(mulp,mulop,opt)
                        
                    if not os.path.isfile(mulolp) and os.path.isfile(mulop):
                        shutil.copy2(mulop,mulolp)      
                            
                    ####  Pansharpen
                    if os.path.isfile(panolp) and os.path.isfile(mulolp) and not os.path.isfile(panshtp):
                        cmd = 'gdal_landsat_pansharp -rgb "%s" -pan "%s" -o "%s"' %(mulolp,panolp,panshtp)
                        ExecCmd(cmd)
                    else:
                        print "Pan or Multi warped image does not exist\n\t%s\n\t%s" %(panolp,mulolp)
                    
                    #### Compress
                    if os.path.isfile(panshtp) and not os.path.isfile(panshlp):
                        cmd = 'gdal_translate -stats -co BIGTIFF=IF_SAFER -co COMPRESS=LZW -co TILED=YES "%s" "%s"' %(panshtp,panshlp)
                        ExecCmd(cmd)
                    
                    #### Make pyramids
                    if os.path.isfile(panshlp):
                       cmd = 'gdaladdo "%s" 2 4 8 16' %(panshlp)
                       ExecCmd(cmd)
                    
                    #### Copy pansharpened output
                    if wd <> dstdir:
                        for local_path, dst_path in [(panshlp,panshp), (panolp,panop), (mulolp,mulop)]:
                            if os.path.isfile(local_path) and not os.path.isfile(dst_path):
                                shutil.copy2(local_path,dst_path)
                            
                    #### Delete Temp Files
                    temp_files = [panshtp]
                    wd_files = [
                        panshlp,
                        panolp,
                        mulolp
                    ]
                    
                    if not opt.save_temps:
                        for f in temp_files:
                            try:
                                os.remove(f)
                            except Exception, e:
                                LogMsg('Could not remove %s: %s' %(os.path.basename(f),e))
                    
                        if wd <> dstdir:
                            for f in wd_files:
                                try:
                                    os.remove(f)
                                except Exception, e:
                                    LogMsg('Could not remove %s: %s' %(os.path.basename(f),e))
                                    
                    

    
if __name__ == '__main__':
    main()
