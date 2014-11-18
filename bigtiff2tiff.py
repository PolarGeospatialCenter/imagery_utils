import os, string, sys, subprocess, glob, shutil

if len(sys.argv) == 3:
    s_dir = sys.argv[1]
    t_dir = sys.argv[2]

else:
    print "Incorredct number of args (2 required: source dir, target dir)"
    sys.exit()
    
if not os.path.isdir(s_dir):
    print "Source dir does not exist"
    sys.exit()
    
if not os.path.isdir(t_dir):
    os.makedirs(t_dir)

for s_fp in glob.glob(os.path.join(s_dir,"*.tif")):
    print "Converting %s" %os.path.basename(s_fp)
    t_fp = os.path.join(t_dir,os.path.basename(s_fp))
    
    if not os.path.isfile(t_fp):
        cmd = 'gdal_translate -stats -of GTiff -co bigtiff=no -co compress=lzw -co tiled=yes "%s" "%s"' %(s_fp,t_fp)
        rc = subprocess.call(cmd,shell=True)
        print ("Return code: %i" %rc)
    
        cmd = 'gdaladdo "%s" 2 4 8 16' %t_fp
        rc = subprocess.call(cmd,shell=True)
        print ("Return code: %i" %rc)
    
    for fp in glob.glob(os.path.join(s_dir,os.path.basename(s_fp)[:-4]+".*")):
        
        if not ".tif" in fp and not os.path.isfile(os.path.join(t_dir,os.path.basename(fp))):
            print "Copying %s" %os.path.basename(fp)
            shutil.copy2(fp,os.path.join(t_dir,os.path.basename(fp)))