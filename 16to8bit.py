import os, string, sys, glob, shutil
from optparse import OptionParser
import subprocess

usage = "python %prog [options] arg1 arg2 arg3\n\n  arg1 - src file path\n  arg2 - output directory\n  arg3 - input max\n\n  Type %prog -h for options"
parser = OptionParser()
parser.usage = usage

(opt, args) = parser.parse_args()
if len(args) != 3:
    parser.error("Incorrect number of arguments")
    
srcdir = os.path.abspath(args[0])
dstdir = os.path.abspath(args[1])

if not os.path.isdir(srcdir):
    parser.error("Arg1 is not a valid file path: %s" %srcdir)
if not os.path.isdir(dstdir):
    parser.error("Arg2 is not a valid file path: %s" %dstdir)
try:

    imax = int(args[2])
except TypeError:
    parser.error("Arg3 must be an integer")
    
files = glob.glob(srcdir+r"/*u16ns*.tif")
print files
for srcfp in files:
    print srcfp    
    srcfp_local = os.path.join("/local/",os.path.basename(srcfp))
    dstfp_local = os.path.join("/local","temp_"+os.path.basename(srcfp))
    dstfp = os.path.join(dstdir,os.path.basename(srcfp))
    
    if not os.path.isfile(dstfp):
        shutil.copy2(srcfp,srcfp_local)
        cmd = 'gdal_translate --config GDAL_CACHEMAX 2048 -stats -co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co "COMPRESS=LZW" -co "BIGTIFF=NO" -ot Byte -scale 0 %d 0 255 %s %s' %(imax,srcfp_local,dstfp_local)
        print cmd
        subprocess.call(cmd,shell=True)
        cmd = ('gdaladdo "%s" 2 4 8 16' %dstfp_local)
        subprocess.call(cmd,shell=True)
        
        shutil.copy2(dstfp_local,dstfp)
        os.remove(srcfp_local)
        os.remove(dstfp_local)
    
    