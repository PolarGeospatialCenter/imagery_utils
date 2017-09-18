import os, sys, string, shutil, glob
import argparse
import gdal,ogr,osr, gdalconst
from lib import utils


def main():

    #### Set Up Arguments
    parser = argparse.ArgumentParser(description="move/copy ASP deliverable files")

    #### Positional Arguments
    parser.add_argument('src', help="source dir/shp of ASP dems")
    parser.add_argument('dstdir', help="destination directory")

    #### Optional Arguments
    parser.add_argument('-m', '--move', action='store_true', default=False,
                        help='move files instead of copy')
    parser.add_argument('--dryrun', action='store_true', default=False,
                        help='print action but do not alter files\n')
    
    
    parser.add_argument('--exclude-drg', action='store_true', default=False,
                        help='exclude DRG/Ortho')
    parser.add_argument('--dems-only', action='store_true', default=False,
                        help='copy DEMs only - overrides --exclude and --include options, except --include-fltr')
    parser.add_argument('--no-dirs', action='store_true', default=False,
                        help='do not make pairname subdirs for overlaps\n')
    parser.add_argument('--tar-only', action='store_true', default=False,
                        help='copy only tar archive, overrides --exclude and --include options')
    parser.add_argument('--exclude-err', action='store_true', default=False,
                        help='ASP: exclude intersectionErr raster')
    parser.add_argument('--include-pc', action='store_true', default=False,
                        help='ASP: include point cloud')
    parser.add_argument('--include-fltr', action='store_true', default=False,
                        help='ASP: include non-interpolated DEM')
    parser.add_argument('--include-logs', action='store_true', default=False,
                        help='ASP: include stereo logs')    

    #### Parse Arguments
    args = parser.parse_args()
    src = os.path.abspath(args.src)

    if args.dems_only and args.tar_only:
        parser.error("options --tar-only and --dems-only are not not compatible")

    #### Validate args
    if os.path.isdir(src):
        srctype = 'dir'
    elif os.path.isfile(src) and src.endswith(".shp"):
        srctype = "shp"
    else:
        parser.error("Src is not a valid directory or shapefile: %s" %src)


    print "Collecting DEMs from source..."

    #### ID all dems, pairname is dirname
    overlaps = []

    if srctype == 'dir':
        for root, dirs, files in os.walk(src):
            for f in files:
                if (f.endswith(('-DEM.tif','_dem.tif')) and not 'fltr' in f):
                    overlaps.append(os.path.join(root,f))

    elif srctype == 'shp':
        #### open shp

        flds = ("FILEPATH","WIN_PATH")
        dem_fld = "DEM_NAME"
        ds = ogr.Open(src)
        if ds is not None:

            lyr = ds.GetLayerByName(os.path.splitext(os.path.basename(src))[0])
            lyr.ResetReading()

            src_srs = lyr.GetSpatialRef()
            featDefn = lyr.GetLayerDefn()

            for feat in lyr:
                path = None
                paths = []

                try:
                    i = feat.GetFieldIndex(dem_fld)
                    dem_name = feat.GetField(i)
                except ValueError, e:
                    print "Cannot locate DEM name field (%s)" %(dem_fld)

                if not dem_name:
                    print "Cannot locate DEM name field (%s)" %(dem_fld)
                else:

                    for fld in flds:
                        try:
                            i = feat.GetFieldIndex(fld)
                            attrib = feat.GetField(i)
                        except ValueError, e:
                            print "Cannot locate candidate field (%s) in source feature class" %(fld)
                        else:
                            if attrib:
                                attrib_path = os.path.join(attrib,dem_name)
                                paths.append(attrib)
                                paths.append(attrib_path)
                                if os.path.isfile(attrib):
                                    path = attrib
                                elif os.path.isfile(attrib_path):
                                    path = attrib_path
                    if path:
                        print path
                        overlaps.append(path)
                    else:
                        if len(paths) > 0:
                            print "Cannot locate path for DEM in any of the following locations: \n%s" %('\n\t'.join(paths))
                        else:
                            print "Cannot get valid values from candidate fields (%s) in source feature class" %(', '.join(flds))
            ds = None

    overlaps = list(set(overlaps))

    ##### Iterate through dems and copy/move files
    total = len(overlaps)
    i = 0
    for overlap in overlaps:
        i+=1
        print '\n[%d of %d]\t %s' %(i,total,os.path.basename(overlap))

        #### Check that path is not terranova storage location
        if args.move is True and (path.startswith(r'V:\pgc\data\elev\dem') or path.startswith(r'V:/pgc/data/elev/dem') or path.startswith(r'/mnt/pgc/data/elev/dem/asp')):
            print "Cannot use --move flag on DEMs located in /pgc/data/elev/dem/"
        else:
            srcpairdir = os.path.dirname(overlap)
            pairname = os.path.basename(srcpairdir)
            if args.no_dirs:
                file_dstdir = args.dstdir
                
            elif "SETSM" in os.path.basename(overlap):
                file_dstdir = args.dstdir
            
            else:
                file_dstdir = os.path.join(args.dstdir,pairname)
            
            overlap_prefix = os.path.basename(overlap)[:-8]

            #### Copy all files with overlap prefix and Copy pair shp
            for f in os.listdir(srcpairdir):
                
                move_file = utils.check_file_inclusion(f, pairname, overlap_prefix, args)
                
                #### Copy/Move
                if move_file is True:
                    if not args.dryrun:
                        if not os.path.isdir(file_dstdir):
                            os.makedirs(file_dstdir)
                    ifp = os.path.join(srcpairdir,f)
                    ofp = os.path.join(file_dstdir,f)
                    if args.move is True:
                        if not os.path.isfile(ofp):
                            print "Moving %s --> %s" %(ifp,ofp)
                            if not args.dryrun:
                                os.rename(ifp,ofp)

                    else:
                        if not os.path.isfile(ofp):
                            print "Copying %s --> %s" %(ifp,ofp)
                            if not args.dryrun:
                                shutil.copy2(ifp,ofp)

if __name__ == '__main__':
    main()
