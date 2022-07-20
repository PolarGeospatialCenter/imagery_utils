#!/usr/bin/env python

import argparse
import logging
import os
import shutil
import sys

import numpy
from osgeo import gdal

from lib import mosaic, taskhandler, utils
from lib import VERSION

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

gdal.SetConfigOption('GDAL_PAM_ENABLED', 'NO')


def main():
    
    #########################################################
    ####  Handle args
    #########################################################

    #### Set Up Arguments 
    parser = argparse.ArgumentParser(
        description="Create mosaic subtile"
    )
    
    parser.add_argument("tile", help="output tile name")
    parser.add_argument("src", help="textfile of input rasters (tif only)")
    
    parser.add_argument("-r", "--resolution", nargs=2, type=float,
                        help="output pixel resolution -- xres yres (default is same as first input file)")
    parser.add_argument("-e", "--extent", nargs=4, type=float,
                        help="extent of output mosaic -- xmin xmax ymin ymax (default is union of all inputs)")
    parser.add_argument("-t", "--tilesize", nargs=2, type=float,
                        help="tile size in coordinate system units -- xsize ysize (default is 40,000 times output "
                             "resolution)")
    parser.add_argument("--force-pan-to-multi", action="store_true", default=False,
                        help="if output is multiband, force script to also use 1 band images")
    parser.add_argument("-b", "--bands", type=int,
                        help="number of output bands( default is number of bands in the first image)")
    parser.add_argument("--median-remove", action="store_true", default=False,
                        help="subtract the median from each input image before forming the mosaic in order to correct "
                             "for contrast")
    parser.add_argument("--wd",
                        help="scratch space (default is mosaic directory)")
    parser.add_argument("--gtiff-compression", choices=mosaic.GTIFF_COMPRESSIONS, default="lzw",
                        help="GTiff compression type. Default=lzw ({})".format(','.join(mosaic.GTIFF_COMPRESSIONS)))
    parser.add_argument("--skip-cmd-txt", action='store_true', default=True,
                        help='Skip writing the txt file containing the input command.')
    parser.add_argument("--version", action='version', version="imagery_utils v{}".format(VERSION))

    
    #### Parse Arguments
    args = parser.parse_args()

    status = 0
        
    bands = args.bands
    inpath = args.src
    tile = args.tile
    ref_xres, ref_yres = args.resolution
    xmin, xmax, ymin, ymax = args.extent
    dims = "-tr {} {} -te {} {} {} {}".format(ref_xres, ref_yres, xmin, ymin, xmax, ymax)
    
    ##### Configure Logger
    logfile = os.path.splitext(tile)[0] + ".log"
    lfh = logging.FileHandler(logfile)
    lfh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh) 
    
    #### get working directory
    if args.wd:
        if os.path.isdir(args.wd):
            localpath = args.wd
        else:
            parser.error("scratch space directory does not exist: {0}".format(args.wd))
    else:
        localpath = os.path.dirname(tile)

    #### write input command to text file next to output folder for reference
    command_str = ' '.join(sys.argv)
    logger.info("Running command: {}".format(command_str))
    if not args.skip_cmd_txt:
        utils.write_input_command_txt(command_str,localpath)
        args.skip_cmd_txt = True
    
    intersects = []
    
    if os.path.isfile(inpath):
        t = open(inpath, 'r')
        for line in t.readlines():
            line = line.strip('\n').strip('\r')
            
            if ',' in line:
                image, median_string = line.split(',')
                iinfo = mosaic.ImageInfo(image, "IMAGE")
                median = {}
                for stat in median_string.split(";"):
                    k, v = stat.split(":")
                    median[int(k)] = float(v)
                if len(median) == iinfo.bands:
                    iinfo.set_raster_median(median)
                else:
                    logger.warning("Median dct length (%i) does not match band count (%i)", len(median), iinfo.bands)
            
            else:
                iinfo = mosaic.ImageInfo(line, "IMAGE")
            
            intersects.append(iinfo)
        t.close()
    else:
        logger.error("Intersecting image file does not exist: %i", inpath)

    logger.info(tile)

    logger.info("Number of image found in source file: %i", len(intersects))
    
    wd = os.path.join(localpath, os.path.splitext(os.path.basename(tile))[0])
    if not os.path.isdir(wd):
        os.makedirs(wd)
    localtile2 = os.path.join(wd, os.path.basename(tile))
    localtile1 = localtile2.replace(".tif", "_temp.tif")
    
    del_images = []
    images = {}
        
    #### Get Extent geometry 
    poly_wkt = 'POLYGON (( {} {}, {} {}, {} {}, {} {}, {} {} ))'.format(xmin, ymin, xmin, ymax, xmax, ymax, xmax, ymin,
                                                                        xmin, ymin)
    
    c = 0
    for iinfo in intersects:
            
        #### Check if bands number is correct
        mergefile = iinfo.srcfp

        if args.force_pan_to_multi and iinfo.bands > 1:
            if iinfo.bands == 1:
                mergefile = os.path.join(wd, os.path.basename(iinfo.srcfp)[:-4]) + "_merge.tif"
                cmd = 'gdal_merge.py -ps {} {} -separate -o "{}" "{}"'.format(ref_xres,
                                                                              ref_yres,
                                                                              mergefile,
                                                                              '" "'.join([iinfo.srcfp] * iinfo.bands))
                taskhandler.exec_cmd(cmd)
        srcnodata = " ".join([str(ndv) for ndv in iinfo.nodatavalue])

        if args.median_remove:
            dst = os.path.join(wd, os.path.basename(mergefile)[:-4]) + "_median_removed.tif"
            status = BandSubtractMedian(iinfo, dst)
            if status == 1:
                logger.error("BandSubtractMedian() failed on %s", mergefile)
                sys.exit(1)
            ds = gdal.Open(dst)
            if ds:
                srcnodata_val = ds.GetRasterBand(1).GetNoDataValue()
                srcnodata = " ".join([str(srcnodata_val)] * bands)
                mergefile = dst
            else:
                logger.error("BandSubtractMedian() failed at gdal.Open(%s)", dst)
                sys.exit(1)
            
        if c == 0:
            if os.path.isfile(localtile1):
                logger.info("localtile1 already exists")
                status = 1
                break
            cmd = 'gdalwarp {} -srcnodata "{}" -dstnodata "{}" "{}" "{}"'.format(dims, srcnodata, srcnodata, mergefile,
                                                                                 localtile1)
            taskhandler.exec_cmd(cmd)
            
        else:
            cmd = 'gdalwarp -srcnodata "{}" "{}" "{}"'.format(srcnodata, mergefile, localtile1)
            taskhandler.exec_cmd(cmd)
            
        c += 1
       
        if not mergefile == iinfo.srcfp:
            del_images.append(mergefile)
            
    del_images.append(localtile1)        
    
    if status == 0:
        ####  Write to Compressed file
        if os.path.isfile(localtile1):
            if args.gtiff_compression == 'lzw':
                compress_option = '-co "compress=lzw"'
            elif args.gtiff_compression == 'jpeg95':
                compress_option = '-co "compress=jpeg" -co "jpeg_quality=95"'
                
            cmd = 'gdal_translate -stats -of GTiff {} -co "PHOTOMETRIC=MINISBLACK" -co "TILED=YES" -co ' \
                  '"BIGTIFF=YES" "{}" "{}"'.format(compress_option, localtile1, localtile2)
            taskhandler.exec_cmd(cmd)
        
        ####  Build Pyramids        
        if os.path.isfile(localtile2):
            cmd = 'gdaladdo "{}" 2 4 8 16 30'.format(localtile2)
            taskhandler.exec_cmd(cmd)
        
        #### Copy tile to destination
        if os.path.isfile(localtile2):
            logger.info("Copying output files to destination dir")
            mosaic.copyall(localtile2, os.path.dirname(tile))
            
        del_images.append(localtile2)
    
    
    #### Delete temp files
    utils.delete_temp_files(del_images)
    shutil.rmtree(wd)
   
    logger.info("Done")


def BandSubtractMedian(iinfo, dstfp):
    # Subtract the median from each band of srcfp and write the result
    # to dstfp.
    # Band types byte, uint16 and int16 will be output as int16 with nodata -32768.
    # Band types uint32 and int32 will be output as int32 with nodata -2147483648.

    if not (iinfo.datatype in [1, 2, 3, 4, 5]):
        logger.error("BandSubtractMedian only works on integer data types")
        return 1
    elif iinfo.datatype in [1, 2, 3]:
        out_datatype = 3
        out_nodataval = -32768
        out_min = -32767
    else:
        out_datatype = 5
        out_nodataval = -2147483648
        out_min = -2147483647
    
    if not os.path.isfile(dstfp):
        gtiff_options = ['TILED=YES', 'COMPRESS=LZW', 'BIGTIFF=YES']
        driver = gdal.GetDriverByName('GTiff')
        out_ds = driver.Create(dstfp, iinfo.xsize, iinfo.ysize, iinfo.bands, out_datatype, gtiff_options)
        if not out_ds:
            logger.error("BandSubtractMedian(): !driver.Create(%s)", dstfp)
            return 1
        
        ds = gdal.Open(iinfo.srcfp)
        if not ds:
            logger.error("BandSubtractMedian(): !gdal.Open(%s)", iinfo.srcfp)
            return 1
        
        out_ds.SetGeoTransform(ds.GetGeoTransform())
        out_ds.SetProjection(ds.GetProjectionRef())
        
        ## check if median was passed in, calculate if not
        try:
            keys = list(iinfo.median.keys())
        except KeyError:
            iinfo.get_raster_median()
            keys = list(iinfo.median.keys())
        
        keys.sort()
        for band in keys:
            band_median = iinfo.median[band]
            if band_median is not None:
                band_data = ds.GetRasterBand(band)
                band_nodata = band_data.GetNoDataValue()
                # default nodata to zero
                if band_nodata is None:
                    logger.info("Defaulting band %i nodata to zero", band)
                    band_nodata = 0.0 
                band_array = numpy.array(band_data.ReadAsArray())
                nodata_mask = (band_array == band_nodata)

                if out_datatype == 3:
                    band_corrected = numpy.full_like(band_array, fill_value=out_nodataval, dtype=numpy.int16)
                else:
                    band_corrected = numpy.full_like(band_array, fill_value=out_nodataval, dtype=numpy.int32)
                band_valid = band_array[~nodata_mask]
                if band_valid.size != 0:          
                    band_min = numpy.min(band_valid)
                    corr_min = numpy.subtract(float(band_min), float(band_median))
                    if corr_min < float(out_min):
                        logger.error("BandSubtractMedian() returns min out of range for %s band %i", iinfo.srcfp, band)
                        return 1
                    band_corrected[~nodata_mask] = numpy.subtract(band_array[~nodata_mask], band_median)
                else:
                    logger.warning("Band %i has no valid data", band)
                out_band = out_ds.GetRasterBand(band)
                out_band.WriteArray(band_corrected)
                out_band.SetNoDataValue(out_nodataval)

            else:
                logger.error("BandSubtractMedian(): iinfo.median[%i] is None, image %s", band, iinfo.srcfp)
                return 1
        ds = None
        out_ds = None

    # ## redo pyramids -- WHY?
    # cmd = 'gdaladdo "%s" 2 4 8 16' %(srcfp)
    # taskhandler.exec_cmd(cmd)

    else:
        logger.info("BandSubtractMedian(): %s exists", dstfp)

    return 0


if __name__ == '__main__':
    main()
