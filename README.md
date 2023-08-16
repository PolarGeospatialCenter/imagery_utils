# PGC Imagery Utils


## INTRODUCTION
PGC Imagery Utils is a collection of commercial satellite imagery manipulation tools to handle batch processing of a few tasks:

1) Correct for terrain and radiometry
2) Mosaic several images into one set of tiles
3) Pansharpen a multispectral image with its panchromatic partner
4) Calculate an NDVI raster from a multispectral image.

These tools are build on the GDAL/OGR image processing API using Python.  The code is built to run primarily on a Linux HPC cluster running Maui/Torque for queue management.  Some of the tools will work on a Windows platform.

The code is tightly coupled to the systems on which it was developed.  You should have no expectation of it running on another system without some patching.

## SCRIPT USAGE
Files starting with "qsub" and "slurm" are PBS and SLURM submission scripts.  See the script-specific documentation for more details on usage.

### ORTHO

The orthorectification script can correct for terrain displacement and radiometric settings as well as alter the bit depth of the imagery.  Using the --pbs or --slurm options will submit the jobs to an Torque job scheduler.  Alternatively, using the --parallel-processes option will instruct the script to run multiple tasks in parallel.  Using --threads N will enable threading for gdalwarp, where N is the number of threads (or ALL_CPUS); this option will not work with --pbs/--slurm, and (threads * parallel processes) cannot exceed number of threads available on system.

Example:
```
python pgc_ortho.py --epsg 3031 --dem DEM.tif --format GTiff --stretch ns --outtype UInt16 input_dir output dir
```

This example will take all the nitf or tif files in the input_dir and orthorectify them using DEM.tif.  The output files will be written to output_dir and be 16 bit (like the original image) GeoTiffs with no stretch applied with a spatial reference of EPSG 3031, or Antarctic Polar Stereographic -71.

### MOSAIC

The mosaicking toolset mosaics multiple input images into a set of non-overlapping output tile images.  It can sort the images according to several factors including cloud cover, sun elevation angle, off-nadir angle, probability of overexposure, and proximity to a specific date.  It consists of 3 scripts:

1. pgc_mosaic.py - initializes the output mosaic, creates cutlines, and run the subtile processes.
2. pgc_mosaic_query_index.py - takes mosaic parameters and a shapefile index and determines which images will contribute to the resulting mosaic. The resulting list can be used to reduce the number of images that are run through the orthorectification script to those that will be eventually used.
3. pgc_mosaic_build_tile.py - builds an individual mosaic tile.  This script is invoked by pgc_mosaic.

Example:
```
python pgc_mosaic.py --pbs --bands 1 --tilesize 20000 20000 --resolution 0.5 0.5 input_dir output_mosaic_name
```

This example will evaluate all the 1-band images in input_dir and sort them according to their quality score.  It will submit a job to the cluster queue to build each tile of size 20,000 x 20,000 pixels at 0.5 meters resolution.  The output tiles will be Geotiffs named by appending a row and column identifier to the output_mosaic_name.

### PANSHARPEN

The pansharpening utility applies the orthorectification process to both the pan and multi image in a pair and then pansharpens them using the GDAL tool gdal_pansharpen.  GDAL 2.1 is required for this tool to function.  The --threads flag will apply threading to both gdalwarp and gdal_pansharpen operations.

### NDVI

The NDVI utility calculates NDVI from multispectral image(s).  The tool is designed to run on data that have already been run through the pgc_ortho utility.

## INSTALLATION AND DEPENDANCIES
PGC uses the Mamabaforge installer to build our Python/GDAL software stack.  You can find installers for your OS here:
https://github.com/conda-forge/miniforge#mambaforge

Users should expect a recent (less than 1-2 years old) version of Python and GDAL to be compatible with tools in this repo.
The following conda/mamba environment likely contains more dependencies than are needed for tools in this repo, but should suffice:
```
conda create --name pgc -c conda-forge python=3.11 gdal=3.6.4 numpy scipy pandas geopandas rasterio shapely postgresql psycopg2 sqlalchemy configargparse lxml pathlib2 python-dateutil pytest rtree xlsxwriter
```


## CONTACT
Claire Porter
Polar Geospatial Center
porte254@umn.edu
