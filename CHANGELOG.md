History
=======

2.2.1 (2025-09-09)
------------------
* Mosaic settings updates
* Allow pgc_ortho and pgc_pansharpen to accept 1 or two values for resolution
* Make Unittest style test runable by pytest
* Auto DEM Bug Repair
* Gdal 393

2.2.0 (2024-10-31)
------------------

* Add slurm queue arg to ortho and pansharpen scripts
* Fix ortho metadata file for IKONOS imagery
* add stack_landsat.py script to the repo
* Mosaic exclude list from sandwich
* Pansharpened mosaic selection
* Remove default cmd txt behavior 
* Update radiometric calibration factors and add CAVIS

2.1.3 (2024-07-03)
------------------

* Minor bugfix for slurm job submission memory settings

2.1.2 (2024-07-02)
------------------

* add option for passing custom slurm job name
* Patch for running pansharpen in mamba env on windows
* Slurm script default settings
* auto DEM windows bug fix

2.1.1 (2024-05-23)
------------------

* patch to fix processing bug with `auto` DEM flag

2.1.0 (2024-05-17)
------------------

* add option to write input command to txt file next to output dir
* Add functionality for accepting ESRI codes in the EPSG arg
* Minor bug fixes for writing out the input command
* Add `--queue` arg to ortho/pansh scripts
* Fix `ElementTree.getiterator()` call, removed in Python 3.8
* Fix ortho of multi-band-separate-files Ikonos imagery
* Apply all script arg settings to pansharpen outputs
* use EARLIESTACQTIME if FIRSTLINETIME is not in the metadata file
* Add `--epsg-auto-nad83` option to use NAD83 datum for auto UTM projection
* Remove stacked Ikonos NTF temp file with other temp files
* Set SLURM job log filenames to match PBS job log filenames
* Repo readme update
* Fix: Assign NoData to a value outside of the valid data range for outputs of pgc_ortho.py and pgc_pansharpen.py
* bug fix for inadvertent testing commit
* Add auto DEM functionality to pgc_ortho.py
* Slurm log location option
* Update orthoing code to handle SWIR and CAVIS

2.1.0 (2022-06-14)
------------------

* Added "REGION_ID" field to DemInfo attributes
* bugfix: add allow_invalid_geom to qsub key removal list
* Changes to address rare bug in pgc_ortho.py --tasks-per-job feature
* Threading for gdalwarp and gdal_pansharpen
* Automatically scale default memory request for ortho and pansharpen jobs
* Update osgeo import syntax
* Bugfix escaped quotes in command string for parallel processing
* Change gdal version to 2.1.3 in qsub scripts to resolve numpy issue
* Show full stack trace in error messages/logs
* Refactor changes to remove duplicate code
* Updated pgc_ortho.py to check for existing .vrt files that are left b…
* Enable footprinting DG ortho imagery using image GCPs
* Added JPEG support to format list. 
* pgc_ortho.py new CSV argument list source type
* Update regex in doesCross180() to accept lat/lon integer values, not just floats
* Added gdalwarp --tap argument, couple bugfixes
* Automatic output ortho/pansh EPSG settings
* Fix bytes to string error when extracting RPB from tarfile
* Subset VRT tile mosaic DEM argument using src CSV argument list
* Miscellaneous fixes
* Fix bug where float32 outputs are integer values on pgc_ortho.py
* Revert "Fix bug where float32 outputs are integer values on pgc_ortho.py"
* Repair introduced bug in taskhandler.py
* Undo unintented revert
* Change standard GDAL GTiff creation option from 'BIGTIFF=IF_SAFER' to…
* Add check in pgc_pansharpen.py to match pan and mul scenes that differ by 1 sec
* Adjust ortho image metadata for old GeoEye and Ikonos imagery so fp.py works on them
* Update Versioning Scheme
