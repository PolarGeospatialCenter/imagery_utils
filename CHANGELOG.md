History
=======

2.2.0 (2024-10-31)
------------------

* Add slurm queue arg to ortho and pansharpen scripts by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/84
* Fix ortho metadata file for IKONOS imagery by @dannyim in https://github.com/PolarGeospatialCenter/imagery_utils/pull/85
* add stack_landsat.py script to the repo by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/87
* Mosaic exclude list from sandwich by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/86
* Pansharpened mosaic selection by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/88
* Remove default cmd txt behavior  by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/89
* Update radiometric calibration factors and add CAVIS by @clairecporter in https://github.com/PolarGeospatialCenter/imagery_utils/pull/90

2.1.3 (2024-07-03)
------------------

* Minor bugfix for slurm job submission memory settings

2.1.2 (2024-07-02)
------------------

* add option for passing custom slurm job name by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/80
* Patch for running pansharpen in mamba env on windows by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/81
* Slurm script default settings by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/82
* auto DEM windows bug fix by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/83

2.1.1 (2024-05-23)
------------------

* patch to fix processing bug with `auto` DEM flag

2.1.0 (2024-05-17)
------------------

* add option to write input command to txt file next to output dir by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/52
* Add functionality for accepting ESRI codes in the EPSG arg by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/53
* Minor bug fixes for writing out the input command by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/54
* Add `--queue` arg to ortho/pansh scripts by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/56
* Fix `ElementTree.getiterator()` call, removed in Python 3.8 by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/57
* Fix ortho of multi-band-separate-files Ikonos imagery by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/58
* Apply all script arg settings to pansharpen outputs by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/62
* use EARLIESTACQTIME if FIRSTLINETIME is not in the metadata file by @dannyim in https://github.com/PolarGeospatialCenter/imagery_utils/pull/61
* Add `--epsg-auto-nad83` option to use NAD83 datum for auto UTM projection by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/63
* Remove stacked Ikonos NTF temp file with other temp files by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/64
* Set SLURM job log filenames to match PBS job log filenames by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/65
* Repo readme update by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/66
* Fix: Assign NoData to a value outside of the valid data range for outputs of pgc_ortho.py and pgc_pansharpen.py by @power720 in https://github.com/PolarGeospatialCenter/imagery_utils/pull/74
* bug fix for inadvertent testing commit by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/77
* Add auto DEM functionality to pgc_ortho.py by @SAliHossaini in https://github.com/PolarGeospatialCenter/imagery_utils/pull/76
* Slurm log location option by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/78
* Update orthoing code to handle SWIR and CAVIS by @clairecporter in https://github.com/PolarGeospatialCenter/imagery_utils/pull/75

2.1.0 (2022-06-14)
------------------

* Added "REGION_ID" field to DemInfo attributes by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/18
* bugfix: add allow_invalid_geom to qsub key removal list by @stevefoga in https://github.com/PolarGeospatialCenter/imagery_utils/pull/19
* Changes to to address rare bug in pgc_ortho.py --tasks-per-job feature by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/20
* Threading for gdalwarp and gdal_pansharpen by @stevefoga in https://github.com/PolarGeospatialCenter/imagery_utils/pull/21
* Automatically scale default memory request for ortho and pansharpen jobs by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/22
* Update osgeo import syntax by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/24
* Bugfix escaped quotes in command string for parallel processing by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/25
* Change gdal version to 2.1.3 in qsub scripts to resolve numpy issue by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/28
* Show full stack trace in error messages/logs by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/30
* Refactor changes to remove duplicate code by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/32
* Updated pgc_ortho.py to check for existing .vrt files that are left b… by @bagl0025 in https://github.com/PolarGeospatialCenter/imagery_utils/pull/31
* Enable footprinting DG ortho imagery using image GCPs by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/33
* Added JPEG support to format list.  by @bagl0025 in https://github.com/PolarGeospatialCenter/imagery_utils/pull/35
* pgc_ortho.py new CSV argument list source type by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/34
* Update regex in doesCross180() to accept lat/lon integer values, not just floats by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/36
* Added gdalwarp --tap argument, couple bugfixes by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/37
* Automatic output ortho/pansh EPSG settings by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/38
* Fix bytes to string error when extracting RPB from tarfile by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/40
* Subset VRT tile mosaic DEM argument using src CSV argument list by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/39
* Miscellaneous fixes by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/42
* Fix bug where float32 outputs are integer values on pgc_ortho.py by @clairecporter in https://github.com/PolarGeospatialCenter/imagery_utils/pull/43
* Revert "Fix bug where float32 outputs are integer values on pgc_ortho.py" by @clairecporter in https://github.com/PolarGeospatialCenter/imagery_utils/pull/44
* Repair introduced bug in taskhandler.py by @clairecporter in https://github.com/PolarGeospatialCenter/imagery_utils/pull/45
* Undo unintented revert by @clairecporter in https://github.com/PolarGeospatialCenter/imagery_utils/pull/46
* Change standard GDAL GTiff creation option from 'BIGTIFF=IF_SAFER' to… by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/47
* Add check in pgc_pansharpen.py to match pan and mul scenes that differ by 1 sec by @bakkerbakker in https://github.com/PolarGeospatialCenter/imagery_utils/pull/48
* Adjust ortho image metadata for old GeoEye and Ikonos imagery so fp.py works on them by @ehusby in https://github.com/PolarGeospatialCenter/imagery_utils/pull/50
* Update Versioning Scheme by @clairecporter in https://github.com/PolarGeospatialCenter/imagery_utils/pull/51