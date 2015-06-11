#!/bin/bash

#PBS -l walltime=24:00:00,nodes=1:ppn=2
#PBS -m n
#PBS -k oe
#PBS -j oe

cd $PBS_O_WORKDIR

echo $PBS_JOBID
echo $PBS_O_HOST
echo $PBS_NODEFILE
echo $a1


module load gdal/1.11.1
module load dans-gdal-scripts
module unload gdal/1.9.0 gcc/4.6.3 geos/3.3.0 python/2.7.2 openjpeg/2-debug proj/4.7.0 cfitsio/3.26

echo $p1
python $p1
