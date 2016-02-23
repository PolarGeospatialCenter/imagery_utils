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


module load gdal/2.0.0-FileGDB
module load dans-gdal-scripts/20150904

echo $p1
python $p1
