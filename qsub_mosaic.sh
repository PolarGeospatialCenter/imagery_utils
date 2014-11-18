#!/bin/bash

#PBS -l nodes=1:ppn=8
#PBS -l walltime=24:00:00
#PBS -m n
#PBS -k oe
#PBS -j oe

module load gdal/1.10.0

python $p1
