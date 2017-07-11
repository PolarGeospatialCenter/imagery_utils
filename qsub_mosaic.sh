#!/bin/bash

#PBS -l nodes=1:ppn=32
#PBS -l walltime=48:00:00
#PBS -m n
#PBS -k oe
#PBS -j oe

module load gdal/2.1.1

python $p1
