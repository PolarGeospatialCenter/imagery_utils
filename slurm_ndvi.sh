#!/bin/bash

# number of nodes
#SBATCH -N 1

# number of cpus per task
#SBATCH -c 2

# job log path
#SBATCH -o slurm.%N.%j.out
#SBATCH -e slurm.%N.%j.err

# init gdal tools
source /opt/PGSC-2.1.0/init-gdal.sh

echo $p1
python $p1
