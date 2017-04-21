#!/bin/bash

# number of nodes
#SBATCH -N 1

# number of cpus per task
#SBATCH -c 2

# job log path
##SBATCH -o opath

#SBATCH --mail-type=NONE

# init gdal tools
module load gdal/2.1.1

echo $p1
python $p1
