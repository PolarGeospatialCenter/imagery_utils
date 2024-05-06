#!/bin/bash

# number of nodes
#SBATCH -N 1

# number of cpus per task
#SBATCH -c 24

# job log path
#SBATCH -o %x.o%j
#SBATCH -e %x.o%j

echo ________________________________________
echo
echo SLURM Job Log
echo Start time: $(date)
echo
echo Job name: $SLURM_JOB_NAME
echo Job ID: $SLURM_JOBID
echo Submitted by user: $USER
echo User effective group ID: $(id -ng)
echo
echo SLURM account used: $SLURM_ACCOUNT
echo Hostname of submission: $SLURM_SUBMIT_HOST
echo Submitted to cluster: $SLURM_CLUSTER_NAME
echo Submitted to node: $SLURMD_NODENAME
echo Cores on node: $SLURM_CPUS_ON_NODE
echo Requested cores per task: $SLURM_CPUS_PER_TASK
echo Requested cores per job: $SLURM_NTASKS
echo Requested walltime: $SBATCH_TIMELIMIT
echo Nodes assigned to job: $SLURM_JOB_NODELIST
echo Running node index: $SLURM_NODEID
echo
echo Running on hostname: $HOSTNAME
echo Parent PID: $PPID
echo Process PID: $$
echo
echo Working directory: $SLURM_SUBMIT_DIR
echo ________________________________________________________
echo

# init gdal tools
source ~/.bashrc; conda activate pgc

echo $p1
time eval $p1
