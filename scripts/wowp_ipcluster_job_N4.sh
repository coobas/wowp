#!/bin/bash -l

#SBATCH -p regular
#SBATCH -N 4
#SBATCH -t 00:15:00
#SBATCH --cpus-per-task=1

#SBATCH -J wowp_test_job
#SBATCH -e wowp_test_job.err
#SBATCH -o wowp_test_job.out

umask 0022
cd $SLURM_SUBMIT_DIR

# module load python/2.7-anaconda
# source activate wowp
date
which python
echo "--- env ---"
env 
echo "--- env end ---"

echo "--- srun wowp_ipcluster_test ---"
job_file="$SLURM_SUBMIT_DIR""_$SLURM_JOB_NAME""_$SLURM_JOB_ID""_running"
echo "job file = $job_file"
date > $job_file
srun /global/homes/j/jurban/workspace/wowp/scripts/wowp_on_slurm --job-file $job_file /global/homes/j/jurban/workspace/test_job.py

# dsrun /global/homes/j/jurban/workspace/wowp/scripts/wowp_on_slurm -e distributed --job-file $job_file /global/homes/j/jurban/workspace/test_job.py

# srun /global/homes/j/jurban/workspace/wowp/scripts/wowp_on_slurm 

rm -f $job_file
echo "--- the end ---"
date
