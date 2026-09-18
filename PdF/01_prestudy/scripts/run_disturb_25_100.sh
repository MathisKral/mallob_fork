#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=192
#SBATCH --ntasks-per-core=2  # use hyperthreading
#SBATCH -J mallob-disturbance-25-100

# ATTENTION: this benchmark should be performed on partition "diffie" or "hellman"

# configuration
dir="./PdF/01_prestudy"
results_dir="${dir}/results/${SLURM_JOB_ID}_results_disdurb_25_100"
nbenchmarks="$(cat ${dir}/templates/benchmarks.txt.0 | wc -l)"
job_time_limit=300
time_limit=$(($nbenchmarks * ($job_time_limit * 0.85))) # assume a job takes an average time of 0.85*300s
nprocs=64
nthreads=3 # nprocs * nthreads = total cpus on one node


# command
cmd="
`# run mallob with 2 client (disturbed)` build/mallob -c=2 \
`# templates for the 1/2 client(s)` -client-template=${dir}/templates/client-template-disturbed-25-100.json \
`# job templates` -job-template=${dir}/templates/job-template.json \
`# job descriptions` -job-desc-template=${dir}/templates/benchmarks.txt \
`# active jobs per client` -ajpc=1 \
`# loaded jobs per client` -ljpc=8 \
`# finish after n seconds` -T=${time_limit} \
`# threads per process` -t=${nthreads} \
`# shuffle job descriptions` -sjd=1 \
`# portfolio` -satsolver=k_
`# output file` -sro=${results_dir}/results.json \
`# log dir` -log=${results_dir} \
`# verbosity` -v=2 \
`# dont wait for subprocesses to finish` -terminate-abruptly=1 \
`# sat config directory` -sat-config-dirs=config/sat/base/ \
`# preprocessor configuration` -preprocess-config=config/satwithpre/actors_onechain.json
"

# log
echo "CONFIGURATION =============================="
echo "#benchmarks: $nbenchmarks"
echo "#nodes: $SLURM_NTASKS"
echo "#processes: $nprocs"
echo "#threads_per_process: $nthreads"
echo "command: \n $cmd"
echo "time limit: $time_limit seconds"

# run job
echo "LOGGING ===================================="
mpirun -n $nprocs --bind-to core --map-by core $cmd

echo "job finished"