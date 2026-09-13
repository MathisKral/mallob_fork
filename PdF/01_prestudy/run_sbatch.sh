#!/bin/bash
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --ntasks-per-core=2
#SBATCH -J mallob-malleable-benchmark

# log number of ranks
echo "#ranks: $SLURM_NTASKS"

dir="./PdF/01_prestudy"
nbenchmarks="$(cat ${dir}/templates/benchmarks.txt.0 | wc -l)"

# log number of benchmarks
echo "#benchmarks: $nbenchmarks"

# ===============
# 50% disturbance
# ===============

#configuration
results_dir="${dir}/results/${SLURM_JOB_ID}_results_50"

# command
cmd="
`# run mallob with 2 clients` build/mallob -c=2 \
`# templates for the 2 clients` -client-template=${dir}/templates/client-template-50.json \
`# job templates` -job-template=${dir}/templates/job-template.json \
`# job descriptions` -job-desc-template=${dir}/templates/benchmarks.txt \
`# active jobs per client` -ajpc=1 \
`# loaded jobs per client` -ljpc=8 \
`# job wallclock limit` -jwl=300 \
`# threads per process` -t=16 \
`# shuffle job descriptions` -sjd=1 \
`# portfolio` -satsolver=c
`# output file` -sro=${results_dir}/results.json \
`# log dir` -log=${results_dir} \
`# verbosity` -v=2
"

echo "launching mallob with 50% disturbance"
echo $cmd
mpirun -n $SLURM_NTASKS --bind-to core --map-by numa $cmd
echo "finished mallob with 50% disturbance"


# ===============
# 75% disturbance
# ===============

#configuration
results_dir="${dir}/results/${SLURM_JOB_ID}_results_75"

# command
cmd="
`# run mallob with 2 clients` build/mallob -c=2 \
`# templates for the 2 clients` -client-template=${dir}/templates/client-template-75.json \
`# job templates` -job-template=${dir}/templates/job-template.json \
`# job descriptions` -job-desc-template=${dir}/templates/benchmarks.txt \
`# active jobs per client` -ajpc=1 \
`# loaded jobs per client` -ljpc=8 \
`# job wallclock limit` -jwl=300 \
`# threads per process` -t=16 \
`# shuffle job descriptions` -sjd=1 \
`# portfolio` -satsolver=c
`# output file` -sro=${results_dir}/results.json \
`# log dir` -log=${results_dir} \
`# verbosity` -v=2
"

echo "launching mallob with 75% disturbance"
echo $cmd
mpirun -n $SLURM_NTASKS --bind-to core --map-by numa $cmd
echo "finished mallob with 57% disturbance"



# ===============
# 0% disturbance
# ===============

#configuration
results_dir="${dir}/results/${SLURM_JOB_ID}_results_00"

# command
cmd="
`# run mallob with 1 client` build/mallob -c=1 \
`# templates for the 2 clients` -client-template=${dir}/templates/client-template-00.json \
`# job templates` -job-template=${dir}/templates/job-template.json \
`# job descriptions` -job-desc-template=${dir}/templates/benchmarks.txt \
`# active jobs per client` -ajpc=1 \
`# loaded jobs per client` -ljpc=8 \
`# job wallclock limit` -jwl=300 \
`# threads per process` -t=16 \
`# shuffle job descriptions` -sjd=1 \
`# portfolio` -satsolver=c
`# output file` -sro=${results_dir}/results.json \
`# log dir` -log=${results_dir} \
`# verbosity` -v=2
"

echo "launching mallob with 0% disturbance"
echo $cmd
mpirun -n $SLURM_NTASKS --bind-to core --map-by numa $cmd
echo "finished mallob with 0% disturbance"
