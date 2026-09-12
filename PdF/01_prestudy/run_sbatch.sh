#!/bin/bash
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --ntasks-per-core=2

#configuration
dir="./PdF/01_prestudy"
results_dir="${dir}/results"
nbenchmarks="$(cat ${dir}/templates/benchmarks.txt.0 | wc -l)"

# command
cmd="
`# run mallob with 2 clients` build/mallob -c=2 \
`# templates for the 2 clients` -client-template=${dir}/templates/client-template.json \
`# job templates` -job-template=${dir}/templates/job-template.json \
`# job descriptions` -job-desc-template=${dir}/templates/benchmarks.txt \
`# active jobs per client` -ajpc=1 \
`# loaded jobs per client` -ljpc=8 \
`# job wallclock limit` -jwl=300 \
`# threads per process` -t=2 \
`# finish only when all benchmarks are finidhed` -SJ=$nbenchmarks \
`# shuffle job descriptions` -sjd=1 \
`# portfolio` -satsolver=c
`# output file` -sro=${results_dir}/results.json \
`# log dir` -log=${results_dir} \
`# verbosity` -v=2
"

# clear results dir
rm -r ${dir}/results/*

echo JOB_LAUNCHING
echo $cmd
mpirun -n $SLURM_NTASKS --bind-to core --map-by numa $cmd
echo JOB_FINISHED
