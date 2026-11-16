
#!/bin/bash

# configuration
bench_dir="./PdF/01_prestudy"
results_dir="./PdF/04_distributed_clause_storage/results/_results_disturb_50_100"
nbenchmarks="$(cat ${bench_dir}/templates/benchmarks.txt.0 | wc -l)"
job_time_limit=300
time_limit=$(($nbenchmarks * ($job_time_limit - 100))) # assume a job takes an average time of 200s
nprocs=4
nthreads=2 # nprocs * nthreads = total cpus on one node


# command
cmd="
`# run mallob with 2 client (disturbed)` build/mallob -c=2 \
`# templates for the 1/2 client(s)` -client-template=${bench_dir}/templates/client-template-disturbed-50-100.json \
`# job templates` -job-template=${bench_dir}/templates/job-template.json \
`# job descriptions` -job-desc-template=${bench_dir}/templates/benchmarks.txt \
`# active jobs per client` -ajpc=1 \
`# loaded jobs per client` -ljpc=8 \
`# finish after n seconds` -T=${time_limit} \
`# threads per process` -t=${nthreads} \
`# shuffle job descriptions` -sjd=1 \
`# portfolio (k_ when satwithpre)` -satsolver=k_
`# output file` -sro=${results_dir}/results.json \
`# log dir` -log=${results_dir} \
`# verbosity` -v=4 \
`# dont wait for subprocesses to finish` -terminate-abruptly=1 \
`# sat config directory` -sat-config-dirs=config/sat/base/ \
`# preprocessor configuration` -preprocess-config=config/satwithpre/actors_onechain.json \
`# select clause history 0: none; 1: DCS` -ch=1 \
"

# log
echo "CONFIGURATION =============================="
echo "#benchmarks: $nbenchmarks"
echo "#processes: $nprocs"
echo "#threads_per_process: $nthreads"
echo "command: \ $cmd"
echo "time limit: $time_limit seconds"

# run job
echo "LOGGING ===================================="
mpirun -n $nprocs --bind-to core --map-by core $cmd

echo "job finished"
