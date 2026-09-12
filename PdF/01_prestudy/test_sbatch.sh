#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-core=1 # hyperthreading
#SBATCH -t 10:00:00
#SBATCH -p micro # for nodes<=16
#SBATCH -J mallob-test

# run test script
bash scripts/run/systest.sh mono drysched sched osc
