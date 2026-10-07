# Prestudy Disturbance Benchmarks
- benchmark from 2024 MallobSAT paper
- BUT: slightly different run configurations

## Benchmarks
- take combined set of benchmarks from 2025 and 2026 main track of limited size
- query: `(track=main_2026 or track=main_2025) and variables<12000` (main_benchmarks_combined_2025_2026)
- 405 benchmarks total
- disturbance task that mallob cannot solve

## Run Configurations
- 100% ressources undisturbed as baseline
- 75% ressources undisturbed 
- 50%-100% ressources disturbed 
- 62.5% ressources undisturbed
- 25%-100% ressources disturbed

## What to compare
- 75% undisturbed <-> 50%-100% disturbed (should ideally be equal)
- 62.5% undisturbed <-> 25%-100% disturbed (should ideally be equal)