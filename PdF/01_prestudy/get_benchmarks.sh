dir="./PdF/01_prestudy"

# clrear directories
rm ${dir}/benchmarks/*
rm ${dir}/benchmarks_hard/*

wget -P ${dir}/benchmarks --content-disposition -i ${dir}/track_main_2026.uri
wget -P ${dir}/benchmarks_hard https://raw.githubusercontent.com/Byt-wyze-technology/sat-instances-library/refs/heads/main/instances/sat-Hard-C-445935conflicts-transition.cnf

# unpack benchmarks
# for file in ${dir}/benchmarks/*; do
#     unxz $file
# done

# write benchmarkfiles
ls ${dir}/benchmarks > ${dir}/templates/benchmarks.txt.0
ls ${dir}/benchmarks_hard > ${dir}/templates/benchmarks.txt.1

