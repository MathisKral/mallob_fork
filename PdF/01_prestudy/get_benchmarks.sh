dir="./PdF/01_prestudy"
dir_esc=".\/PdF\/01_prestudy"

# clrear directories
if [ $(ls ${dir}/_benchmarks | wc -l) -gt 0 ]; then
    rm -r ${dir}/_benchmarks/*
fi

if [ $(ls ${dir}/_benchmarks_hard | wc -l) -gt 0 ]; then
    rm -r ${dir}/_benchmarks_hard/*
fi

wget -P ${dir}/_benchmarks --content-disposition -i ${dir}/main_benchmarks_combined_2025_2026.uri

# get the "unsolvable" instance (mallob timeout in competition results)
wget -P ${dir}/_benchmarks_hard --content-disposition https://benchmark-database.de/file/1dce69ee6685597d6c56e5fd7a47f8e0

# unpack benchmarks
# for file in ${dir}/benchmarks/*; do
#     unxz $file
# done

# write benchmarkfiles
ls ${dir}/_benchmarks > ${dir}/templates/benchmarks.txt.0
ls ${dir}/_benchmarks_hard > ${dir}/templates/benchmarks.txt.1

# add file paths to benchmarkfiles
sed -i 's/^/'${dir_esc}'\/_benchmarks\//' ${dir}/templates/benchmarks.txt.0
sed -i 's/^/'${dir_esc}'\/_benchmarks_hard\//' ${dir}/templates/benchmarks.txt.1

