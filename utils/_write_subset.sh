#!/usr/bin/env bash
#
# Extracts subset of samples from a dataset
#
# Author: Nik Baya (2021-08-17)
#
#$ -N _write_subset
#$ -wd /well/lindgren/UKBIOBANK/nbaya/resources/ref/ukb_wes_200k/ukb_ref_panel
#$ -o logs/write_subset.log
#$ -e logs/write_subset.log
#$ -P lindgren.prjc
#$ -pe shmem 8
#$ -q short.qe

set -o errexit
set -o nounset
module purge
source utils/bash_utils.sh

# directories
readonly spark_dir="logs/spark"

# input args
readonly input_path_template=$1
readonly input_type=$2
readonly individuals_to_keep=$3
readonly output_path_template=$4
readonly output_type=$5
readonly bgen_samples="$6"

# parsed input args
readonly input_path=$( echo ${input_path_template} | sed "s/<CHROM>/${SGE_TASK_ID}/g")
readonly output_path=$( echo ${output_path_template} | sed "s/<CHROM>/${SGE_TASK_ID}/g")

# hail script
readonly hail_script="utils/get_subset_hail.py"

SECONDS=0
set_up_hail

python3 ${hail_script} \
  --input_path ${input_path} \
  --input_type ${input_type} \
  --individuals_to_keep ${individuals_to_keep} \
  --output_path ${output_path} \
  --output_type ${output_type} \
  --bgen_samples ${bgen_samples}

print_update "Finished writing dataset of type=${output_type} to ${output_path}" "${SECONDS}"

