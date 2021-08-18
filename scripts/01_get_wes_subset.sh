#!/usr/bin/env bash
#
# Creates reference panel by extracting subset of samples from UKB WES
#
# Author: Nik Baya (2021-08-18)
#
#$ -N get_wes_subset
#$ -wd /well/lindgren/UKBIOBANK/nbaya/resources/ref/ukb_wes_200k/ukb_ref_panel
#$ -o logs/get_wes_subset.log
#$ -e logs/get_wes_subset.log
#$ -P lindgren.prjc
#$ -pe shmem 1
#$ -q short.qe
#$ -V # necessar for `qsub` command to be available

set -o errexit
set -o nounset
module purge
source utils/bash_utils.sh

# directories
readonly in_dir="/well/lindgren/UKBIOBANK/nbaya/wes_200k/ukb_wes_qc/data/filtered"
readonly spark_dir="logs/spark"
readonly samples_dir="data/samples"

# options
readonly num_samples=5000
readonly ancestry="eur" # ancestry to subset to before sampling. Options: "eur" (genetically-confirmed Europeans), "all" (all ancestries)

# input path
readonly input_prefix="${in_dir}/ukb_wes_200k_filtered"
readonly input_type="mt"
readonly input_path="${input_prefix}_chr21.${input_type}" # use chr21 MatrixTable because it's the smallest for WES and fastest to work with

# output paths
readonly output_prefix="ukb_wes_200k_${ancestry}_ref_panel_$(( num_samples / 1000 ))k"
readonly output_path="${samples_dir}/${output_prefix}.tsv"

# hail script
readonly hail_script="utils/get_subset_hail.py"

# write list of samples in subset
if [ ! -f ${output_path} ]; then
  SECONDS=0
  mkdir -p ${samples_dir}
  set_up_hail

  python3 ${hail_script} \
    --input_path ${input_path} \
    --input_type ${input_type} \
    --choose_samples \
    --num_samples ${num_samples} \
    --ancestry ${ancestry} \
    --output_path "${output_path}" \
    --output_type "tsv"

  print_update "Finished writing ${output_path}" "${SECONDS}"
fi

# submit jobs for extracting chosen sample subset from each per-chromosome file
readonly write_subset_script="utils/_write_subset.sh"

readonly input_path_template="${input_prefix}_chr<CHROM>.mt"
readonly output_type="plink"
readonly output_path_template="data/${output_type}/${output_prefix}_chr<CHROM>" # if output_type="plink", don't include a file suffix
readonly individuals_to_keep=${output_path}

qsub -N "_write_wes_subset" \
  -t 21-22 \
  -pe shmem 1 \
  -o ${SGE_STDOUT_PATH} \
  -e ${SGE_STDERR_PATH} \
  ${write_subset_script} \
  ${input_path_template} \
  "mt" \
  ${individuals_to_keep} \
  ${output_path_template} \
  ${output_type}
