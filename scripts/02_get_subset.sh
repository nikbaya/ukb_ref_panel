#!/usr/bin/env bash
#
# Creates reference panel by extracting subset of samples from UKB WES
#
# Author: Nik Baya (2021-08-10)
#
#$ -N get_subset
#$ -wd /well/lindgren/UKBIOBANK/nbaya/resources/ref/ukb_wes_200k/ukb_wes_ref_panel
#$ -o logs/get_subset.log
#$ -e logs/get_subset.log
#$ -P lindgren.prjc
#$ -pe shmem 8
#$ -q short.qe
#$ -t 1-22

set -o errexit
set -o nounset
module purge
source utils/bash_utils.sh

# directories
readonly in_dir="/well/lindgren/UKBIOBANK/nbaya/wes_200k/ukb_wes_qc/data/filtered"
readonly spark_dir="logs/spark"
readonly vcf_dir="data/vcf"
readonly plink_dir="data/plink"

# options
readonly chr=${SGE_TASK_ID} # only works for autosomes
readonly n_samples=5000
readonly ancestry="eur" # ancestry to subset to before sampling. Options: "eur" (genetically-confirmed Europeans), "all" (all ancestries)

# input path
readonly in="${in_dir}/ukb_wes_200k_filtered_chr${chr}.mt"

# output paths
readonly out_prefix="ukb_wes_200k_${ancestry}_ref_panel_5k_chr${chr}"
readonly out_vcf="${vcf_dir}/${out_prefix}.vcf.bgz"
readonly out_plink="${plink_dir}/${out_prefix}"

# hail script
readonly hail_script="utils/get_subset_hail.py"

if [ $( wc -l ${out_plink}.{bed,bim,fam} ) -ne 3 ]; then
  SECONDS=0
  mkdir -p ${plink_dir}
  set_up_hail

  python3 ${hail_script} \
    --input_path ${in} \
    --input_type "mt" \
    --num_samples ${num_samples} \
    --ancestry ${ancestry} \
    --out_path ${out_plink} \
    --out_type "plink"

  print_update "Finished writing ${out_plink}.{bed,bim,fam}" "${SECONDS}"
fi


