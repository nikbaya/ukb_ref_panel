#!/usr/bin/env bash
#
# Creates reference panel by extracting subset of samples from UKB imputed V3
#
# Author: Nik Baya (2021-08-12)
#
#$ -N get_imp_subset
#$ -wd /well/lindgren/UKBIOBANK/nbaya/resources/ref/ukb_wes_200k/ukb_wes_ref_panel
#$ -o logs/get_imp_subset.log
#$ -e logs/get_imp_subset.log
#$ -P lindgren.prjc
#$ -pe shmem 8
#$ -q short.qe
#$ -t 1-22

set -o errexit
set -o nounset
module purge
source utils/bash_utils.sh

# directories
readonly in_dir="/well/ukbb-wtchg/v3/imputation"
readonly spark_dir="logs/spark"
readonly vcf_dir="data/vcf"
readonly plink_dir="data/plink"

# options
readonly chr=${SGE_TASK_ID} # only works for autosomes
readonly num_samples=5000
readonly ancestry="eur" # ancestry to subset to before sampling. Options: "eur" (genetically-confirmed Europeans), "all" (all ancestries)

# input path
readonly in="${in_dir}/ukb_imp_chr${chr}_v3.bgen"

# output paths
readonly out_prefix="ukb_imp_v3_${ancestry}_ref_panel_$(( num_samples / 1000 ))k_chr${chr}"
readonly out_vcf="${vcf_dir}/${out_prefix}.vcf.bgz"
readonly out_plink="${plink_dir}/${out_prefix}"

# hail script
readonly hail_script="utils/get_subset_hail.py"

if [ $( ls -1 ${out_prefix}.{bed,bim,fam} 2> /dev/null | wc -l ) -ne 3 ]; then
  SECONDS=0
  mkdir -p ${plink_dir}
  set_up_hail

  python3 ${hail_script} \
    --input_path ${in} \
    --input_type "bgen" \
    --num_samples ${num_samples} \
    --ancestry ${ancestry} \
    --output_path ${out_plink} \
    --output_type "plink"

  print_update "Finished writing ${out_plink}.{bed,bim,fam}" "${SECONDS}"
fi


