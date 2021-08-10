#!/usr/bin/env bash
#
# Creates reference panel by extracting subset of samples from UKB WES
#
# Author: Nik Baya (2021-08-04)
#
#$ -N create_ref_panel
#$ -wd /well/lindgren/UKBIOBANK/nbaya/resources/ref/ukb_wes_200k/ukb_wes_ref_panel
#$ -o logs/create_ref_panel.log
#$ -e logs/create_ref_panel.log
#$ -P lindgren.prjc
#$ -pe shmem 8
#$ -q short.qe
#$ -t 1-22

set -o errexit
set -o nounset
module purge

# directories
readonly in_dir="/well/lindgren/UKBIOBANK/nbaya/wes_200k/ukb_wes_qc/data/filtered"
readonly vcf_dir="data/vcf"
readonly plink_dir="data/plink"

# input path
readonly n_samples=5000
readonly samples="data/ukb_wes_200k_ref_panel_$(( n_samples / 1000 ))k.txt"
readonly chr=${SGE_TASK_ID} # only works for autosomes
readonly in_vcf="${in_dir}/ukb_wes_200k_filtered_chr${chr}.vcf.bgz"

# output paths
readonly out_prefix="ukb_wes_200k_ref_panel_5k_chr${chr}"
readonly out_vcf="${vcf_dir}/${out_prefix}.vcf.bgz"
readonly out_plink="${plink_dir}/${out_prefix}"

if [ ! -f ${out_vcf} ]; then
  mkdir -p ${vcf_dir} ${plink_dir}
  module load BCFtools/1.10.2-GCC-8.3.0

  bcftools view \
    ${in_vcf} \
    --samples-file ${samples} \
    --threads $(( ${NSLOTS}-1 )) \
    > ${out_vcf}

  bcftools index \
    ${out_vcf} \
    --tbi \
    threads $(( ${NSLOTS}-1 ))
    
fi


