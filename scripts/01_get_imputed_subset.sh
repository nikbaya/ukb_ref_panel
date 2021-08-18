#!/usr/bin/env bash
#
# Creates reference panel by extracting subset of samples from UKB imputed V3
#
# Author: Nik Baya (2021-08-18)
#
#$ -N get_imp_subset
#$ -wd /well/lindgren/UKBIOBANK/nbaya/resources/ref/ukb_wes_200k/ukb_ref_panel
#$ -o logs/get_imp_subset.log
#$ -e logs/get_imp_subset.log
#$ -P lindgren.prjc
#$ -pe shmem 1
#$ -q short.qe
#$ -V # necessary for `qsub` command to be available

set -o errexit
set -o nounset
module purge
source utils/bash_utils.sh

# directories
readonly spark_dir="logs/spark"
readonly samples_dir="data/samples"

# options
readonly chr=${SGE_TASK_ID} # only works for autosomes
readonly num_samples=5000
readonly ancestry="eur" # ancestry to subset to before sampling. Options: "eur" (genetically-confirmed Europeans), "all" (all ancestries)

# input paths
readonly input_type="bgen"
readonly input_path_template="data/imputed_v3_bgen/ukb_imp_chr<CHROM>_v3.${input_type}"
readonly input_path=$( echo ${input_path_template} | sed 's/<CHROM>/22/g') # use chr22 because it's the smallest and fastest for imputed data
readonly bgen_samples="/well/lindgren/UKBIOBANK/DATA/SAMPLE_FAM/ukb11867_imp_chr1_v3_s487395.sample" 

# output paths
readonly output_prefix="ukb_imp_v3_${ancestry}_ref_panel_$(( num_samples / 1000 ))k"
readonly output_path="${samples_dir}/${output_prefix}.tsv"

# hail script
readonly hail_script="utils/get_subset_hail.py"

if [ ! -f ${output_path} ]; then
  SECONDS=0
  mkdir -p ${samples_dir}
  set_up_hail

  python3 ${hail_script} \
    --input_path ${input_path} \
    --input_type "bgen" \
    --bgen_samples ${bgen_samples} \
    --choose_samples \
    --num_samples ${num_samples} \
    --ancestry ${ancestry} \
    --output_path ${output_path} \
    --output_type "tsv"

  print_update "Finished writing ${output_path}" "${SECONDS}"
fi

# submit jobs for extracting chosen sample subset from each per-chromosome file
readonly write_subset_script="utils/_write_subset.sh"

readonly output_type="plink"
readonly output_path_template="data/${output_type}/${output_prefix}_chr<CHROM>" # if output_type="plink", don't include a file suffix
readonly individuals_to_keep=${output_path}

qsub -N "_write_imp_subset" \
  -t 21-22 \
  -pe shmem 1 \
  -o ${SGE_STDOUT_PATH} \
  -e ${SGE_STDERR_PATH} \
  ${write_subset_script} \
  ${input_path_template} \
  ${input_type} \
  ${individuals_to_keep} \
  ${output_path_template} \
  ${output_type} \
  ${bgen_samples}
