#!/usr/bin/env bash
#
# Chooses random subset of samples from a VCF
#
# Author: Nik Baya (2021-08-04)
#
# $ -N choose_samples
# $ -wd /well/lindgren/UKBIOBANK/nbaya/resources/ref/ukb_wes_200k/ukb_wes_ref_panel
# $ -o logs/choose_samples.log
# $ -e logs/choose_samples.log
# $ -P lindgren.prjc
# $ -pe shmem 1
# $ -q short.qe

set - o errexit
set - o nounset
module purge

# directories
readonly in_dir = "/well/lindgren/UKBIOBANK/nbaya/wes_200k/ukb_wes_qc/data/filtered"
readonly out_dir = "data"

# input path
readonly n_samples = 5000
# assume all chromosomes have the same individuals
readonly vcf = "${in_dir}/ukb_wes_200k_filtered_chr1.vcf.bgz"

# output paths
readonly samples = "data/ukb_wes_200k_ref_panel_$(( n_samples / 1000 ))k.txt"

if [! -f ${samples}]
then
module load BCFtools/1.10.2-GCC-8.3.0
bcftools query \
    ${vcf} \
    - -list-samples \
    | shuf \
    | head - n ${n_samples} \
    > ${samples}
fi
