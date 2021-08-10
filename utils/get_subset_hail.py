#!/usr/bin/env python3
"""
Export subset of individuals in post-QC VCFs.

Created: 2021/08/10
@author: nbaya
"""

import argparse
import hail as hl

def main(args):
    

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", default=None, help="Path to VCF/MatrixTable/PLINK dataset to subset")
    parser.add_argument("--input_type", default=None, help="Type of input dataset (options: 'vcf', 'mt', 'plink')")
    parser.add_argument("--num_samples", help="Number of samples to subset")
    parser.add_argument("--out_path", help="Path to output file")
    parser.add_argument("--out_type", help="Type of output dataset (options: 'vcf', 'mt', 'plink')")
    args = parser.parse_args()

    main(args)
