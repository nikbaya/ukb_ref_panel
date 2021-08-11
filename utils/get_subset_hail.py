#!/usr/bin/env python3
"""
Export subset of individuals in post-QC VCFs.

Created: 2021/08/10
@author: nbaya
"""

import argparse
import hail as hl
import numpy as np


def read_input(input_type, input_path):
    assert input_type in {"mt"}
    if input_type == "mt":
        mt = hl.read_matrix_table(
            path=input_path
        )
    elif input_type == "vcf":
        pass
        # mt = hl.import_vcf(
        #     path = input_path,
        #     force_bgz = True
        # )
    elif input_type == "plink":
        pass
        # mt = hl.import_plink(
        #     **{suffix: f'{input_path}.{suffix}' for suffix in {"bed", "bim", "fam"}}
        # )
    return mt


def filter_to_ancestry(mt, ancestry="eur"):
    assert ancestry in {"all", "eur"}
    if ancestry == "eur":
        ht = hl.import_table(
            "/well/lindgren/UKBIOBANK/laura/k_means_clustering_pcs/ukbb_genetically_european_k4_4PCs_self_rep_Nov2020.txt", key="eid")
        mt = mt.filter_cols(ht[mt.s].genetically_european == "1")
    return mt


def get_subset(mt, num_samples):
    indices = np.random.choice(
        a=mt.count_cols(),
        size=num_samples,
        replace=False
    )
    indices.sort()  # not important, but will keep samples in the same order
    mt = mt.choose_cols(indices.tolist())
    return mt


def write_output(mt, output_type, output_path):
    assert output_type in {"mt", "plink"}
    if output_type == "mt":
        assert ".mt" in output_path
        mt.write(output_path)
    elif output_type == "plink":
        assert "." not in output_path.split("/")[-1]
        hl.export_plink(
            dataset=mt,
            output=output_path,
            fam_id=mt.s,
            ind_id=mt.s
        )


def main(args):
    hl.init(log="logs/get_subset_hail.log")

    mt = read_input(
        input_type=args.input_type,
        input_path=args.input_path
    )

    mt = filter_to_ancestry(
        mt=mt,
        ancestry=args.ancestry
    )

    mt = get_subset(
        mt=mt,
        num_samples=int(args.num_samples)
    )

    write_output(
        mt=mt,
        output_type=args.output_type,
        output_path=args.output_path
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", default=None,
                        help="Path to VCF/MatrixTable/PLINK dataset to subset")
    parser.add_argument("--input_type", default=None,
                        help="Type of input dataset. Options: 'vcf', 'mt', 'plink'")
    parser.add_argument("--num_samples", help="Number of samples to subset")
    parser.add_argument("--ancestry", default="eur",
                        help="Ancestry subset to use. Options: eur (genetically-confirmed Europeans), all (all individuals, no ancestry filter)")
    parser.add_argument("--output_path", help="Path to output file")
    parser.add_argument(
        "--output_type", help="Type of output dataset (options: 'vcf', 'mt', 'plink')")
    args = parser.parse_args()

    main(args)
