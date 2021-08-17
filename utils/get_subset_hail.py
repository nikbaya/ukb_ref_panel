#!/usr/bin/env python3
"""
Export subset of individuals in post-QC VCFs.

@author: Nikolas Baya (2021/08/10)
"""

import argparse
import hail as hl
import numpy as np


def read_input(input_type, input_path, bgen_samples=None):
    assert input_type in {"mt", "bgen"}
    if input_type == "mt":
        mt = hl.read_matrix_table(
            path=input_path
        )
    elif input_type == "bgen":
        if not hl.hadoop_is_dir(input_path+".idx2"):
            hl.index_bgen(input_path)
        mt = hl.import_bgen(
            path=input_path,
            entry_fields=["GT"],  # Can also read in GP and dosage
            sample_file=bgen_samples
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
            paths="/well/lindgren/UKBIOBANK/laura/k_means_clustering_pcs/ukbb_genetically_european_k4_4PCs_self_rep_Nov2020.txt",
            key="eid"
        )
        mt = mt.filter_cols(ht[mt.s].genetically_european == "1")
    return mt


def filter_to_unrelated(mt, kinship_coef_cutoff=2**(-4.5)):
    """
    Identifies related sample pairs by filter to pairs with kinship coefficient > `kinship_coef_cutoff`.
    Default cutoff of 2^-4.5 corresponds to the midpoint in log space between 3rd and 4th degree relatives.
    Thus, individuals who are 1st, 2nd, 3rd degree relatives would be considered relatives.

    Uses Hail's `maximal_independent_set` function to find the fewest number of individuals to remove 
    in order to have an unrelated set of samples.
    """
    assert (kinship_coef_cutoff >= 0) & (kinship_coef_cutoff <= 0.5), "kinship_coef_cutoff must be in the interval [0, 0.5]"
    kinship = hl.import_table(
        paths="/well/lindgren/UKBIOBANK/DATA/QC/ukb1186_rel_s488366.dat",
        types={"ID1": "str", "ID2": "str", "HetHet": "float",
               "IBS0": "float", "Kinship": "float"},
        delimiter='\s+'
    )
    both_in_mt = hl.is_defined(mt.cols()[kinship.ID1]) & hl.is_defined(mt.cols()[kinship.ID2]) # both individuals in pair are present in mt
    related_pair = kinship.Kinship > kinship_coef_cutoff # boolean indicating if individuals in pair are related
    related = kinship.filter(both_in_mt & related_pair)
    related_to_remove = hl.maximal_independent_set(related.ID1, related.ID2, keep=False)
    mt = mt.filter_cols(hl.is_defined(related_to_remove[mt.s]), keep=False)
    return mt


def get_subset(mt, num_samples):
    indices = np.random.choice(
        a=mt.count_cols(),
        size=num_samples,
        replace=False
    )
    indices.sort()  # not required, but will keep samples in the same order
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
            ind_id=mt.s,
            varid=mt.rsid
        )


def main(args):
    hl.init(log="logs/get_subset_hail.log")

    mt = read_input(
        input_type=args.input_type,
        input_path=args.input_path,
        bgen_samples=args.bgen_samples
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
    parser.add_argument("--bgen_samples", default=None,
                        help="BGEN sample file")
    parser.add_argument("--num_samples", help="Number of samples to subset")
    parser.add_argument("--ancestry", default="eur",
                        help="Ancestry subset to use. Options: eur (genetically-confirmed Europeans), all (all individuals, no ancestry filter)")

    parser.add_argument("--output_path", help="Path to output file")
    parser.add_argument(
        "--output_type", help="Type of output dataset (options: 'vcf', 'mt', 'plink')")
    args = parser.parse_args()

    main(args)
