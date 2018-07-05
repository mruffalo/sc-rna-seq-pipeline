#!/usr/bin/env python3
import argparse
from pathlib import Path
import pickle
from typing import Tuple

import pandas as pd

from data_path_utils import find_newest_data_path

def map_reads_to_genes(sam_path: Path) -> Tuple[pd.Series, pd.Series]:
    tree_path = find_newest_data_path('build_tree')
    with open(tree_path / 'trees.pickle', 'rb') as f:
        tree_data = pickle.load(f)

        trees = tree_data['trees']
        gene_length = tree_data['gene_length']
        intervals_by_gene = tree_data['intervals_by_gene']

    read_counts = pd.Series(0, index=sorted(intervals_by_gene))
    reads_mapped_to_genes = 0
    reads_aligned = 0
    reads_total = 0

    print('Reading', sam_path)
    with open(sam_path) as f:
        # Read each line of the SAM file.
        for line in f:
            # Filter out the line that is not a read.
            if line.startswith('@'):
                continue

            reads_total += 1

            col = line.split('\t')
            flags = int(col[1])
            if flags & 0x4:
                # unmapped
                continue

            reads_aligned += 1

            chrom = col[2]
            start = int(col[3])
            read_length = len(col[9])
            end = start + read_length

            # Get the gene id at a certain point if there is any.
            gene_ids = trees[chrom][start:end]

            # Reads shouldn't map to multiple genes, but it's still better to be
            # safe with this and not count reads multiple times if this happens
            if gene_ids:
                reads_mapped_to_genes += 1

            for gene_id in gene_ids:
                read_counts.loc[gene_id.data] += 1

    rpkm = (read_counts * 1000000) / (reads_total * gene_length)

    gene_count = (read_counts > 0).sum()

    summary_data = pd.Series(
        {
            'read_count': reads_total,
            'reads_aligned': reads_aligned,
            'mapped_to_genes': reads_mapped_to_genes,
            'genes_with_reads': gene_count,
        }
    )

    return rpkm, summary_data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('sam_path', type=Path, help='Path to SAM file')
    args = parser.parse_args()

    map_reads_to_genes(args.sam_path)
