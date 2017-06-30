#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd

from build_tree import intervals_by_gene, gene_length, trees
from utils import DATA_PATH, append_to_filename, ensure_dir, replace_extension

RPKM_DATA_PATH = DATA_PATH / 'rpkm'
ensure_dir(RPKM_DATA_PATH)

def main(sam_path: Path):
    base_csv_path = replace_extension(sam_path, 'csv')

    # Read the SAM file of an alignment.
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
    rpkm_csv_path = append_to_filename(base_csv_path, '_rpkm')
    print('Saving RPKM values to', rpkm_csv_path)
    rpkm.to_csv(rpkm_csv_path)

    gene_count = (read_counts > 0).sum()

    print('Reads (+1000bp):', reads_mapped_to_genes)
    print('Genes (+1000bp):', gene_count)
    print('Total:', reads_total)

    summary_data = pd.Series(
        {
            'read_count': reads_total,
            'reads_aligned': reads_aligned,
            'mapped_to_genes': reads_mapped_to_genes,
            'genes_with_reads': gene_count,
        }
    )
    summary_csv_path = append_to_filename(base_csv_path, '_summary')
    print('Saving summary data to', summary_csv_path)
    summary_data.to_csv(summary_csv_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('sam_path', type=Path, help='Path to SAM file')
    args = parser.parse_args()

    main(args.sam_path)
