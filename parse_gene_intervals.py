#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd

from build_tree import intervals_by_gene, gene_length, trees
from utils import DATA_PATH, ensure_dir, replace_extension

RPKM_DATA_PATH = DATA_PATH / 'rpkm'
ensure_dir(RPKM_DATA_PATH)

def main(sam_path: Path):
    # Read the SAM file of an alignment.
    read_counts = pd.Series(0, index=sorted(intervals_by_gene))
    reads_mapped_to_genes = 0
    reads_aligned = 0
    reads_total = 0

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

            # Reads should never map to multiple genes, but it's still better to be
            # safe with this and not count reads multiple times if this happens
            if gene_ids:
                reads_mapped_to_genes += 1

            for gene_id in gene_ids:
                read_counts.loc[gene_id.data] += 1

    rpkm_csv_path = replace_extension(sam_path, 'csv')

    # Count the reads mapped to the genome and calculate the RPKM of each gene.
    with open(rpkm_csv_path, 'w') as output:
        gene_count = 0

        # The unit of total reads count would be million.
        reads_total /= 1000000

        for gene_id, interval_data in intervals_by_gene.items():
            reads_num = read_counts.loc[gene_id]
            length = gene_length.loc[gene_id]
            if reads_num != 0:
                # Calculate the RPKM of genes in that cell.
                rpkm = reads_num / (reads_total * length)
                row = gene_id + '\t' + str(rpkm) + '\n'
                output.write(row.encode())
                gene_count += 1
        # combined = pd.concat([gene_counts, build_tree.gene_length], axis=1)
        # combined.to_csv('/home/zhilinh/output')
        print('Reads (+1000bp): ' + str(reads_mapped_to_genes))
        print("Genes (+1000bp): " + str(gene_count))
        print('Total: ' + str(reads_total))
        print()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'geoid', default='', help='GEO_id.')
    args = parser.parse_args()
    main(args.geoid)
