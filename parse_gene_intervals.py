#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import argparse
from build_tree import intervals_by_gene, gene_length, trees


def main(geo_id):
    # Read the SAM file of an alignment.
    reads_counts = pd.Series(0, index=sorted(intervals_by_gene))
    sam_path = Path('/scratch/lin/' + geo_id + '.sam').expanduser()
    mapped_reads = 0
    total = 0

    with open(sam_path) as f:
        # Read each line of the SAM file.
        r = f.readlines()
        for line in r:
            # Filter out the line that is not a read.
            if line.startswith('@'):
                continue
            else:
                total += 1
                col = line.split('\t')
                chrom = col[2]
                start = int(col[3])

                # Get the gene id at a certain point if there is any.
                gene_ids = trees[chrom][start]
                for gene_id in gene_ids:
                    reads_counts.loc[gene_id.data] += 1
                    mapped_reads += 1

    # If the overall alignment rate is lower than 20%, all reads will be discarded.
    if mapped_reads / total < 0.2:
        print()
        return

    # Count the reads mapped to the genome and calculate the RPKM of each gene.
    output = open('/scratch/lin/' + geo_id + '.csv', 'wb')
    gene_count = 0

    # The unit of total reads count would be million.
    total /= 1000000

    for gene_id, interval_data in intervals_by_gene.items():
        reads_num = reads_counts.loc[gene_id]
        length = gene_length.loc[gene_id]
        if reads_num != 0:
            # Calculate the RPKM of genes in that cell.
            rpkm = reads_num / (total * length)
            row = gene_id + '\t' + str(rpkm) + '\n'
            output.write(row.encode())
            gene_count += 1
    # combined = pd.concat([gene_counts, build_tree.gene_length], axis=1)
    # combined.to_csv('/home/zhilinh/output')
    print(geo_id)
    print('Reads (+1000bp): ' + str(mapped_reads))
    print("Genes (+1000bp): " + str(gene_count))
    print('Total: ' + str(total))
    print()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'geoid', default='', help='GEO_id.')
    args = parser.parse_args()
    main(args.geoid)
