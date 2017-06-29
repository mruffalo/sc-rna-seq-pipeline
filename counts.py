#!/usr/bin/env python3
import os
import csv
import argparse
from pathlib import Path


def main(geo_id):
    ccds_current_path = Path('/scratch/lin/data/CCDS.current.txt').expanduser()
    gene_set = set()
    with open(ccds_current_path) as f:
        r = csv.DictReader(f, delimiter='\t')
        for line in r:
            gene_id = line['gene_id']
            gene_set.add(gene_id)

    # Generate the RPKM matrix of cell profiles from the same project.
    output = open('/scratch/lin/' + geo_id + '.csv', 'wb')
    cells = {}
    for root, dirs, files in os.walk("/scratch/lin/output/"):
        for name in files:
            if geo_id in name:
                cells[name] = {}

    # Walk through each cell and get the RPKM of genes.
    genes = list(gene_set)
    line = 'GEO_ID\t'
    for gene in genes:
        line += gene + '\t'
    line += '\n'
    output.write(line.encode())
    for cell in cells.keys():
        lines = open('/scratch/lin/output/' + cell, 'r').readlines()
        for line in lines:
            line = line.strip().split('\t')
            cells[cell][line[0]] = line[1]

    # Print the RPKM matrix for each gene and cell.
    for cell in cells.keys():
        line = cell + '\t'
        for gene in genes:
            if gene in cells[cell]:
                line += cells[cell][gene] + '\t'
            else:
                line += '0\t'
        line += '\n'
        output.write(line.encode())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'geoid', default='', help='GEO_id.')
    args = parser.parse_args()
    main(args.geoid)
