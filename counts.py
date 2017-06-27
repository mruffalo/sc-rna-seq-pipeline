#!/usr/bin/env python3
import os
import argparse


def main(geo_id):
    # Generate the RPKM matrix of cell profiles from the same project.
    output = open('/home/zhilinh/' + geo_id + '.csv', 'wb')
    genes = set()
    cells = {}
    for root, dirs, files in os.walk("/home/zhilinh/output/"):
        for name in files:
            if geo_id in name:
                cells[name] = {}
                lines = open('/home/zhilinh/output/' + name, 'r').readlines()
                for line in lines:
                    # Get all expressed genes in the experiments. Should give all genes.
                    line = line.strip().split('\t')
                    genes.add(line[0])

    # Walk through each cell and get the RPKM of genes.
    genes = list(genes)
    for cell in cells.keys():
        lines = open('/home/zhilinh/output/' + cell, 'r').readlines()
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
