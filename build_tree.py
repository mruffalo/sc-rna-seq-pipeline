#!/usr/bin/env python3
from argparse import ArgumentParser
from collections import defaultdict
import csv
from pathlib import Path
import pickle
import re

from data_path_utils import create_data_path
from intervaltree import IntervalTree
import pandas as pd

DEFAULT_CCDS_PATH = Path('~/data/ccds/current_mouse/CCDS.current.txt').expanduser()

def main(ccds_path: Path):
    gene_set = set()
    interval_separator = re.compile(r'\s*,\s*')

    print('Reading CCDS data from', ccds_path)
    with open(ccds_path) as f:
        # Maps gene IDs to sets of intervals in string format, e.g. '129415220-129415360'.
        # Used to consolidate isoforms that are each listed separately.
        intervals_by_gene = defaultdict(set)

        r = csv.DictReader(f, delimiter='\t')
        # First line starts with "#", so the field name for chromosome is actually "#chromosome"
        for line in r:
            chrom_name = 'chr{}'.format(line['#chromosome'])
            intervals = interval_separator.split(line['cds_locations'].strip('[]'))
            gene_id = line['gene_id']
            gene_set.add(gene_id)
            for interval in intervals:
                intervals_by_gene[gene_id].add((chrom_name, interval))

    trees = defaultdict(IntervalTree)
    gene_length = pd.Series(0.0, index=sorted(intervals_by_gene))
    print('Read data for', len(intervals_by_gene), 'genes')

    for gene, interval_data in intervals_by_gene.items():
        chrom_dict = {}
        for chrom, interval_string in interval_data:
            # Skip invalid interval lists
            if interval_string == '-':
                continue
            if chrom not in chrom_dict:
                chrom_dict[chrom] = [float('inf'), 0]

            interval_pieces = interval_string.split('-')
            start = int(interval_pieces[0])
            end = int(interval_pieces[1])

            chrom_dict[chrom][0] = min(chrom_dict[chrom][0], start)
            chrom_dict[chrom][1] = max(chrom_dict[chrom][1], end)

        for chrom in chrom_dict:
            trees[chrom][chrom_dict[chrom][0]:chrom_dict[chrom][1]] = gene
            # Kilobases, so divide by 1000
            gene_length.loc[gene] = (chrom_dict[chrom][1] - chrom_dict[chrom][0]) / 1000

    data_path = create_data_path('build_tree')
    pickle_file = data_path / 'trees.pickle'
    print('Saving interval trees to', pickle_file)

    data_to_save = {
        'trees': trees,
        'gene_length': gene_length,
        'intervals_by_gene': intervals_by_gene,
    }

    with open(pickle_file, 'wb') as f:
        pickle.dump(data_to_save, f, protocol=pickle.HIGHEST_PROTOCOL)

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('ccds_file', type=Path, default=DEFAULT_CCDS_PATH)
    args = p.parse_args()

    main(args.ccds_file)
