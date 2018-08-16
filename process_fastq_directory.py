#!/usr/bin/env python3
"""
This file performs multiple runs of the following:

1. Align reads with HISAT2, in single- or paired-end as appropriate
2. Map reads to genes
3. Convert counts to RPKM
4. Save RPKM and summary data
"""
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path
from pprint import pprint
from typing import Dict, List

import pandas as pd

from alignment import align_fastq_compute_expr
from utils import add_common_command_line_arguments, normalize_whitespace

FASTQ_PATTERN = '*.fastq'

def group_fastq_files(directory: Path) -> List[List[Path]]:
    """
    :param directory:
    :return:
    """
    fastq_groups: Dict[str, List[Path]] = defaultdict(list)

    for fastq_path in directory.glob(FASTQ_PATTERN):
        filename_pieces = fastq_path.stem.rsplit('_', 1)
        if len(filename_pieces) == 2 and filename_pieces[1] in {'1', '2'}:
            key = filename_pieces[0]
        else:
            key = fastq_path.stem
        fastq_groups[key].append(fastq_path)

    return list(fastq_groups.values())

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument(
        'fastq_directory',
        type=Path,
        help=normalize_whitespace(
            """
            Directory containing FASTQ files with a specific filename pattern.
            If two files have the same prefix, but end in '_1.fastq' and '_2.fastq',
            these two files will be aligned in paired-end mode. FASTQ files will
            be aligned in single-end mode if 1) they do not match the pattern
            "*_{1,2}.fastq", or 2) one of the expected "paired" files is missing.
            """
        )
    )
    add_common_command_line_arguments(p)
    args = p.parse_args()

    all_rpkm = []
    all_alignment_metadata = []

    for fastq_group in group_fastq_files(args.fastq_directory):
        rpkm, alignment_metadata = align_fastq_compute_expr(
            fastq_paths=fastq_group,
            subprocesses=args.subprocesses,
            hisat2_options=args.hisat2_options,
            reference_path=args.reference_path
        )
        all_rpkm.append(rpkm)
        all_alignment_metadata.append(alignment_metadata)

        print(f'Alignment metadata for {fastq_group}:')
        pprint(alignment_metadata)

    rpkm = pd.DataFrame(all_rpkm)
    alignment_metadata = pd.DataFrame(all_alignment_metadata)

    if args.output_file is None:
        args.output_file = args.fastq_directory / 'expr.hdf5'

    print('Saving expression and alignment metadata to', args.output_file)
    with pd.HDFStore(args.output_file) as store:
        store['rpkm'] = pd.DataFrame(rpkm)
        store['alignment_metadata'] = pd.DataFrame(alignment_metadata)
