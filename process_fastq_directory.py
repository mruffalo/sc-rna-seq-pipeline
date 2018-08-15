#!/usr/bin/env python3
"""
This file performs a multiple runs of the following:

1. Align reads with HISAT2, in single- or paired-end as appropriate
2. Map reads to genes
3. Convert counts to RPKM
4. Save RPKM and summary data
"""
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path
import sys
from typing import Dict, List

import pandas as pd

from alignment import align_fastq_compute_expr
from utils import normalize_whitespace

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
    p.add_argument(
        '-s',
        '--subprocesses',
        help='Number of subprocesses for alignment in each run of HISAT2',
        type=int,
        default=1,
    )
    p.add_argument('--reference-path', type=Path)
    p.add_argument(
        '--output-file',
        type=Path,
        help=normalize_whitespace(
            """
            Output file for gene expression and alignment metadata, saved in HDF5
            format (.hdf5 or .h5 file extension recommended). If omitted, data will
            be saved to 'expr.hdf5' inside the directory containing the FASTQ files. 
            """
        ),
    )
    p.add_argument(
        '--hisat2-options',
        help=normalize_whitespace(
            """
            Extra options to pass to the HISAT2 aligner, passed as a single string.
            If passing multiple options, you will likely need to enclose the HISAT2
            options in quotes, e.g. --hisat2-options="--mp 4,2 --phred64"
            """
        ),
    )
    args = p.parse_args()

    if len(args.fastq_file) not in {1, 2}:
        message = 'One or two FASTQ files must be specified, for single- or paired-end alignment.'
        sys.exit(message)

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

    rpkm = pd.DataFrame(all_rpkm)
    alignment_metadata = pd.DataFrame(all_alignment_metadata)

    if args.output_file is None:
        args.output_file = args.fastq_file[0].with_suffix('.hdf5')

    print('Saving expression and alignment metadata to', args.output_file)
    with pd.HDFStore(args.output_file) as store:
        store['rpkm'] = pd.DataFrame(rpkm)
        store['alignment_metadata'] = pd.DataFrame(alignment_metadata)
