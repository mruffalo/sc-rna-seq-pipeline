#!/usr/bin/env python3
"""
This file performs a single run of the following:

1. Align reads with HISAT2, in single- or paired-end as appropriate
2. Map reads to genes
3. Convert counts to RPKM
4. Save RPKM and summary data to the `data` directory
"""

from argparse import ArgumentParser
from pathlib import Path

from alignment import align_fastq_compute_expr
if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('fastq_file', type=Path, nargs='+')
    p.add_argument(
        '-s',
        '--subprocesses',
        help='Number of subprocesses for alignment in each run of HISAT2',
        type=int,
        default=1
    )
    args = p.parse_args()

    if len(args.fastq_file) not in {1, 2}:
        raise ValueError('One or two FASTQ files must be specified (single- or paired-end).')

    align_fastq_compute_expr(args.fastq_file, args.subprocesses)
