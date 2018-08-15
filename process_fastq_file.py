#!/usr/bin/env python3
"""
This file performs a single run of the following:

1. Align reads with HISAT2, in single- or paired-end as appropriate
2. Map reads to genes
3. Convert counts to RPKM
4. Save RPKM and summary data
"""
from argparse import ArgumentParser
from pathlib import Path
import sys

import pandas as pd

from alignment import align_fastq_compute_expr
from utils import normalize_whitespace

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('fastq_file', type=Path, nargs='+')
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
            be saved to the same location as the first FASTQ file, but with file
            extension .hdf5 instead of .fastq.
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

    rpkm, alignment_metadata = align_fastq_compute_expr(
        fastq_paths=args.fastq_file,
        subprocesses=args.subprocesses,
        hisat2_options=args.hisat2_options,
        reference_path=args.reference_path
    )

    if args.output_file is None:
        args.output_file = args.fastq_file[0].with_suffix('.hdf5')

    print('Saving expression and alignment metadata to', args.output_file)
    with pd.HDFStore(args.output_file) as store:
        store['rpkm'] = pd.DataFrame(rpkm)
        store['alignment_metadata'] = pd.DataFrame(alignment_metadata)
