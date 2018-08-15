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
from pprint import pprint
import sys

import pandas as pd

from alignment import align_fastq_compute_expr
from utils import add_common_command_line_arguments

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument(
        'fastq_file',
        type=Path,
        nargs='+',
        help='One or two paths to FASTQ files'
    )
    add_common_command_line_arguments(p)
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
    print('Alignment metadata:')
    pprint(alignment_metadata)

    if args.output_file is None:
        args.output_file = args.fastq_file[0].with_suffix('.hdf5')

    print('Saving expression and alignment metadata to', args.output_file)
    with pd.HDFStore(args.output_file) as store:
        store['rpkm'] = pd.DataFrame(rpkm)
        store['alignment_metadata'] = pd.DataFrame(alignment_metadata)
