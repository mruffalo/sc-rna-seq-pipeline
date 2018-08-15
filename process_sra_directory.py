#!/usr/bin/env python3
"""
This file performs multiple runs of the following:

1. Convert reads from SRA to FASTQ
2. Align reads with HISAT2, in single- or paired-end as appropriate
3. Map reads to genes
4. Convert counts to RPKM
5. Save RPKM and summary data
"""
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path
import sys
from typing import Dict, List

import pandas as pd

from alignment import process_sra_file
from utils import add_common_command_line_arguments

SRA_PATTERN = '*.sra'

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument(
        'sra_directory',
        type=Path,
        help='Directory containing SRA files',
    )
    add_common_command_line_arguments(p)
    args = p.parse_args()

    all_rpkm = []
    all_alignment_metadata = []

    for sra_file in args.sra_directory.iterdir():
        rpkm, summary = process_sra_file(args.sam_path, args.subprocesses)


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
