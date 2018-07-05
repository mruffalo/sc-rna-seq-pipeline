#!/usr/bin/env python3
import argparse
from pathlib import Path

from alignment import process_sra_file

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('sra_path', type=Path, help='Path to SRA file')
    parser.add_argument(
        '-s',
        '--subprocesses',
        type=int,
        default=1,
        help='Number of subprocesses used in HISAT2'
    )
    args = parser.parse_args()

    rpkm, summary = process_sra_file(args.sam_path, args.subprocesses)
