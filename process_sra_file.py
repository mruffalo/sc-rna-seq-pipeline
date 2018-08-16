#!/usr/bin/env python3
"""
This file performs a single run of the following:

1. Convert reads from SRA to FASTQ
2. Align reads with HISAT2, in single- or paired-end as appropriate
3. Map reads to genes
4. Convert counts to RPKM
5. Save RPKM and summary data
"""
import argparse
from pathlib import Path
from pprint import pprint

from data_path_utils import append_to_filename
import pandas as pd

from alignment import process_sra_file
from utils import add_common_command_line_arguments

if __name__ == '__main__':
    p = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('sra_path', type=Path, help='Path to SRA file')
    add_common_command_line_arguments(p)
    args = p.parse_args()

    rpkm, alignment_metadata = process_sra_file(args.sam_path, args.subprocesses)
    print('Alignment metadata:')
    pprint(alignment_metadata)

    rpkm_path = append_to_filename(args.sra_path.with_suffix('.hdf5'), '_rpkm')
    print('Saving RPKM to', rpkm_path)
    with pd.HDFStore(rpkm_path) as store:
        store['rpkm'] = pd.DataFrame({args.sra_path.stem: rpkm})
        store['alignment_metadata'] = pd.DataFrame({args.sra_path.stem: alignment_metadata})
