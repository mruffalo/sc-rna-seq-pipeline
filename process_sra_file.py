#!/usr/bin/env python3
import argparse
from pathlib import Path
from pprint import pprint

from data_path_utils import append_to_filename
import pandas as pd

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
    print('Summary data:')
    pprint(summary)

    rpkm_path = append_to_filename(args.sra_path.with_suffix('.hdf5'), '_rpkm')
    print('Saving RPKM to', rpkm_path)
    with pd.HDFStore(rpkm_path) as store:
        d = pd.DataFrame({args.sra_path.stem: rpkm})
        store['rpkm'] = d
