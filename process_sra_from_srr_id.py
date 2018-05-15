#!/usr/bin/env python3
"""
This file performs a single run of the following:

1. Download a .sra file using the NCBI SRA prefetch program
2. Convert to FASTQ format
3. Align reads with HISAT2
4. Map reads to genes
5. Convert counts to RPKM
6. Save RPKM and summary data to the `data` directory

It is intended that many runs of this file (with different FTP URLs)
will be submitted to the cluster in parallel.
"""

from argparse import ArgumentParser
import os
from pathlib import Path
from subprocess import check_call

from alignment import process_sra_file
from ncbi_sra_toolkit_config import get_ncbi_download_path

def download_sra(srr_id: str) -> Path:
    command = ['prefetch', srr_id]
    print('Running', ' '.join(command))
    check_call(command)

    ncbi_download_path = get_ncbi_download_path()
    downloaded_path = ncbi_download_path / 'sra' / f'{srr_id}.sra'
    if not downloaded_path.is_file():
        raise EnvironmentError(f'Download of {srr_id} seemed to succeed, but no file {downloaded_path} exists')

    return downloaded_path

def process_sra_from_srr_id(srr_id: str, subprocesses: int):
    local_path = download_sra(srr_id)
    try:
        process_sra_file(local_path, subprocesses)
    finally:
        local_path.unlink()

def get_srr_id(srr_list_file: Path) -> str:
    """
    Reads `srr_list_file` and returns the item selected by the SLURM_ARRAY_TASK_ID
    environment variable. This is a separate function so the full list of URLs can
    be garbage collected afterward -- I don't think this will use much memory at all,
    but may as well not keep more things alive in memory than we need to.
    """
    with open(srr_list_file) as f:
        srr_ids = [line.strip() for line in f]

    file_index = int(os.environ['SLURM_ARRAY_TASK_ID'])

    return srr_ids[file_index]

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('srr_list_file', type=Path)
    p.add_argument(
        '-s',
        '--subprocesses',
        help='Number of subprocesses for alignment in each run of HISAT2',
        type=int,
        default=1
    )
    args = p.parse_args()

    srr_id = get_srr_id(args.srr_list_file)
    process_sra_from_srr_id(srr_id, args.subprocesses)
