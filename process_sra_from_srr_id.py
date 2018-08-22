#!/usr/bin/env python3
"""
This file performs a single run of the following:

1. Download a .sra file using the NCBI SRA prefetch program
2. Convert to FASTQ format
3. Align reads with HISAT2
4. Map reads to genes
5. Convert counts to RPKM
6. Save RPKM and summary data to the `data` directory
"""

from argparse import ArgumentParser
from pathlib import Path
from subprocess import check_call
from typing import Optional

from alignment import process_sra_file
from ncbi_sra_toolkit_config import get_ncbi_download_path
from utils import add_common_command_line_arguments

def download_sra(srr_id: str) -> Path:
    command = ['prefetch', srr_id]
    print('Running', ' '.join(command))
    check_call(command)

    ncbi_download_path = get_ncbi_download_path()
    downloaded_path = ncbi_download_path / 'sra' / f'{srr_id}.sra'
    if not downloaded_path.is_file():
        raise EnvironmentError(f'Download of {srr_id} seemed to succeed, but no file {downloaded_path} exists')

    return downloaded_path

def process_sra_from_srr_id(
        srr_id: str,
        subprocesses: int,
        hisat2_options: Optional[str]=None,
        reference_path: Optional[Path]=None,
):
    local_path = download_sra(srr_id)
    try:
        process_sra_file(
            sra_path=local_path,
            subprocesses=subprocesses,
            hisat2_options=hisat2_options,
            reference_path=reference_path,
        )
    finally:
        local_path.unlink()

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('srr_id')
    add_common_command_line_arguments(p)
    args = p.parse_args()

    process_sra_from_srr_id(
        srr_id=args.srr_id,
        subprocesses=args.subprocesses,
        hisat2_options=args.hisat2_options,
        reference_path=args.reference_path,
    )
