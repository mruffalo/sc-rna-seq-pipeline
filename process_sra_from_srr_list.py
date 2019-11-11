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
import os
from pathlib import Path
from subprocess import check_call
from typing import Optional

from alignment import process_sra_file
from ncbi_sra_toolkit_config import get_ncbi_download_path
from paths import OUTPUT_PATH
from utils import add_common_command_line_arguments

def download_sra(srr_id: str) -> Path:
    command = [
        'prefetch',
        srr_id,
    ]
    print('Running', ' '.join(command))
    check_call(command)

    ncbi_download_path = get_ncbi_download_path()
    downloaded_path = ncbi_download_path / 'sra' / f'{srr_id}.sra'
    if not downloaded_path.is_file():
        raise EnvironmentError(f'NCBI download of {srr_id} reported success, but no file {downloaded_path} exists')

    return downloaded_path

def process_sra_from_srr_id(
        srr_id: str,
        subprocesses: int,
        hisat2_options: Optional[str]=None,
        reference_path: Optional[Path]=None,
):
    local_path = download_sra(srr_id)
    try:
        rpkm, summary = process_sra_file(
            sra_path=local_path,
            subprocesses=subprocesses,
            hisat2_options=hisat2_options,
            reference_path=reference_path,
        )
    finally:
        local_path.unlink()
    return rpkm, summary

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
    add_common_command_line_arguments(p)
    args = p.parse_args()

    srr_id = get_srr_id(args.srr_list_file)
    rpkm, summary = process_sra_from_srr_id(
        srr_id=srr_id,
        subprocesses=args.subprocesses,
        hisat2_options=args.hisat2_options,
        reference_path=args.reference_path,
    )

    filename = f'{srr_id}.csv'

    rpkm_dir = OUTPUT_PATH / 'rpkm'
    summary_dir = OUTPUT_PATH / 'summary'

    rpkm.to_csv(rpkm_dir / filename)
    summary.to_csv(summary_dir / filename)
