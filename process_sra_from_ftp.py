#!/usr/bin/env python3
"""
This file performs a single run of the following:

1. Download a .sra file from a FTP url given as a command-line argument
2. Convert to FASTQ format
3. Align reads with HISAT2
4. Map reads to genes
5. Convert counts to RPKM
6. Save RPKM and summary data to the `data` directory

It is intended that many runs of this file (with different FTP URLs)
will be submitted to the cluster in parallel.
"""

from argparse import ArgumentParser
from ftplib import FTP
import os
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

from automated_bash import process_sra_file
from utils import SCRATCH_PATH, USERNAME, ensure_dir

FTP_HOST_DEFAULT = 'ftp.ncbi.nlm.nih.gov'

def create_ftp_session(ftp_host=FTP_HOST_DEFAULT) -> FTP:
    ftp = FTP(ftp_host)
    ftp.set_pasv(True)
    # anonymous
    ftp.login()
    return ftp

def download_ftp_url(ftp_url: str) -> Path:
    url_pieces = urlparse(ftp_url)
    ftp_path = PurePosixPath(url_pieces.path)
    srr_name = ftp_path.stem
    srr_dir = ensure_dir(SCRATCH_PATH / USERNAME / srr_name)
    local_path = srr_dir / ftp_path.name

    with create_ftp_session(url_pieces.netloc) as ftp:
        # pathlib is too convenient not to use here, but FTP paths always have
        # POSIX path semantics, so use PurePosixPath to not get Windows-specific
        # things like path separators if running on that platform
        ftp.cwd(str(ftp_path.parent))

        with open(local_path, 'wb') as f:
            print(f'Downloading {ftp_url} to {local_path}')
            ftp.retrbinary(f'RETR {ftp_path.name}', f.write)

    return local_path

def process_sra_from_ftp(ftp_url: str, subprocesses: int):
    local_path = download_ftp_url(ftp_url)
    process_sra_file(local_path, subprocesses)

def get_ftp_url(ftp_list_file: Path) -> str:
    """
    Reads `ftp_list_file` and returns the item selected by the SLURM_ARRAY_TASK_ID
    environment variable. This is a separate function so the full list of URLs can
    be garbage collected afterward -- I don't think this will use much memory at all,
    but may as well not keep more things alive in memory than we need to.
    """
    with open(ftp_list_file) as f:
        ftp_urls = [line.strip() for line in f]

    file_index = int(os.environ['SLURM_ARRAY_TASK_ID'])

    return ftp_urls[file_index]

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('ftp_list_file', type=Path)
    p.add_argument(
        '-s',
        '--subprocesses',
        help='Number of subprocesses for alignment in each run of HISAT2',
        type=int,
        default=1
    )
    args = p.parse_args()

    ftp_url = get_ftp_url(args.ftp_list_file)
    process_sra_from_ftp(ftp_url, args.subprocesses)
