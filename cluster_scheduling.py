#!/usr/bin/env python3
from argparse import ArgumentParser
from subprocess import check_call
from pathlib import Path
import shlex
from typing import Dict

from data_path_utils import create_slurm_path

from utils import SCRATCH_PATH

script_template = """
#!/bin/bash

#SBATCH -p {pool}
#SBATCH --mem=8192
#SBATCH --mincpus={subprocesses}

python3 process_sra_from_ftp.py -s {subprocesses} {ftp_list_file}
""".strip()

SBATCH_COMMAND_TEMPLATE = [
    'sbatch',
    '--array={array_index_spec}',
    '{script_filename}',
]

SCRIPT_FILENAME = 'bulk_download.sh'

NCBI_CONFIG_FILE_PATH = Path('~/.ncbi/user-settings.mkfg').expanduser()

def read_ncbi_config() -> Dict[str, str]:
    print('Reading NCBI download configuration from', NCBI_CONFIG_FILE_PATH)
    config_data = {}
    with open(NCBI_CONFIG_FILE_PATH) as f:
        for line in f:
            l = line.strip()
            if l.startswith('#') or not l:
                continue
            pieces = l.split('=')
            config_data[pieces[0].strip()] = pieces[1].strip().strip('"')
    return config_data

NCBI_DOWNLOAD_PATH_KEY = '/repository/user/default-path'
# Download must be a subdirectory of this
REQUIRED_DOWNLOAD_DIR = SCRATCH_PATH

def check_ncbi_prefetch_location():
    try:
        ncbi_config = read_ncbi_config()
    except FileNotFoundError as e:
        message = (
            f"No NCBI configuration found at {NCBI_CONFIG_FILE_PATH}. Not downloading to user's home directory. "
            "(Consider configuring this download location by running `vdb-config -i` from the SRA toolkit.)"
        )
        raise EnvironmentError(message) from e
    if NCBI_DOWNLOAD_PATH_KEY not in ncbi_config:
        message = f"No download path specified in {NCBI_CONFIG_FILE_PATH}. Not downloading to user's home directory."
        raise EnvironmentError(message)
    download_path = Path(ncbi_config[NCBI_DOWNLOAD_PATH_KEY])
    if REQUIRED_DOWNLOAD_DIR not in download_path.parents:
        raise EnvironmentError(f'Not downloading to {download_path} (not a subdirectory of {REQUIRED_DOWNLOAD_DIR}).')

def queue_jobs(ftp_list_file: Path, array_index_spec: str, pool: str, subprocesses: int):
    slurm_path = create_slurm_path('cluster_scheduling')

    script_file = slurm_path / SCRIPT_FILENAME
    print('Saving script to', script_file)
    with open(script_file, 'w') as f:
        script_content = script_template.format(
            ftp_list_file=ftp_list_file,
            pool=pool,
            subprocesses=subprocesses,
        )
        print(script_content, file=f)

    slurm_command = [
        piece.format(
            array_index_spec=array_index_spec,
            script_filename=script_file,
        )
        for piece in SBATCH_COMMAND_TEMPLATE
    ]
    print('Running', ' '.join(slurm_command))
    check_call(slurm_command)

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('ftp_list_file', type=Path)
    p.add_argument(
        'array_index_spec',
        help=(
            'Selection of FTP URLs to process, in Slurm array format. Examples: "0-10", "1,3,5,7". '
            'Note that this parameter is not validated here and is passed as-is to the Slurm `sbatch` '
            'call. See https://slurm.schedmd.com/job_array.html for more information.'
        ),
    )
    p.add_argument('--pool', default='zbj1', help='Node pool')
    p.add_argument('-s', '--subprocesses', type=int, default=1)
    args = p.parse_args()

    check_ncbi_prefetch_location()
    queue_jobs(args.ftp_list_file, args.array_index_spec, args.pool, args.subprocesses)
