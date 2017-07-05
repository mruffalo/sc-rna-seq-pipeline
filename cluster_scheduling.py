#!/usr/bin/env python3
from argparse import ArgumentParser
from subprocess import check_call
from pathlib import Path

from utils import create_slurm_path

script_template = """
#!/bin/bash

#SBATCH -p zbj1
#SBATCH --mem=8192
#SBATCH --mincpus=1

python3 process_sra_from_ftp.py {ftp_list_file}
""".strip()

SBATCH_COMMAND_TEMPLATE = [
    'sbatch',
    '--array={array_index_spec}',
    '{script_filename}',
]

SCRIPT_FILENAME = 'bulk_download.sh'

def queue_jobs(ftp_list_file: Path, array_index_spec: str):
    slurm_path = create_slurm_path('cluster_scheduling')

    script_file = slurm_path / SCRIPT_FILENAME
    print('Saving script to', script_file)
    with open(script_file, 'w') as f:
        print(script_template.format(ftp_list_file=ftp_list_file), file=f)

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
    args = p.parse_args()

    queue_jobs(args.ftp_list_file, args.array_index_spec)
