#!/usr/bin/env python3
from argparse import ArgumentParser
from subprocess import check_call
from pathlib import Path

from data_path_utils import create_slurm_path

from ncbi_sra_toolkit_config import check_ncbi_prefetch_location

script_template = """
#!/bin/bash

#SBATCH -p {pool}
#SBATCH --mem=8192
#SBATCH --mincpus={subprocesses}
#SBATCH -o {stdout_path}

python3 process_sra_from_srr_id.py -s {subprocesses} {srr_list_file}
""".strip()

SBATCH_COMMAND_TEMPLATE = [
    'sbatch',
    '--array={array_index_spec}',
    '{script_filename}',
]

SCRIPT_FILENAME = 'bulk_download.sh'

def queue_jobs(srr_list_file: Path, array_index_spec: str, pool: str, subprocesses: int):
    slurm_path = create_slurm_path('cluster_scheduling')

    script_file = slurm_path / SCRIPT_FILENAME
    print('Saving script to', script_file)
    with open(script_file, 'w') as f:
        script_content = script_template.format(
            srr_list_file=srr_list_file,
            pool=pool,
            subprocesses=subprocesses,
            stdout_path=script_file.with_suffix('.out')
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
    p.add_argument('srr_list_file', type=Path)
    p.add_argument(
        'array_index_spec',
        help=(
            'Selection of SRR IDs to process, in Slurm array format. Examples: "0-10", "1,3,5,7". '
            'Note that this parameter is not validated here and is passed as-is to the Slurm `sbatch` '
            'call. See https://slurm.schedmd.com/job_array.html for more information.'
        ),
    )
    p.add_argument('--pool', default='zbj1', help='Node pool')
    p.add_argument('-s', '--subprocesses', type=int, default=1)
    args = p.parse_args()

    check_ncbi_prefetch_location()
    queue_jobs(args.srr_list_file, args.array_index_spec, args.pool, args.subprocesses)
