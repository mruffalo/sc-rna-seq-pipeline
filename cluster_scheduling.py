#!/usr/bin/env python3
from argparse import ArgumentParser
from math import ceil
from subprocess import check_call
from pathlib import Path

from data_path_utils import create_data_path, create_slurm_path

from ncbi_sra_toolkit_config import check_ncbi_prefetch_location
from utils import digits, grouper

script_template = """
#!/bin/bash

#SBATCH -p {pool}
#SBATCH --mem=8192
#SBATCH --mincpus={subprocesses}
#SBATCH -o {stdout_path}

python3 process_sra_from_srr_list.py -s {subprocesses} {srr_list_file}
""".strip()

SBATCH_COMMAND_TEMPLATE = [
    'sbatch',
    '--array={array_index_spec}',
    '{script_filename}',
]

SCRIPT_FILENAME_TEMPLATE = 'bulk_download_{:0{}}.sh'
SRR_LIST_FILENAME_TEMPLATE = 'srr_ids_{:0{}}.txt'
SCRIPT_LABEL = 'cluster_scheduling'

# Could also get this from `scontrol show config`, but hardcoding isn't too bad
SLURM_ARRAY_MAX = 1000

def queue_jobs(srr_list_file: Path, pool: str, subprocesses: int):
    data_path = create_data_path(SCRIPT_LABEL)
    slurm_path = create_slurm_path(SCRIPT_LABEL)

    with open(srr_list_file) as f:
        srr_ids = [line.strip() for line in f]

    srr_sublist_count = ceil(len(srr_ids) / SLURM_ARRAY_MAX)
    srr_filename_digits = digits(srr_sublist_count)

    for i, srr_sublist_raw in enumerate(grouper(srr_ids, SLURM_ARRAY_MAX)):
        srr_sublist_path = data_path / SRR_LIST_FILENAME_TEMPLATE.format(i, srr_filename_digits)
        print(f'{i:0{srr_filename_digits}} Saving SRR sublist to {srr_sublist_path}')

        srr_sublist = list(filter(None, srr_sublist_raw))

        with open(srr_sublist_path, 'w') as f:
            for srr_id in srr_sublist:
                print(srr_id, file=f)

        array_index_spec = f'0-{len(srr_sublist) - 1}'

        script_file = slurm_path / SCRIPT_FILENAME_TEMPLATE.format(i, srr_filename_digits)
        print(f'{i:0{srr_filename_digits}} Saving script to {script_file}')
        with open(script_file, 'w') as f:
            script_content = script_template.format(
                srr_list_file=srr_sublist_path.absolute(),
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
        print(f'{i:0{srr_filename_digits}} Running', ' '.join(slurm_command))
        check_call(slurm_command)

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('srr_list_file', type=Path)
    p.add_argument('--pool', default='zbj1', help='Node pool')
    p.add_argument('-s', '--subprocesses', type=int, default=1)
    args = p.parse_args()

    check_ncbi_prefetch_location()
    queue_jobs(args.srr_list_file, args.pool, args.subprocesses)
