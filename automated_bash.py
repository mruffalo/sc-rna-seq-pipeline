#!/usr/bin/env python3
from argparse import ArgumentParser
import os
from pathlib import Path
import pwd
from shutil import rmtree
from subprocess import check_call

from parse_gene_intervals import parse_gene_intervals
from paths import *
from utils import SCRATCH_PATH, ensure_dir, pathlib_walk_glob, replace_extension

FASTQ_CONVERT_COMMAND_TEMPLATE = [
    '{fastq_dump_command}',
    '{input_path}',
    '-O',
    '{output_path}',
]

HISAT2_COMMAND_TEMPLATE = [
    '{hisat2_command}',
    '-x',
    '{reference_path}',
    '-U',
    '{input_path}',
    '-S',
    '{output_path}',
    '-p',
    '{subprocesses}',
]

USERNAME = pwd.getpwuid(os.getuid())[0]

def process_sra_file(sra_path: Path, subprocesses: int):
    scratch_path = ensure_dir(SCRATCH_PATH / USERNAME / sra_path.stem)

    try:
        fastq_command = [
            piece.format(
                fastq_dump_command=FASTQ_DUMP_PATH,
                input_path=sra_path,
                output_path=scratch_path,
            )
            for piece in FASTQ_CONVERT_COMMAND_TEMPLATE
        ]
        print('Running', ' '.join(fastq_command))
        check_call(fastq_command)

        fastq_path = scratch_path / replace_extension(sra_path, 'fastq').name
        sam_path = replace_extension(fastq_path, 'sam')

        # Align FASTQ files to the mouse genome using 24 cores.
        hisat_command = [
            piece.format(
                hisat2_command=HISAT2_PATH,
                reference_path=REFERENCE_INDEX_PATH,
                input_path=fastq_path,
                output_path=sam_path,
                subprocesses=subprocesses,
            )
            for piece in HISAT2_COMMAND_TEMPLATE
        ]
        print('Running', ' '.join(hisat_command))
        check_call(hisat_command)

        print('Computing RPKM from', sam_path)
        parse_gene_intervals(sam_path)

    finally:
        print('Deleting', scratch_path)
        rmtree(scratch_path)

def main(subprocesses: int):
    # Walk through all SRA files in the directory
    data_path = SCRATCH_PATH / USERNAME / 'data'
    for sra_path in pathlib_walk_glob(data_path, '*.sra'):
        process_sra_file(sra_path, subprocesses)

if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument(
        '-s',
        '--subprocesses',
        help='Number of subprocesses for alignment in each run of HISAT2',
        type=int,
        default=1
    )
    args = p.parse_args()

    main(args.subprocesses)
