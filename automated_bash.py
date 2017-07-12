#!/usr/bin/env python3
from argparse import ArgumentParser
from pathlib import Path
from shutil import rmtree
from subprocess import check_call, check_output

from map_reads_to_genes import map_reads_to_genes
from paths import *
from utils import SCRATCH_PATH, USERNAME, ensure_dir, pathlib_walk_glob, replace_extension, append_to_filename

FASTQ_TEST_COMMAND_TEMPLATE = [
    '{fastq_dump_command}',
    '-X',
    '1',
    '-Z',
    '--split-spot',
    '{input_path}'
]

FASTQ_CONVERT_COMMAND_TEMPLATE = [
    '{fastq_dump_command}',
    '{input_path}',
    '-O',
    '{output_path}',
]

HISAT2_PAIR_END_COMMAND_TEMPLATE = [
    '{hisat2_command}',
    '-x',
    '{reference_path}',
    '-1',
    '{input_path_1}',
    '-2',
    '{input_path_2}',
    '-S',
    '{output_path}',
    '-p',
    '{subprocesses}',
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


def is_paired_sra(sra_path: Path):
    try:
        fastq_command = [
            piece.format(
                fastq_dump_command=FASTQ_DUMP_PATH,
                input_path=sra_path
            )
            for piece in FASTQ_TEST_COMMAND_TEMPLATE
        ]
        print('Running', ' '.join(fastq_command))
        contents = check_output(fastq_command)
    except:
        raise Exception("Error running fastq-dump on", sra_path)
    enter = '\n'
    if contents.count(enter.encode()) == 4:
        return False
    elif contents.count(enter.encode()) == 8:
        return True
    else:
        raise Exception("Unexpected output from fast-dump on ", sra_path)


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

        if is_paired_sra(sra_path):
            fastq_command.append('--split-files')

            print('Running', ' '.join(fastq_command))
            check_call(fastq_command)

            fastq_path_1 = scratch_path / replace_extension(sra_path, 'fastq').name
            fastq_path_1 = append_to_filename(fastq_path_1, '_1')
            fastq_path_2 = scratch_path / replace_extension(sra_path, 'fastq').name
            fastq_path_2 = append_to_filename(fastq_path_2, '_2')
            sam_path = replace_extension(sra_path, 'sam')

            hisat_command = [
                piece.format(
                    hisat2_command=HISAT2_PATH,
                    reference_path=REFERENCE_INDEX_PATH,
                    input_path_1=fastq_path_1,
                    input_path_2=fastq_path_2,
                    output_path=sam_path,
                    subprocesses=subprocesses,
                )
                for piece in HISAT2_PAIR_END_COMMAND_TEMPLATE
            ]
        else:
            print('Running', ' '.join(fastq_command))
            check_call(fastq_command)

            fastq_path = scratch_path / replace_extension(sra_path, 'fastq').name
            sam_path = replace_extension(fastq_path, 'sam')

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
        map_reads_to_genes(sam_path)

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
        default=1,
    )
    args = p.parse_args()

    main(args.subprocesses)
