#!/usr/bin/env python3
from pathlib import Path
from subprocess import check_call, check_output
from typing import List, Optional, Tuple

from data_path_utils import append_to_filename
import pandas as pd

from map_reads_to_genes import map_reads_to_genes
from paths import *

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

HISAT2_COMMAND_COMMON_PIECES = [
    '{hisat2_command}',
    '-x',
    '{reference_path}',
    '-S',
    '{output_path}',
    '-p',
    '{subprocesses}',
]

HISAT2_PAIRED_END_PIECES = [
    '-1',
    '{input_path_1}',
    '-2',
    '{input_path_2}',
]

HISAT2_SINGLE_END_PIECES = [
    '-U',
    '{input_path}',
]

def is_paired_sra(sra_path: Path) -> bool:
    try:
        fastq_command = [
            piece.format(
                fastq_dump_command=FASTQ_DUMP_PATH,
                input_path=sra_path
            )
            for piece in FASTQ_TEST_COMMAND_TEMPLATE
        ]
        print('Running', ' '.join(fastq_command))
        contents = check_output(fastq_command).decode('utf-8')
    except Exception as e:
        raise Exception(f"Error running fastq-dump on {sra_path}") from e
    newline = '\n'
    if contents.count(newline) == 4:
        return False
    elif contents.count(newline) == 8:
        return True
    else:
        raise ValueError(f'Unexpected output from {FASTQ_DUMP_PATH.name} on {sra_path}:\n{contents!r}')

def convert_sra_to_fastq(sra_path: Path, scratch_dir: Optional[Path]=None) -> List[Path]:
    """
    :param sra_path:
    :param scratch_dir: Directory where the FASTQ file will be stored, with
        the same name as the SRA file. If omitted, the FASTQ file will be
        written to the same directory as the SRA file.
    :return: A List of fastq Paths. Contains one Path if single-end,
        two if paired-end.
    """
    if scratch_dir is None:
        scratch_dir = sra_path.parent

    fastq_command = [
            piece.format(
                fastq_dump_command=FASTQ_DUMP_PATH,
                input_path=sra_path,
                output_path=scratch_dir,
            )
            for piece in FASTQ_CONVERT_COMMAND_TEMPLATE
        ]

    paired_end = is_paired_sra(sra_path)

    if paired_end:
        fastq_command.append('--split-files')

    print('Running', ' '.join(fastq_command))
    check_call(fastq_command)

    if paired_end:
        fastq_path_1 = append_to_filename(scratch_dir / sra_path.with_suffix('.fastq').name, '_1')
        fastq_path_2 = append_to_filename(scratch_dir / sra_path.with_suffix('.fastq').name, '_2')
        fastq_paths = [fastq_path_1, fastq_path_2]
    else:
        fastq_paths = [scratch_dir / sra_path.with_suffix('.fastq').name]

    return fastq_paths

def align_fastq_compute_expr(
        fastq_paths: List[Path],
        subprocesses: int,
        sam_path: Optional[Path]=None,
        hisat2_options: Optional[str]=None,
        reference_path: Optional[Path]=None,
) -> Tuple[pd.Series, pd.Series]:
    # Bail out early if no FASTQ files provided, so we can use the first
    # to assign `sam_path` if necessary
    if not fastq_paths:
        raise ValueError('No FASTQ files provided')

    if sam_path is None:
        sam_path = fastq_paths[0].with_suffix('.sam')

    if reference_path is None:
        reference_path = REFERENCE_INDEX_PATH

    hisat_command = [
        piece.format(
            hisat2_command=HISAT2_PATH,
            reference_path=reference_path,
            output_path=sam_path,
            subprocesses=subprocesses,
        )
        for piece in HISAT2_COMMAND_COMMON_PIECES
    ]
    if hisat2_options is not None:
        hisat_command.extend(hisat2_options.split())

    if len(fastq_paths) == 2:
        hisat_command.extend(
            piece.format(
                input_path_1=fastq_paths[0],
                input_path_2=fastq_paths[1],
            )
            for piece in HISAT2_PAIRED_END_PIECES
        )
    elif len(fastq_paths) == 1:
        hisat_command.extend(
            piece.format(
                input_path=fastq_paths[0],
            )
            for piece in HISAT2_SINGLE_END_PIECES
        )
    else:
        message_pieces = [f'Bad FASTQ file count: {len(fastq_paths)}']
        message_pieces.extend(f'\t{path}' for path in fastq_paths)
        raise ValueError('\n'.join(message_pieces))

    try:
        print('Running', ' '.join(hisat_command))
        check_call(hisat_command)
        print('Computing RPKM from', sam_path)
        rpkm, summary = map_reads_to_genes(sam_path)
    finally:
        sam_path.unlink()
    return rpkm, summary

def process_sra_file(
        sra_path: Path,
        subprocesses: int,
        sam_path: Optional[Path]=None,
        hisat2_options: Optional[str] = None,
        reference_path: Optional[Path] = None,
) -> Tuple[pd.Series, pd.Series]:
    fastq_paths = convert_sra_to_fastq(sra_path)
    return align_fastq_compute_expr(
        fastq_paths=fastq_paths,
        subprocesses=subprocesses,
        sam_path=sam_path,
        hisat2_options=hisat2_options,
        reference_path=reference_path,
    )
