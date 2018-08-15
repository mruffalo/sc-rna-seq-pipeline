from argparse import ArgumentParser
from itertools import cycle, islice, zip_longest
from math import ceil, log10
from os import getuid
from pathlib import Path
import pwd
from typing import Iterable, List, TypeVar

DOWNLOAD_PATH = Path('download')

# Depends on running on a Lane cluster node
SCRATCH_PATH = Path("/scratch")

USERNAME = pwd.getpwuid(getuid())[0]

SLURM_SUBMIT_COMMAND_TEMPLATE = [
    'sbatch',
    '{script_path}',
]

T = TypeVar('T')

def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks
    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def roundrobin(*iterables):
    "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
    # Recipe credited to George Sakkis
    pending = len(iterables)
    nexts = cycle(iter(it).__next__ for it in iterables)
    while pending:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            pending -= 1
            nexts = cycle(islice(nexts, pending))

def sorted_set_op(items: Iterable[T], func) -> List[T]:
    sets = [set(item) for item in items]
    data = func(*sets)
    return sorted(data)

def sorted_intersection(*items: T) -> List[T]:
    return sorted_set_op(items, set.intersection)

def sorted_union(*items: T) -> List[T]:
    return sorted_set_op(items, set.union)

def first(iterable: Iterable[T]) -> T:
    return next(iter(iterable))

def strip_prefix(string: str, prefix: str) -> str:
    """
    :param string: String to strip `prefix` from, if present
    :param prefix: Prefix to remove from `string`
    :return:
    """
    if string.startswith(prefix):
        return string[len(prefix):]
    return string

def digits(value: int) -> int:
    return ceil(log10(value + 1))

def normalize_whitespace(string: str) -> str:
    """
    :return: A new string with all whitespace normalized to single
    spaces (newlines, tabs, multiple spaces in a row, etc.)
    """
    return ' '.join(string.split())

def add_common_command_line_arguments(p: ArgumentParser):
    p.add_argument(
        '-s',
        '--subprocesses',
        help='Number of subprocesses for alignment in each run of HISAT2',
        type=int,
        default=1,
    )
    p.add_argument('--reference-path', type=Path)
    p.add_argument(
        '--output-file',
        type=Path,
        help=normalize_whitespace(
            """
            Output file for gene expression and alignment metadata, saved in HDF5
            format (.hdf5 or .h5 file extension recommended). If omitted, data will
            be saved to 'expr.hdf5' inside the directory containing the FASTQ files.
            """
        ),
    )
    p.add_argument(
        '--hisat2-options',
        help=normalize_whitespace(
            """
            Extra options to pass to the HISAT2 aligner, passed as a single string.
            If passing multiple options, you will likely need to enclose the HISAT2
            options in quotes, e.g. --hisat2-options="--mp 4,2 --phred64"
            """
        ),
    )

del T
