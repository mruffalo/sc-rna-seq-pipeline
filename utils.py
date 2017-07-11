from datetime import datetime
from itertools import cycle, islice, zip_longest
from math import factorial
from os import environ, getuid, PathLike, scandir
from pathlib import Path
import pwd
from subprocess import Popen, check_output
from typing import Iterable, List, Sequence, Tuple, TypeVar

if 'SSH_CONNECTION' in environ:
    import matplotlib
    matplotlib.use('Agg')

TIMESTAMP_FORMAT = '%Y%m%d-%H%M%S'

DATA_PATH = Path('data')
DOWNLOAD_PATH = Path('download')
OUTPUT_PATH = Path('output')
SLURM_PATH = Path('slurm_generated')

# Depends on running on a Lane cluster node
SCRATCH_PATH = Path("/scratch")

USERNAME = pwd.getpwuid(getuid())[0]

SLURM_SUBMIT_COMMAND_TEMPLATE = [
    'sbatch',
    '{script_path}',
]

T = TypeVar('T')

def replace_extension(path: PathLike, new_extension: str) -> Path:
    """
    new_extension can include a leading . or not; doesn't matter

    >>> p = Path('file.txt')
    >>> replace_extension(p, 'csv')
    PosixPath('file.csv')
    >>> replace_extension(p, '.csv')
    PosixPath('file.csv')
    """
    fixed_new_extension = new_extension.lstrip('.')
    p = Path(path)
    return p.parent / (p.stem + '.' + fixed_new_extension)

def append_to_filename(path: Path, addition: str) -> Path:
    """
    >>> p = Path('file.txt')
    >>> append_to_filename(p, '-appended')
    PosixPath('file-appended.txt')
    """
    new_filename = f'{path.stem}{addition}{path.suffix}'
    return path.parent / new_filename

def get_now_str() -> str:
    return datetime.now().strftime(TIMESTAMP_FORMAT)

def ensure_dir(path: Path):
    try:
        path.mkdir(parents=True)
    except FileExistsError:
        pass
    return path

def ensure_parent_dir(path: Path):
    return ensure_dir(path.parent)

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

GIT_FILENAMES = {
    'revision': 'revision.txt',
    'staged_patch': 'staged.patch',
    'unstaged_patch': 'unstaged.patch',
}

def git_revision() -> str:
    command = ['git', 'rev-parse', 'HEAD']
    revision = check_output(command).decode().strip()
    return revision

def git_staged_changes_exist() -> bool:
    command = ['git', 'diff-index', '--quiet', '--cached', 'HEAD']
    p = Popen(command)
    p.wait()
    return bool(p.returncode)

def git_staged_patch() -> str:
    command = ['git', 'diff', '--cached']
    patch = check_output(command).decode()
    return patch

def git_unstaged_changes_exist() -> bool:
    command = ['git', 'diff-files', '--quiet']
    p = Popen(command)
    p.wait()
    return bool(p.returncode)

def git_unstaged_patch() -> str:
    command = ['git', 'diff']
    patch = check_output(command).decode()
    return patch

def write_git_data(data_path: Path):
    with open(data_path / GIT_FILENAMES['revision'], 'w') as f:
        print(git_revision(), file=f)

    if git_staged_changes_exist():
        with open(data_path / GIT_FILENAMES['staged_patch'], 'w') as f:
            print(git_staged_patch(), file=f)

    if git_unstaged_changes_exist():
        with open(data_path / GIT_FILENAMES['unstaged_patch'], 'w') as f:
            print(git_unstaged_patch(), file=f)

def create_path(base_path: Path, description: str, path_desc: str, print_path=True) -> Path:
    path = base_path / '{}_{}'.format(description, get_now_str())
    ensure_dir(path)
    if print_path:
        print(f'{path_desc} directory: {path}')
    return path

def create_output_path(description: str, print_path=True) -> Path:
    return create_path(OUTPUT_PATH, description, 'Output', print_path)

def create_data_path(description: str, print_path=True) -> Path:
    data_path = create_path(DATA_PATH, description, 'Data', print_path)
    # write_git_data(data_path)
    return data_path

def create_slurm_path(description: str, print_path=True) -> Path:
    return create_path(SLURM_PATH, description, 'Slurm', print_path)

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

Walk_Result = Tuple[Path, Sequence[Path], Sequence[Path]]

def pathlib_walk(top, topdown=True, onerror=None, followlinks=False) -> Iterable[Walk_Result]:
    """Like Python 3.5's implementation of os.walk() -- faster than
    the pre-Python 3.5 version as it uses scandir() internally.
    """
    dirs = []
    nondirs = []

    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.
    top = Path(top)
    try:
        scandir_it = scandir(str(top))
    except OSError as error:
        if onerror is not None:
            onerror(error)
        return

    while True:
        try:
            try:
                entry = next(scandir_it)
            except StopIteration:
                break
        except OSError as error:
            if onerror is not None:
                onerror(error)
            return

        try:
            is_dir = entry.is_dir()
        except OSError:
            # If is_dir() raises an OSError, consider that the entry is not
            # a directory, same behaviour than os.path.isdir().
            is_dir = False

        p = Path(entry.path)
        if is_dir:
            dirs.append(p)
        else:
            nondirs.append(p)

        if not topdown and is_dir:
            # Bottom-up: recurse into sub-directory, but exclude symlinks to
            # directories if followlinks is False
            if followlinks:
                walk_into = True
            else:
                try:
                    is_symlink = entry.is_symlink()
                except OSError:
                    # If is_symlink() raises an OSError, consider that the
                    # entry is not a symbolic link, same behaviour than
                    # os.path.islink().
                    is_symlink = False
                walk_into = not is_symlink

            if walk_into:
                for entry in pathlib_walk(entry.path, topdown, onerror, followlinks):
                    yield entry

    # Yield before recursion if going top down
    if topdown:
        yield top, dirs, nondirs

        # Recurse into sub-directories
        for new_path in dirs:
            # Issue #23605: os.path.islink() is used instead of caching
            # entry.is_symlink() result during the loop on os.scandir() because
            # the caller can replace the directory entry during the "yield"
            # above.
            if followlinks or not new_path.is_symlink():
                for entry in pathlib_walk(new_path, topdown, onerror, followlinks):
                    yield entry
    else:
        # Yield after recursion if going bottom up
        yield top, dirs, nondirs

def pathlib_walk_glob(base_path: Path, pattern: str) -> Iterable[Path]:
    for dirpath, _, filepaths in pathlib_walk(base_path):
        for filepath in filepaths:
            if filepath.match(pattern):
                yield filepath

def find_newest_path(base_path: Path, label: str) -> Path:
    candidates = []

    for child in base_path.iterdir():
        if not child.is_dir():
            continue
        try:
            pieces = child.name.rsplit('_', maxsplit=1)
            if pieces[0] != label:
                continue

            # Skip empty directories, or those containing only Git data
            filenames = {f.name for f in child.iterdir()}
            if not (filenames - set(GIT_FILENAMES.values())):
                continue

            dt = datetime.strptime(pieces[1], TIMESTAMP_FORMAT)
            candidates.append((dt, child))
        except (IndexError, ValueError):
            continue

    if not candidates:
        raise FileNotFoundError(f'No data paths exist with label "{label}"')

    return max(candidates)[1]

def find_newest_data_path(label: str) -> Path:
    return find_newest_path(DATA_PATH, label)

def strip_prefix(string: str, prefix: str) -> str:
    """
    :param string: String to strip `prefix` from, if present
    :param prefix: Prefix to remove from `string`
    :return:
    """
    if string.startswith(prefix):
        return string[len(prefix):]
    return string

del T
