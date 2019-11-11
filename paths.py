from pathlib import Path as _Path

# Most of the values here will be used in string formatting operations to
# build command lists for the `subprocess` module, so it doesn't really
# matter whether these values are of type `str` or `Path`. Might be useful
# to use `Path` objects in overrides though, for things like `Path.expanduser`.

FASTQ_DUMP_PATH = _Path('fastq-dump')
HISAT2_PATH = _Path('hisat2')
# Not an actual file on disk; this is the "base" name
REFERENCE_INDEX_PATH = _Path('~/data/hisat2-indexes/mm10-splice-sites/mm10').expanduser()

OUTPUT_PATH = _Path('~/data/expr-processed').expanduser()

del _Path

# Keep this as the last section of this file:
try:
    from paths_override import *
except ImportError:
    pass
