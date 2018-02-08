from pathlib import Path
from typing import Dict

from utils import SCRATCH_PATH

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

NCBI_DOWNLOAD_PATH_KEY = '/repository/user/main/public/root'
# Download must be a subdirectory of this
REQUIRED_DOWNLOAD_DIR = SCRATCH_PATH

def get_ncbi_download_path() -> Path:
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
    return Path(ncbi_config[NCBI_DOWNLOAD_PATH_KEY])

def check_ncbi_prefetch_location():
    download_path = get_ncbi_download_path()
    if REQUIRED_DOWNLOAD_DIR not in download_path.parents:
        raise EnvironmentError(f'Not downloading to {download_path} (not a subdirectory of {REQUIRED_DOWNLOAD_DIR}).')
