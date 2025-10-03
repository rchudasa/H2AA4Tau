import json
import glob
import subprocess
import os
import logging
import warnings
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
def get_fileset(json_files, max_files=None):
    fileset = {}
    for json_file in glob.glob(json_files) if isinstance(json_files, str) else json_files:
        with open(json_file, "r") as f:
            data = json.load(f)
            for dataset, info in data.items():
                fileset[dataset] = {
                    "files": info["files"][:max_files] if max_files else info["files"],
                    "metadata": info["metadata"]
                }
    return fileset


def get_proxy():
    """
    Use voms-proxy-info to check if a proxy is available.
    If so, copy it to $HOME/.proxy and return the path.
    An exception is raised in the following cases:
    - voms-proxy-info is not installed
    - the proxy is not valid

    :return: Path to proxy
    :rtype: str
    """
    if subprocess.getstatusoutput("voms-proxy-info")[0] != 0:
        logger.error("voms-proxy-init not found, You need a valid certificate to access data over xrootd.")
        warnings.warn("voms-proxy-init not found, You need a valid certificate to access data over xrootd.", stacklevel=1)
    else:
        stat, out = subprocess.getstatusoutput("voms-proxy-info -e -p")
        # stat is 0 if the proxy is valid
        if stat != 0:
            logger.warning("No valid proxy found. Please create one.")
            warnings.warn("No valid proxy found. Please create one.", stacklevel=1)

        _x509_localpath = out
        _x509_path = os.environ["HOME"] + f"/.{_x509_localpath.split('/')[-1]}"
        logger.info(f"Copying proxy from {_x509_localpath} to {_x509_path}")
        os.system(f"cp {_x509_localpath} {_x509_path}")

        return _x509_path