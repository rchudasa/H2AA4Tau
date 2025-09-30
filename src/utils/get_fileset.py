import json
import glob

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
