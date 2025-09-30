import json
import os

def files_to_json(input_files, output_json, dataset_name, is_mc, xsec=None):
    with open(input_files) as f:
        files = [f"root://cms-xrd-global.cern.ch/{line.strip()}" for line in f if line.strip()]
    data = {
        dataset_name: {
            "files": files,
            "metadata": {"is_mc": is_mc, "xsec": xsec} if is_mc else {"is_mc": is_mc}
        }
    }
    with open(output_json, "w") as f:
        json.dump(data, f, indent=4)

# Ensure metadata directory exists
os.makedirs("metadata", exist_ok=True)

# Generate individual JSONs
#files_to_json("metadata/signal_files.txt", "metadata/signal.json", "Signal", True, 0.001)  # Update xsec
files_to_json("../metadata/DYto2L_files.txt", "../metadata/DYto2L.json", "DY", True, 6077.22)
files_to_json("../metadata/data_tau2023C.txt", "../metadata/data_tau2023C.json", "Data", False)
