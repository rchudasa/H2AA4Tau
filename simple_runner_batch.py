#!/usr/bin/env python
import json
from importlib import resources
import os
import sys
import time
from typing import List
from copy import deepcopy

import numpy as np
import uproot
from coffea import util
from coffea.nanoevents import NanoAODSchema, BaseSchema
from dask.distributed import Client, Worker, WorkerPlugin
from src.processors.fourtau_processor import FourTauProcessor
import uproot
import coffea.processor as processor
from dask_lxplus import CernCluster
import socket
from src.utils.misc import get_proxy

_x509_path = get_proxy() 
print(f"Using proxy at {_x509_path}")


fileset = {
    'QCD': ["root://xrootd-cms.infn.it//store/mc/Run3Summer23NanoAODv12/QCD_PT-15to7000_TuneCP5_Flat2022_13p6TeV_pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_v15-v3/2830000/18d50e3a-5e8a-4870-80d6-f11b8dcd3dd5.root"
    #'QCD': ["root://cms-xrd-global.cern.ch//store/mc/Run3Summer23BPixNanoAODv12/QCD_PT-15to7000_TuneCP5_13p6TeV_pythia8/NANOAODSIM/KeepRAW_130X_mcRun3_2023_realistic_postBPix_v2-v2/2560000/659093b4-f452-4576-9559-bf307c9eab83.root"
    ],
    'Data': ["root://xrootd-cms.infn.it//store/data/Run2023C/Tau/NANOAOD/22Sep2023_v1-v2/2560000/99612f9d-18b2-4402-b7a9-8127b7398a62.root"]
    #'Data': ["root://cms-xrd-global.cern.ch//store/data/Run2023C/Tau/NANOAOD/PromptNanoAODv12_v3-v1/70000/83788139-9176-4859-9ead-f1037ab3da96.root"]
}
def main():
    n_port = 8786
    cluster = CernCluster(
            cores=1,
            memory='3000MB',
            disk='1000MB',
            death_timeout = '3600',
            image_type="singularity",
            worker_image="/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-al9:latest",
            log_directory = "/afs/cern.ch/user/r/rchudasa/condor/log",
            scheduler_options={'port': n_port,'host': socket.gethostname()},
            job_extra={
                "log": "dask_job_output.log",
                "output": "dask_job_output.out",
                "error": "dask_job_output.err",
                "should_transfer_files": "Yes",
                "when_to_transfer_output": "ON_EXIT",
                '+JobFlavour': "longlunch"
            },
            job_script_prologue=[
                "export XRD_RUNFORKHANDLER=1",
                f"export X509_USER_PROXY={_x509_path}",
                "export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH",
                f"export PYTHONPATH=$PYTHONPATH:{os.getcwd()}:$_CONDOR_SCRATCH_DIR",
            ]
            ) 
    cluster.adapt(minimum=1, maximum=10)
    client = Client(cluster)
    print("Waiting for at least one worker...")
    client.wait_for_workers(1)

    runner = processor.Runner(
        executor=processor.DaskExecutor(client=client, retries=10),
        schema=NanoAODSchema, 
        chunksize=100000,      
        maxchunks=None,  
        skipbadfiles=True,
        xrootdtimeout=30,
    )
    output = runner(
        fileset,
        treename="Events",
        processor_instance=FourTauProcessor(year="2023")
    )
    util.save(output, f"output/histos_dask.coffea")
    print('Result is {}'.format(output))

if __name__ == '__main__':
    main()
