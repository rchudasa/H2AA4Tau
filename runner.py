# import argparse
# import coffea
# from coffea.nanoevents import NanoAODSchema
# from coffea.processor import Runner, DaskExecutor, IterativeExecutor
# from src.utils.get_fileset import get_fileset
# from src.processors.fourtau_processor import FourTauProcessor
# import yaml
# import logging

# # Set up logging for debugging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def main(args):
#     logger.info(f"Starting analysis with Coffea {coffea.__version__}")
    
#     # Load config
#     try:
#         with open(args.cfg) as f:
#             cfg = yaml.safe_load(f)
#         logger.info(f"Loaded config from {args.cfg}")
#     except FileNotFoundError:
#         logger.error(f"Config file {args.cfg} not found")
#         raise

#     # Get fileset
#     try:
#         fileset = get_fileset(cfg["dataset"]["jsons"], max_files=args.limit)
#         logger.info(f"Loaded fileset with datasets: {list(fileset.keys())}")
#     except Exception as e:
#         logger.error(f"Failed to load fileset: {str(e)}")
#         raise

#     # Executor setup
#     if args.executor == "dask":
#         try:
#             from dask.distributed import Client
#             client = Client("tcp://dask-lxplus.cern.ch:8786")
#             executor = DaskExecutor(client=client)
#             logger.info("Initialized DaskExecutor with LXPLUS scheduler")
#         except Exception as e:
#             logger.error(f"Failed to initialize Dask client: {str(e)}")
#             raise
#     else:
#         executor = IterativeExecutor()
#         logger.info("Initialized IterativeExecutor")

#     # Runner setup
#     try:
#         runner = Runner(
#             executor=executor,
#             schema=NanoAODSchema,
#             chunksize=cfg["run_options"]["chunk"],
#             maxchunks=None,
#             skipbadfiles=cfg["run_options"]["skipbadfiles"],
#             #retries=cfg["run_options"]["retries"],
#             xrootdtimeout=60,
#             align_clusters=False,
#             savemetrics=False,
#             processor_compression=1,
#             use_skyhook=False
#         )
#         logger.info("Initialized Runner with NanoAODSchema")
#     except Exception as e:
#         logger.error(f"Failed to initialize Runner: {str(e)}")
#         raise

#     # Run processing
#     try:
#         output = runner(
#             fileset=fileset,
#             treename="Events",
#             processor_instance=FourTauProcessor(year=cfg["dataset"]["year"]),
#             uproot_options={}
#         )
#         logger.info("Processing completed successfully")
#     except Exception as e:
#         logger.error(f"Processing failed: {str(e)}")
#         raise

#     # Save output
#     try:
#         from coffea import util
#         util.save(output, f"output/histos_{args.executor}.coffea")
#         logger.info(f"Output saved to output/histos_{args.executor}.coffea")
#     except Exception as e:
#         logger.error(f"Failed to save output: {str(e)}")
#         raise

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Run H->AA->4Tau Processor")
#     parser.add_argument("--cfg", default="config/run3_4tau.py", help="Config file (YAML)")
#     parser.add_argument("--executor", choices=["iterative", "dask"], default="iterative", help="Executor type")
#     parser.add_argument("--limit", type=int, default=None, help="Max files per dataset")
#     args = parser.parse_args()
#     main(args)

from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from src.processors.fourtau_processor import FourTauProcessor
import uproot
import coffea.processor as processor


# Define the fileset
fileset = {
    'QCD': ["root://cms-xrd-global.cern.ch//store/mc/Run3Summer23BPixNanoAODv12/QCD_PT-15to7000_TuneCP5_13p6TeV_pythia8/NANOAODSIM/KeepRAW_130X_mcRun3_2023_realistic_postBPix_v2-v2/2560000/659093b4-f452-4576-9559-bf307c9eab83.root"
    ],
    'Data': ["root://cms-xrd-global.cern.ch//store/data/Run2023C/Tau/NANOAOD/PromptNanoAODv12_v3-v1/70000/83788139-9176-4859-9ead-f1037ab3da96.root"
    ]
}

# Configure the executor
executor = processor.FuturesExecutor(workers=4)

# Set up the runner
runner = processor.Runner(
    executor=executor,
    schema=NanoAODSchema,
    chunksize=100000,  # Number of events per chunk
    maxchunks=None  # Process all chunks; set a number to limit
)

# Run the processor
result = runner(
    fileset,
    treename='Events',
    processor_instance=FourTauProcessor(year="2023")
)

# Save or inspect the output
print(result)

from coffea import util
util.save(result, f"output/histos_v1.coffea")