import hist
import dask
import awkward as ak
import hist.dask as hda
import dask_awkward as dak
from coffea.dataset_tools import apply_to_fileset
from coffea import processor
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea.nanoevents.methods import candidate
from coffea.dataset_tools import (
    apply_to_fileset,
    max_chunks,
    preprocess,
)
import os
import argparse
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-o', '--outdir', default='figures', type=str, help='Output image file dir.')
args = parser.parse_args()

out_dir = args.outdir
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
    print(f"Directory '{out_dir}' created.")
else:
    print(f"Directory '{out_dir}' already exists.")

#from distributed import Client

#client = Client()

class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        pass

    def process(self, events):
        print("Events type:", type(events))
        dataset_axis = hist.axis.StrCategory(
            [], growth=True, name="dataset", label="Primary dataset"
        )
        mass_axis = hist.axis.Regular(
            75, 0, 150, name="mass", label=r"$m_{\mu\mu}$ [GeV]"
        )
        pt_axis = hist.axis.Regular(75, 0, 150, name="pt", label=r"$p_{T,\mu}$ [GeV]")
        
        dataset = events.metadata['dataset']
        print("_______________________",type(dataset))
        print(events.jetPt)
        print(ak.num(events,axis=0))
        
        h_pt = hda.hist.Hist(
            dataset_axis, pt_axis
        )

        h_pt.fill(
            dataset=dataset,
            pt=ak.flatten(events.jetPt),
        )
        print("Total events:", ak.num(events,axis=0)) 
        #  q2_hist = (
        # hda.Hist.new.Reg(100, 0, 200, name="ptj", label="Jet $p_{T}$ [GeV]")
        # .Double()
        # .fill(ak.flatten(events.jetPt))
        # )
        
        return {
            "entries": ak.num(events, axis=0),
            "jetPt": h_pt,
            
        }

    def postprocess(self, accumulator):
        pass

import os

def makeFileSet(path):
    files = os.listdir(path)
    root_files = [path+f+":fevt/RHTree" for f in files if f.endswith(".root")]
    root_set = set(root_files)
    return root_set



# path_3p7 = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/withTrigger/HToAATo4Tau_M_3p7_pythia8_2018UL_AOD/3p7_RHEvent-Trigger_ntuples_v2/240813_103752/0000/"
# root_3p7_set = makeFileSet(path_3p7)
# path_8 = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/withTrigger/HToAATo4Tau_M_8_pythia8_2018UL_AOD/8_RHEvent-Trigger_ntuples_v2/240814_091111/0000/"
# root_8_set = makeFileSet(path_8)




infile = '/afs/cern.ch/work/r/rchudasa/private/TauClassification/run3/CMSSW_13_0_14/src/MLAnalyzerRun3/HAA4tau_M3p7_allRH_sameNtuples_slimmedHCALRH_numEvent10000.root'

fileset = {
    "Mass3p7": {
        "files": infile+":fevt/RHTree"
    },
    # "Mass8": {
    #     "files": root_8_set
    # },
}
to_compute = apply_to_fileset(
    MyProcessor(),
    fileset,
    schemaclass=BaseSchema,
)



dak.necessary_columns(to_compute)
import time
tstart = time.time()

(out,) = dask.compute(to_compute)
print(out)

elapsed = time.time() - tstart
print(elapsed)

# scaled = {}
# for (name1, h1) in zip(
#     out["Mass3p7"].items()
# ):
#     if isinstance(h1, hist.Hist) :
#         scaled[name1] = h1.copy()


import matplotlib.pyplot as plt
#plt.ion()
fig, ax = plt.subplots()
out['Mass3p7']['jetPt'].plot1d(ax=ax, overlay="dataset")
ax.set_yscale("log")
ax.set_ylim(1, None)
plt.savefig(f"{out_dir}/miniAOD_jetPt.pdf",facecolor='w',dpi=300)
#plt.show()

# scaled = {}
# for (name1, h1), (name2, h2) in zip(
#     #out["ZZto4mu"].items(), out["SMHiggsToZZTo4L"].items()
#     out["Mass3p7"].items(), out["Mass8"].items()
# ):
#     if isinstance(h1, hist.Hist) and isinstance(h2, hist.Hist):
#         scaled[name1] = h1.copy() + h2.copy()


# import matplotlib.pyplot as plt
# plt.ion()
# fig, ax = plt.subplots()
# scaled['jetPt'].plot1d(ax=ax, overlay="dataset")
# ax.set_yscale("log")
# ax.set_ylim(1, None)
# #plt.show()

