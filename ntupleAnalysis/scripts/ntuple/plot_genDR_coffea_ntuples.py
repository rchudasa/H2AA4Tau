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
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use([hep.style.CMS, hep.style.firamath])

#from distributed import Client

#client = Client()

class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        pass

    def process(self, events):
        #print("Events type:", type(events))
        dataset_axis = hist.axis.StrCategory(
            [], growth=True, name="dataset", label="Primary dataset"
        )

        dr_axis = hist.axis.Regular(100, 0, 1.0, name="dR_tau1_tau2", label="dR($\tau_{1}$,$\tau_{2}$)")   
        dataset = events.metadata['dataset']
        #print("_______________________",type(dataset))

        h_tau1_tau2_dr = hda.hist.Hist(dataset_axis, dr_axis)

        h_tau1_tau2_dr.fill(dataset=dataset,dR_tau1_tau2=ak.flatten(events.dR_Tau1_Tau2))
        h_tau1_tau2_dr.fill(dataset=dataset,dR_tau1_tau2=ak.flatten(events.dR_Tau3_Tau4))

        #h_tau1_tau2_dr_norm = h_tau1_tau2_dr / h_tau1_tau2_dr.values().sum()
        pt_axis = hist.axis.Regular(50, 0, 100, name="pt", label="pt") 
 
        
        return {
            "entries": ak.num(events, axis=0),
            "tau1_tau2_dr": h_tau1_tau2_dr          
        }

    def postprocess(self, accumulator):
        pass

import os
inputPath = '/afs/cern.ch/work/r/rchudasa/private/TauClassification/run3/CMSSW_13_0_17/src/Gen/'
fileset = {
    "3p7": {"files": {inputPath+"GenInfo_only_H2AA4Tau_M3p7GeV.root":"fevt/RHTree"}},
    "4": {"files": {inputPath+"GenInfo_only_H2AA4Tau_M4GeV.root":"fevt/RHTree"}},
    "5": {"files": {inputPath+"GenInfo_only_H2AA4Tau_M5GeV.root":"fevt/RHTree"}},
    "6": {"files": {inputPath+"GenInfo_only_H2AA4Tau_M6GeV.root":"fevt/RHTree"}},
    "8": {"files": {inputPath+"GenInfo_only_H2AA4Tau_M8GeV.root":"fevt/RHTree"}},
    "10": {"files": {inputPath+"GenInfo_only_H2AA4Tau_M10GeV.root":"fevt/RHTree"}},
    "12": {"files": {inputPath+"GenInfo_only_H2AA4Tau_M12GeV.root":"fevt/RHTree"}},
    "14": {"files": {inputPath+"GenInfo_only_H2AA4Tau_M14GeV.root":"fevt/RHTree"}}
}

#print(len(fileset))


to_compute = apply_to_fileset(
    MyProcessor(),
    fileset,
    schemaclass=BaseSchema,
)


dak.necessary_columns(to_compute)
import time
tstart = time.time()

(out,) = dask.compute(to_compute)
#print(out['0p01'])

elapsed = time.time() - tstart
print(elapsed)

scaled = {}
dynamic_variables = {}
count = 0
for key,value in out.items():
    count +=1
    print(f"h{count}")
    #print(key, value)
    #print("____________________________________________________")
    if type(value) == dict:
        for key2, value2 in value.items():
            #print("Keys2: ", key2,"   value2", value2)
            if type(value2) == hist.Hist:
                #name = f"h{count}"
                #dynamic_variables[name] = value2
                #f"h{count}" = value2
                if count == 1:
                    scaled[key2] = value2.copy()
                else:
                    scaled[key2]+=value2.copy()


print(scaled)
import matplotlib.pyplot as plt



#plt.ion()
fig, ax = plt.subplots()
# # Plot with error bars for a specific dataset



histogram = scaled["tau1_tau2_dr"]
dataset_axis = histogram.axes["dataset"]
datasets = list(dataset_axis)
num_datasets = len(datasets)

# Use a large color map with many distinct colors
cmap = plt.get_cmap("Set1")  # Can also try "gnuplot", "nipy_spectral", "hsv", etc.
colors = [cmap(i / num_datasets) for i in range(num_datasets)]


for i, ds in enumerate(datasets):
    h_ds = histogram[ds, :]

    # Normalize
    values = h_ds.values()
    norm = values.sum()
    if norm > 0:
        h_ds = h_ds / norm  # Normalize bin content

    hep.histplot(
        h_ds,        # Slice per dataset
        #yerr=True,            # Enable error bars (sqrt(N))
        label=str(ds),        # Label for legend
        ax=ax,
        histtype='step',       # HEP-style step plot
        color=colors[i], 
        linewidth=2,        # Line width
    )

# Labels, legend, CMS style
ax.set_xlabel(r"$\Delta R(\tau_{1},\tau_{2})$")
ax.set_ylabel("Normalized Events")
ax.legend()
hep.cms.label(llabel=r"Simulation", rlabel="13.6 TeV", loc=0, ax=ax)

plt.savefig("dr_HAA4Tau_with_errors_set1.png")
plt.savefig("dr_HAA4Tau_with_errors_set1.pdf")