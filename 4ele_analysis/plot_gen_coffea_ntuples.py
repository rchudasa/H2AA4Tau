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

        dr_axis = hist.axis.Regular(50, 0, 0.1, name="dR_tau1_tau2", label="dR(ele1,ele2)")   
        dataset = events.metadata['dataset']
        #print("_______________________",type(dataset))

        h_tau1_tau2_dr = hda.hist.Hist(dataset_axis, dr_axis)

        h_tau1_tau2_dr.fill(dataset=dataset,dR_tau1_tau2=ak.flatten(events.dR_Ele1_Ele2))
        h_tau1_tau2_dr.fill(dataset=dataset,dR_tau1_tau2=ak.flatten(events.dR_Ele3_Ele4))

        pt_axis = hist.axis.Regular(50, 0, 100, name="pt", label="pt") 
 
        
        return {
            "entries": ak.num(events, axis=0),
            "tau1_tau2_dr": h_tau1_tau2_dr           
        }

    def postprocess(self, accumulator):
        pass

import os

fileset = {
    #  "0p01": {"files": {"GenInfo_only_H2AA4Ele_0p01GeV.root":"fevt/RHTree"}},
    # # "0p03": {"files": {"GenInfo_only_H2AA4Ele_0p03GeV.root":"fevt/RHTree"}},
    #  "0p05": {"files": {"GenInfo_only_H2AA4Ele_0p05GeV.root":"fevt/RHTree"}},
    #  "0p1": {"files": {"GenInfo_only_H2AA4Ele_0p1GeV.root":"fevt/RHTree"}},
    # #"0p12": {"files": {"GenInfo_only_H2AA4Ele_0p12GeV.root":"fevt/RHTree"}},
    # "0p15": {"files": {"GenInfo_only_H2AA4Ele_0p15GeV.root":"fevt/RHTree"}},
    # #"0p17": {"files": {"GenInfo_only_H2AA4Ele_0p17GeV.root":"fevt/RHTree"}},
    # "0p2": {"files": {"GenInfo_only_H2AA4Ele_0p2GeV.root":"fevt/RHTree"}},
    # "0p25": {"files": {"GenInfo_only_H2AA4Ele_0p25GeV.root":"fevt/RHTree"}},
    "0p3": {"files": {"GenInfo_only_H2AA4Ele_0p3GeV.root":"fevt/RHTree"}},
    "0p35": {"files": {"GenInfo_only_H2AA4Ele_0p35GeV.root":"fevt/RHTree"}},
    "0p4": {"files": {"GenInfo_only_H2AA4Ele_0p4GeV.root":"fevt/RHTree"}},
    #"0p45": {"files": {"GenInfo_only_H2AA4Ele_0p45GeV.root":"fevt/RHTree"}},
    "0p6": {"files": {"GenInfo_only_H2AA4Ele_0p6GeV.root":"fevt/RHTree"}},
    "0p8": {"files": {"GenInfo_only_H2AA4Ele_0p8GeV.root":"fevt/RHTree"}},
    "1p0": {"files": {"GenInfo_only_H2AA4Ele_1GeV.root":"fevt/RHTree"}},
    "1p2": {"files": {"GenInfo_only_H2AA4Ele_1p2GeV.root":"fevt/RHTree"}},
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
plt.ion()
fig, ax = plt.subplots()
scaled['tau1_tau2_dr'].plot1d(ax=ax, overlay="dataset")
ax.legend()
#ax.set_yscale("log")
ax.set_ylim(1, None)
plt.savefig('dr_0p3_1p2.png')

