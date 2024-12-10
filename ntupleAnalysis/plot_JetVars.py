import awkward as ak
import hist
import matplotlib.pyplot as plt
import numpy as np
import numba
from coffea.dataset_tools import apply_to_fileset
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea.nanoevents.methods import candidate
from coffea import processor

import dask
import dask_awkward as dak
import hist.dask as hda
import uproot
import os
import time
path = '/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/withTrigger/HToAATo4Tau_M_3p7_pythia8_2018UL_AOD/3p7_RHEvent-Trigger_ntuples_v2/240813_103752/0000/'
files = os.listdir(path)
root_files = [path+f+':fevt/RHTree' for f in files if f.endswith('.root')]

#file1 = 'output_1.root:fevt/RHTree'
tic = time.time()
ff = uproot.dask(root_files)['jetPt']
#ff = uproot.dask([file1,file1])['jetPt']
print(type(ff))
q2_hist = (
    hda.Hist.new.Reg(100, 0, 200, name="ptj", label="Jet $p_{T}$ [GeV]")
    .Double()
    .fill(ak.flatten(ff))
)
fig, ax = plt.subplots()
q2_hist.compute().plot1d(ax=ax,flow="none", yerr=False)
dak.necessary_columns(q2_hist)
toc=time.time()
print("It took", toc-tic)
plt.show()

