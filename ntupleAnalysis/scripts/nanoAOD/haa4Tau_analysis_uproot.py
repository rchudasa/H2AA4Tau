import os
import numpy as np
import awkward as ak
import dask
import dask_awkward as dak
import hist
import hist.dask as hda
import uproot
import matplotlib.pyplot as plt
import mplhep as hep
from collections import OrderedDict
import time

#print(dir(hda.Hist), type(hda.Hist))

print("-------------------------------")
# TAU SELECTION CRITERIA
tau_pt_min = 20.0
tau_eta_max = 2.3
tau_dz_max = 0.2

commonPath = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/"
file3p7 = uproot.open(commonPath+"HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_nanoAODSIM_hadronic/241001_054550/0000/step5_nanoAOD_coffea_75.root")    
file14 = uproot.open(commonPath+"HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/14_nanoAODSIM_hadronic/241001_054627/0000/step5_nanoAOD_coffea_65.root")
tree3p7 = file3p7["Events"]
tree14 = file14["Events"]

#taus = tree3p7.arrays(["Tau_pt", "Tau_eta", "Tau_phi", "Tau_charge", "Tau_dz"], how="zip", entry_stop=1000)

taus = tree3p7.arrays(["Tau_pt", "Tau_eta", "Tau_dz", "Tau_decayMode", "Tau_idDeepTau2017v2p1VSjet"], cut="nTau>=2")
tauPt = taus["Tau_pt"]
print(tauPt, type(tauPt))
tauPt = ak.flatten(tauPt)
print(tauPt, type(tauPt), ak.num(tauPt,axis=0))
#print(ak.sum(tauPt, axis=1))