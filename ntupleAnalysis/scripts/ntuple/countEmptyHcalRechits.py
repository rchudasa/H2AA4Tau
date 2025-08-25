import time
import uproot
import awkward as ak
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mplhep as hep
import os, glob
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import mplhep as hep
from numpy.lib.stride_tricks import as_strided
plt.style.use([hep.style.ROOT, hep.style.firamath])
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.colors import LogNorm, ListedColormap, LinearSegmentedColormap
import matplotlib.patches as mpatches
from scipy.stats import zscore
import statistics
# Define the CMS color scheme
cms_colors = [
    (0.00, '#FFFFFF'),  # White
    (0.33, '#005EB8'),  # Blue
    (0.66, '#FFDD00'),  # Yellow
    (1.00, '#FF0000')   # red
]

# Create the CMS colormap
cms_cmap = LinearSegmentedColormap.from_list('CMS', cms_colors)
from numpy.lib.stride_tricks import as_strided

import argparse
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-i', '--infile', default='HAA4tau_M3p7_AODRH_numEvent10.root', type=str, help='Input AOD root file.')
parser.add_argument('-o', '--outdir', default='figures', type=str, help='Output image file dir.')
parser.add_argument('-d', '--decay', default='test', type=str, help='Decay name.')
parser.add_argument('-n', '--idx', default=1, type=int, help='Input root file index.')
args = parser.parse_args()

#args.infile = '/afs/cern.ch/work/r/rchudasa/private/TauClassification/run3/CMSSW_13_0_14/src/MLAnalyzerRun3/HAA4tau_M3p7_allRH_sameNtuples_slimmedHCALRH_numEvent10000.root'
args.infile = '/afs/cern.ch/work/r/rchudasa/private/TauClassification/run3/CMSSW_13_0_14/src/MLAnalyzerRun3/HAA4tau_M3p7_allRH_sameNtuples_reducedEgammaHCALRH_numEvent10000.root'


out_dir = args.outdir
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
    print(f"Directory '{out_dir}' created.")
else:
    print(f"Directory '{out_dir}' already exists.")

event_id = args.idx

def getMaxMinEnrgy(inputFile):
    rhTree = uproot.open(inputFile)['fevt/RHTree']
    HBHE_energy = rhTree['HBHE_energy'].array(library="np")
    HBHE_energy_AOD = rhTree['HBHE_energy_AOD'].array(library="np")
    HBHE_energy_miniAOD = rhTree['HBHE_energy_miniAOD'].array(library="np")

    maxEnergyRaw = np.array([np.max(e) for e in HBHE_energy])
    maxEnergyAOD = np.array([np.max(e) for e in HBHE_energy_AOD])
    maxEnergyMiniAOD = np.array([np.max(e) for e in HBHE_energy_miniAOD])

    minEnergyRaw = np.array([np.min(e) for e in HBHE_energy])
    minEnergyAOD = np.array([np.min(e) for e in HBHE_energy_AOD])
    minEnergyMiniAOD = np.array([np.min(e) for e in HBHE_energy_miniAOD])
    return maxEnergyRaw, maxEnergyAOD, maxEnergyMiniAOD, minEnergyRaw, minEnergyAOD, minEnergyMiniAOD

def countZeroHCALHits(inputFile, varName):
    rhTreeStr =  uproot.open(inputFile) 
    print("================================================")
    rhTree = rhTreeStr['fevt/RHTree']
    print(inputFile)
   
    HBHE_energy = rhTree[varName].array(library="np")
    maxEnergy = np.array([np.max(e) for e in HBHE_energy])
    print(maxEnergy, np.count_nonzero(maxEnergy == 0.0))
    # count = 0
    # for i in range(0,HBHE_energy.shape[0]):
    #     HBHE_energy[i] = np.array(rhTree[varName])[i].reshape(56,72)
    #     if np.all(HBHE_energy[i] == 0):
    #         count += 1
    # print(f"{varName} there are {count} events with zero energy")  


    
   





def compare1d(q1, q2, q3):
    fig, ax = plt.subplots(dpi=300)
    bins = np.linspace(0, 500, 500)
    #ifOutlier(q1, 'AOD')
    #ifOutlier(q2, 'RAW')
    plt.hist(q1, bins=bins, label='RAW', color='blue', histtype='step', linewidth=2, linestyle='--')
    plt.hist(q2, bins=bins, label='AOD', color='red', histtype='step', linewidth=1, linestyle='--')
    plt.hist(q3, bins=bins, label='miniAOD', color='green', histtype='step', linewidth=2)

    hep.cms.label(llabel=r"Simulation ", rlabel=f"13.6 TeV", loc=0, ax=ax)
    plt.yscale('log')
    plt.xlabel("HCAL rechit energy") #28, 30
    plt.ylabel("") #28, 30
    plt.legend(loc='best')

    plt.savefig(f"{out_dir}/MaxEnergy_HCAL_1d.pdf",facecolor='w',dpi=300)
    plt.savefig(f"{out_dir}/MaxEnergy_HCAL_1d.pdf",facecolor='w',dpi=300)

    #plt.show()


start =time.time()
maxEnergyRaw, maxEnergyAOD, maxEnergyMiniAOD, minEnergyRaw, minEnergyAOD, minEnergyMiniAOD = getMaxMinEnrgy(args.infile)
print("Max max energy raw:", maxEnergyRaw.shape, maxEnergyRaw.max())
print("Max max energy AOD:", maxEnergyAOD.shape, maxEnergyAOD.max())
print("Max max energy miniAOD:", maxEnergyMiniAOD.shape, maxEnergyMiniAOD.max())
print("Min energy raw:", minEnergyRaw.shape, minEnergyRaw.min())
#compare1d(maxEnergyRaw, maxEnergyAOD, maxEnergyMiniAOD)
countZeroHCALHits(args.infile, 'HBHE_energy')
countZeroHCALHits(args.infile, 'HBHE_energy_AOD')
countZeroHCALHits(args.infile, 'HBHE_energy_miniAOD')
#plotJetPt(args.infile)
#aodArray = getHCALHits(args.aodinfile, event_id)
#rawArray = getHCALHits(args.rawinfile, event_id)
#print("AOD shape:", aodArray.shape)
#print("RAW shape:", rawArray.shape)

stop = time.time()

# plotImage(aodArray, event_id, jetPtAOD, "AOD")
# plotImage(rawArray, event_id, jetPtRAW, "RAW")
#compare1d(aod1d, raw1d, event_id)
print("Time taken:", stop-start)  