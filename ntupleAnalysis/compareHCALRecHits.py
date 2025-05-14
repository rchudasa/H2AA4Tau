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
from skimage.measure import block_reduce
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
#parser.add_argument('-i', '--infile', default='/eos/uscms/store/group/lpcml/rchudasa/MCGenerationRun3/HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_MLAnalyzer_miniAOD_bigProductionRe/250430_020739/0000/output_565.root', type=str, help='Input root file.')
parser.add_argument('-i', '--aodinfile', default='HAA4tau_M3p7_AODRH_numEvent10.root', type=str, help='Input AOD root file.')
parser.add_argument('-r', '--rawinfile', default='HAA4tau_M3p7_RAWRH_all_numEvent10.root', type=str, help='Input RAW root file.')
parser.add_argument('-o', '--outdir', default='figures', type=str, help='Output image file dir.')
parser.add_argument('-d', '--decay', default='test', type=str, help='Decay name.')
parser.add_argument('-n', '--idx', default=1, type=int, help='Input root file index.')
args = parser.parse_args()

out_dir = args.outdir
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
    print(f"Directory '{out_dir}' created.")
else:
    print(f"Directory '{out_dir}' already exists.")

event_id = args.idx

def getHCALHits(inputFile, iEvt):
    rhTreeStr =  uproot.open(inputFile) 
    print("================================================")
    rhTree = rhTreeStr['fevt/RHTree']
    #print(inputFile, "event id:", rhTree['eventId'], "run id:", rhTree['runId'], "lumi id:", rhTree['lumiId'][iEvt])
    print(inputFile)
    print(np.array(rhTree['eventId'])[iEvt], "run id:", np.array(rhTree['runId'])[iEvt], "lumi id:", np.array(rhTree['lumiId'])[iEvt])
    
    HBHE_energy = np.array(rhTree['HBHE_energy'])[iEvt].reshape(56,72)
    print("HBHE non zero:", HBHE_energy[HBHE_energy>0].shape)
    #HBHE_energy = HBHE_energy[HBHE_energy<3.0]
    HBHE_energy1D = np.array(rhTree['HBHE_energy'])[iEvt]
    jetPt = np.array(rhTree['jetPt'])[event_id]
    print("Jet pt:", jetPt, "iphi:", np.array(rhTree['jetSeed_iphi'])[event_id], "ieta:", np.array(rhTree['jetSeed_ieta'])[event_id])
    print("Maximum BHE_energy:", HBHE_energy.max(), " minimum:", HBHE_energy.min())
    return HBHE_energy, HBHE_energy1D, jetPt

def plotImage(img, eve, pt, dataTier):
    fig, ax = plt.subplots(dpi=300)
    plt.imshow(img, cmap='Blues')   
    plt.colorbar(label='Energy (GeV)')
    hep.cms.label(llabel=r"Simulation ", rlabel=f"13.6 TeV", loc=0, ax=ax)
    plt.xlabel(r"$i\varphi$") #28, 30
    plt.ylabel(r"$i\eta$") #28, 30
    #LEGEND
    #colors = {0:'orange',1:'blue',2:'grey',3:'green',4:'lightblue',5:'purple',6:'pink'}
    #labels = {0:'Track pT',1:'dz_sig',2:'d0_sig',3:'ECAL',4:'HCAL',5:'BPix L1',6:'BStrip L1'}
    #patches =[mpatches.Patch(color=colors[i],label=labels[i]) for i in colors]
    #plt.legend(handles=patches, bbox_to_anchor=(1.005, 1), loc=2, borderaxespad=0.,fontsize=10 )
    #plt.tight_layout()

    plt.savefig(f"{out_dir}/onlyHCAL_image_event_{eve}_{dataTier}.pdf",facecolor='w',dpi=300,)
    plt.savefig(f"{out_dir}/onlyHCAL_image_event_{eve}_{dataTier}.png",facecolor='w',dpi=300,)

    plt.show()

def ifOutlier(data, dataTier):
    Q1 = np.percentile(data, 25)
    Q3 = np.percentile(data, 75)
    IQR = Q3 - Q1
    # Define outlier bounds
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    # Find outliers
    outliers = data[(data < lower_bound) | (data > upper_bound)]
    print(np.average(data), np.std(data), statistics.median(data))
    #print("Outliers:", outliers)
    fig, ax = plt.subplots(dpi=300)
    plt.boxplot(data, vert=False, patch_artist=True)
    plt.savefig(f"{out_dir}/HCAL_boxplot_{dataTier}.pdf",facecolor='w',dpi=300)
    return outliers

def compare1d(q1, q2, eve):
    fig, ax = plt.subplots(dpi=300)
    bins = np.linspace(0, 200, 100)
    #ifOutlier(q1, 'AOD')
    #ifOutlier(q2, 'RAW')
    plt.hist(q1, bins=bins, label='AOD', color='blue', histtype='step', linewidth=2)
    plt.hist(q2, bins=bins, label='RAW', color='red', histtype='step', linewidth=2)
    hep.cms.label(llabel=r"Simulation ", rlabel=f"13.6 TeV", loc=0, ax=ax)
    plt.yscale('log')
    plt.xlabel("HCAL rechit energy") #28, 30
    plt.ylabel("") #28, 30
    plt.legend(loc='best')

    plt.savefig(f"{out_dir}/HCAL_1d_{eve}.pdf",facecolor='w',dpi=300)
    plt.savefig(f"{out_dir}/HCAL_1d_{eve}.pdf",facecolor='w',dpi=300)

    plt.show()

start =time.time()
aodArray, aod1d, jetPtAOD = getHCALHits(args.aodinfile, event_id)
rawArray, raw1d, jetPtRAW = getHCALHits(args.rawinfile, event_id)
print("AOD shape:", aodArray.shape)
print("RAW shape:", rawArray.shape)
stop = time.time()

#print("Jet pt:", np.array(rawTree['jetPt'])[event_id], "iphi:", np.array(rawTree['jetSeed_iphi'])[event_id], "ieta:", np.array(rawTree['jetSeed_ieta'])[event_id])
plotImage(aodArray, event_id, jetPtAOD, "AOD")
plotImage(rawArray, event_id, jetPtRAW, "RAW")
compare1d(aod1d, raw1d, event_id)
print("Time taken:", stop-start)  


