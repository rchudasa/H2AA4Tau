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
parser.add_argument('-o', '--outdir', default='figuresRH', type=str, help='Output image file dir.')
parser.add_argument('-d', '--decay', default='test', type=str, help='Decay name.')
parser.add_argument('-n', '--idx', default=1, type=int, help='Input root file index.')
args = parser.parse_args()

args.infile = '/afs/cern.ch/work/r/rchudasa/private/TauClassification/run3/CMSSW_13_0_14/src/MLAnalyzerRun3/HAA4tau_M3p7_allRH_sameNtuples_reducedEgammaHCALRH_numEvent10000.root'
out_dir = args.outdir
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
    print(f"Directory '{out_dir}' created.")
else:
    print(f"Directory '{out_dir}' already exists.")

event_id = args.idx


def getECALHits(inputFile, iEvt):
    rhTreeStr =  uproot.open(inputFile) 
    rhTree = rhTreeStr['fevt/RHTree']
    #print(inputFile, "event id:", rhTree['eventId'], "run id:", rhTree['runId'], "lumi id:", rhTree['lumiId'][iEvt])
    #print(inputFile)
    print(np.array(rhTree['eventId'])[iEvt], "run id:", np.array(rhTree['runId'])[iEvt], "lumi id:", np.array(rhTree['lumiId'])[iEvt])
    
    ECAL_energy_RAW = np.array(rhTree['ECAL_energy'])[iEvt].reshape(280,360)
    ECAL_energy_AOD = np.array(rhTree['ECAL_energy_AOD'])[iEvt].reshape(280,360)
    ECAL_energy_miniAOD = np.array(rhTree['ECAL_energy_miniAOD'])[iEvt].reshape(280,360)
    print("ECAL RAW", ECAL_energy_RAW.max(), " AOD", ECAL_energy_AOD.max(), "miniAOD", ECAL_energy_miniAOD.max())
    return ECAL_energy_RAW, ECAL_energy_AOD, ECAL_energy_miniAOD


def getHCALHits(inputFile, iEvt):
    rhTreeStr =  uproot.open(inputFile) 
    print("================================================", iEvt)
    rhTree = rhTreeStr['fevt/RHTree']
    #print(inputFile, "event id:", rhTree['eventId'], "run id:", rhTree['runId'], "lumi id:", rhTree['lumiId'][iEvt])
    #print(inputFile)
    print(np.array(rhTree['eventId'])[iEvt], "run id:", np.array(rhTree['runId'])[iEvt], "lumi id:", np.array(rhTree['lumiId'])[iEvt])
    
    HBHE_energy_RAW = np.array(rhTree['HBHE_energy'])[iEvt].reshape(56,72)
    HBHE_energy_AOD = np.array(rhTree['HBHE_energy_AOD'])[iEvt].reshape(56,72)
    HBHE_energy_miniAOD = np.array(rhTree['HBHE_energy_miniAOD'])[iEvt].reshape(56,72)
    print("HCAL RAW", HBHE_energy_RAW.max(), " AOD", HBHE_energy_AOD.max(), "miniAOD", HBHE_energy_miniAOD.max())
    return HBHE_energy_RAW, HBHE_energy_AOD, HBHE_energy_miniAOD

def plotEcalHcalRH(rawEImg,aodEImg,maodEImg, rawHImg,aodHImg,maodHImg, iEvt):
    fig, ax = plt.subplots(2,3, figsize=(20,20),dpi=300)
    ax = ax.flatten()
    plt.subplots_adjust(wspace=0.5)  
    print("rawEImg type", type(rawEImg))
    im0 = ax[0].imshow(rawEImg, cmap='Greens',norm=LogNorm(vmin=0.001, vmax=rawEImg.max()), alpha=0.9)
    ax[0].set_title('RAW')
    im1 = ax[1].imshow(aodEImg, cmap='Greens',norm=LogNorm(vmin=0.001, vmax=aodEImg.max()), alpha=0.9)
    ax[1].set_title('AOD')
    im2 = ax[2].imshow(maodEImg, cmap='Greens',norm=LogNorm(vmin=0.001, vmax=maodEImg.max()), alpha=0.9) 
    ax[2].set_title('miniAOD')

    im3 = ax[3].imshow(rawHImg, cmap='Blues',norm=LogNorm(vmin=0.001, vmax=rawHImg.max()), alpha=0.9)
    ax[3].set_title('RAW')
    im4 = ax[4].imshow(aodHImg, cmap='Blues',norm=LogNorm(vmin=0.001, vmax=aodHImg.max()), alpha=0.9)
    ax[4].set_title('AOD')
    im5 = ax[5].imshow(maodHImg, cmap='Blues',norm=LogNorm(vmin=0.001, vmax=maodHImg.max()), alpha=0.9)
    ax[5].set_title('miniAOD')
    for a in ax:
        a.set_xlabel(r"$i\varphi$")
        a.set_ylabel(r"$i\eta$")

    fig.colorbar(im0, ax=ax[0])
    fig.colorbar(im1, ax=ax[1])
    fig.colorbar(im2, ax=ax[2])
    fig.colorbar(im3, ax=ax[3])
    fig.colorbar(im4, ax=ax[4])
    fig.colorbar(im5, ax=ax[5])
    #hep.cms.label(llabel=r"Simulation ", rlabel=f"13.6 TeV", loc=0, ax=ax)
    plt.savefig(f"{out_dir}/ecalHcal_event_{iEvt}.pdf",facecolor='w',dpi=300,)


def plotRecHits(rawImg,aodImg,maodImg, iEvt, det): 
    fig, ax = plt.subplots(1,3, figsize=(20,8),dpi=300)
    plt.subplots_adjust(wspace=0.5)  

    im0 = im1 = im2 = None

    if rawImg.max()  > 0: im0 = ax[0].imshow(rawImg, cmap='Greens',norm=LogNorm(vmin=0.001, vmax=rawImg.max()), alpha=0.9)
    ax[0].set_title('RAW')
    if aodImg.max() > 0: im1 = ax[1].imshow(aodImg, cmap='Greens',norm=LogNorm(vmin=0.001, vmax=aodImg.max()), alpha=0.9)
    ax[1].set_title('AOD')
    if maodImg.max() > 0: im2 = ax[2].imshow(maodImg, cmap='Greens',norm=LogNorm(vmin=0.001, vmax=maodImg.max()), alpha=0.9) 
    ax[2].set_title('miniAOD')
    for a in ax:
        a.set_xlabel(r"$i\varphi$")
        a.set_ylabel(r"$i\eta$")
    if im0: fig.colorbar(im0, ax=ax[0])
    if im1: fig.colorbar(im1, ax=ax[1])
    if im2: fig.colorbar(im2, ax=ax[2])
    # plt.colorbar(label='Energy (GeV)')
    # hep.cms.label(llabel=r"Simulation ", rlabel=f"13.6 TeV", loc=0, ax=ax)
    # plt.xlabel(r"$i\varphi$") #28, 30
    # plt.ylabel(r"$i\eta$") #28, 30
    #LEGEND
    #colors = {0:'orange',1:'blue',2:'grey',3:'green',4:'lightblue',5:'purple',6:'pink'}
    #labels = {0:'Track pT',1:'dz_sig',2:'d0_sig',3:'ECAL',4:'HCAL',5:'BPix L1',6:'BStrip L1'}
    #patches =[mpatches.Patch(color=colors[i],label=labels[i]) for i in colors]
    #plt.legend(handles=patches, bbox_to_anchor=(1.005, 1), loc=2, borderaxespad=0.,fontsize=10 )
    #plt.tight_layout()

    plt.savefig(f"{out_dir}/{det}_image_event_{iEvt}.pdf",facecolor='w',dpi=300,)
    plt.savefig(f"{out_dir}/{det}_image_event_{iEvt}.png",facecolor='w',dpi=300,)
    plt.tight_layout()

    #plt.show()

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


    #plt.show()

start =time.time()

for i in range(1, 10):
    event_id = i
    if event_id < 7: continue
    #print("Event id:", event_id)
    rawImg, aodImg, maodImg = getHCALHits(args.infile, event_id)
    if aodImg.max() == 0.0 or maodImg.max() == 0.0:
        print("AOD or miniAOD has zero max energy")
        continue
    
    rawEImg, aodEImg, maodEImg = getECALHits(args.infile, event_id)
    print("RawImg", rawImg.shape, "AODImg", aodImg.shape, "maodImg", maodImg.shape)
    print("RawEImg", rawEImg.shape, "AODImg", aodEImg.shape, "maodImg", maodEImg.shape)
    plotEcalHcalRH(rawEImg, aodEImg, maodEImg, rawImg, aodImg, maodImg, event_id)
    break
stop = time.time()
print("Time taken:", stop-start)  