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
parser.add_argument('-i', '--infile', default='/eos/uscms/store/group/lpcml/rchudasa/MCGenerationRun3/HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_MLAnalyzer_miniAOD_bigProductionRe/250430_020739/0000/output_565.root', type=str, help='Input root file.')
parser.add_argument('-o', '--outdir', default='figures', type=str, help='Output image file dir.')
parser.add_argument('-d', '--decay', default='test', type=str, help='Decay name.')
parser.add_argument('-n', '--idx', default=10, type=int, help='Input root file index.')
args = parser.parse_args()

out_dir = args.outdir
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
    print(f"Directory '{out_dir}' created.")
else:
    print(f"Directory '{out_dir}' already exists.")

event_id = args.idx

def upsample_array(x, b0, b1):

    r, c = x.shape                                    # number of rows/columns
    rs, cs = x.strides                                # row/column strides
    x = as_strided(x, (r, b0, c, b1), (rs, 0, cs, 0)) # view as a larger 4D array

    return x.reshape(r*b0, c*b1)/(b0*b1)              # create new 2D array with same total occupancy 

def resample_EE(imgECAL, factor=2):

    # EE-
    imgEEm = imgECAL[:140-85] # EE- in the first 55 rows
    imgEEm = np.pad(imgEEm, ((1,0),(0,0)), 'constant', constant_values=0) # for even downsampling, zero pad 55 -> 56
    imgEEm_dn = block_reduce(imgEEm, block_size=(factor, factor), func=np.sum) # downsample by summing over [factor, factor] window
    imgEEm_dn_up = upsample_array(imgEEm_dn, factor, factor)/(factor*factor) # upsample will use same values so need to correct scale by factor**2
    imgECAL[:140-85] = imgEEm_dn_up[1:] ## replace the old EE- rows

    # EE+
    imgEEp = imgECAL[140+85:] # EE+ in the last 55 rows
    imgEEp = np.pad(imgEEp, ((0,1),(0,0)), 'constant', constant_values=0) # for even downsampling, zero pad 55 -> 56
    imgEEp_dn = block_reduce(imgEEp, block_size=(factor, factor), func=np.sum) # downsample by summing over [factor, factor] window
    imgEEp_dn_up = upsample_array(imgEEp_dn, factor, factor)/(factor*factor) # upsample will use same values so need to correct scale by factor*factor
    imgECAL[140+85:] = imgEEp_dn_up[:-1] # replace the old EE+ rows

    return imgECAL

def crop_jet(imgECAL, iphi, ieta, jet_shape=125):

    # NOTE: jet_shape here should correspond to the one used in RHAnalyzer
    off = jet_shape//2
    iphi = int(iphi*5 + 2) # 5 EB xtals per HB tower
    ieta = int(ieta*5 + 2) # 5 EB xtals per HB tower

    # Wrap-around on left side
    if iphi < off:
        diff = off-iphi
        img_crop = np.concatenate((imgECAL[:,ieta-off:ieta+off+1,-diff:],
                                   imgECAL[:,ieta-off:ieta+off+1,:iphi+off+1]), axis=-1)
    # Wrap-around on right side
    elif 360-iphi < off:
        diff = off - (360-iphi)
        img_crop = np.concatenate((imgECAL[:,ieta-off:ieta+off+1,iphi-off:],
                                   imgECAL[:,ieta-off:ieta+off+1,:diff+1]), axis=-1)
    # Nominal case
    else:
        img_crop = imgECAL[:,ieta-off:ieta+off+1,iphi-off:iphi+off+1]

    return img_crop

rhTreeStr =  uproot.open(args.infile) 
print(type(rhTreeStr))
rhTree = rhTreeStr['fevt/RHTree']
print(rhTree.keys())

def getImageArrays(rhTree, iEvt):
    ECAL_energy = np.array(rhTree['ECAL_energy'])[iEvt].reshape(280,360)
    ECAL_energy = resample_EE(ECAL_energy)
    HBHE_energy = np.array(rhTree['HBHE_energy'])[iEvt].reshape(56,72)
    HBHE_energy = upsample_array(HBHE_energy, 5, 5) # (280, 360)
    TracksAtECAL_pt    = np.array(rhTree['ECAL_tracksPt_atECALfixIP'])[iEvt].reshape(280,360)
    TracksAtECAL_dZSig = np.array(rhTree['ECAL_tracksDzSig_atECALfixIP'])[iEvt].reshape(280,360)
    TracksAtECAL_d0Sig = np.array(rhTree['ECAL_tracksD0Sig_atECALfixIP'])[iEvt].reshape(280,360)
    PixAtEcal_1        = np.array(rhTree['BPIX_layer1_ECAL_atPV'])[iEvt].reshape(280,360)
    PixAtEcal_2        = np.array(rhTree['BPIX_layer2_ECAL_atPV'])[iEvt].reshape(280,360)
    PixAtEcal_3        = np.array(rhTree['BPIX_layer3_ECAL_atPV'])[iEvt].reshape(280,360)
    PixAtEcal_4        = np.array(rhTree['BPIX_layer4_ECAL_atPV'])[iEvt].reshape(280,360)
    TibAtEcal_1        = np.array(rhTree['TIB_layer1_ECAL_atPV'])[iEvt].reshape(280,360)
    TibAtEcal_2        = np.array(rhTree['TIB_layer2_ECAL_atPV'])[iEvt].reshape(280,360)
    TobAtEcal_1        = np.array(rhTree['TOB_layer1_ECAL_atPV'])[iEvt].reshape(280,360)
    TobAtEcal_2        = np.array(rhTree['TOB_layer2_ECAL_atPV'])[iEvt].reshape(280,360)
    X_CMSII            = np.stack([TracksAtECAL_pt, TracksAtECAL_dZSig, TracksAtECAL_d0Sig, ECAL_energy, HBHE_energy, PixAtEcal_1, PixAtEcal_2, PixAtEcal_3, PixAtEcal_4, TibAtEcal_1, TibAtEcal_2, TobAtEcal_1, TobAtEcal_2], axis=0) # (13, 280, 360)

    return X_CMSII


def plotImage(img, eve, pt):
    mins = [0.001]*len(img)
    maxs =[]
    print("img shape:", len(img))
    for i in range(len(img)):
        #mins.append(img[i].min())
        maxs.append(img[i].max())
    fig, ax = plt.subplots(dpi=300)
    #if maxs[0]  > 0 : plt.imshow(img[0], cmap='Oranges', norm=LogNorm(vmin=mins[0], vmax=maxs[0]),  alpha=0.9)
    #if maxs[1]  > 0 : plt.imshow(img[1], cmap='Blues',   norm=LogNorm(vmin=mins[1], vmax=maxs[1]),  alpha=0.9)
    #if maxs[2]  > 0 : plt.imshow(img[2], cmap='Greys',   norm=LogNorm(vmin=mins[2], vmax=maxs[2]),  alpha=0.9)
    #if maxs[3]  > 0 : plt.imshow(img[3], cmap='Greens',  norm=LogNorm(vmin=mins[3], vmax=maxs[3]),  alpha=0.9)       
    if maxs[4]  > 0 : plt.imshow(img[4], cmap='Blues',   norm=LogNorm(vmin=mins[4], vmax=maxs[4]),  alpha=0.4)   
    #if maxs[5]  > 0 : plt.imshow(img[5], cmap='Purples', norm=LogNorm(vmin=mins[5], vmax=maxs[5]),  alpha=0.9)   
    #if maxs[10] > 0 : plt.imshow(img[10], cmap= 'pink',  norm=LogNorm(vmin=mins[10],vmax=maxs[10]), alpha=0.9)
    hep.cms.label(llabel=r"Simulation ", rlabel=f"13.6 TeV", loc=0, ax=ax)
    plt.xlabel(r"$i\varphi$") #28, 30
    plt.ylabel(r"$i\eta$") #28, 30
    #LEGEND
    colors = {0:'orange',1:'blue',2:'grey',3:'green',4:'lightblue',5:'purple',6:'pink'}
    labels = {0:'Track pT',1:'dz_sig',2:'d0_sig',3:'ECAL',4:'HCAL',5:'BPix L1',6:'BStrip L1'}
    patches =[mpatches.Patch(color=colors[i],label=labels[i]) for i in colors]
    plt.legend(handles=patches, bbox_to_anchor=(1.005, 1), loc=2, borderaxespad=0.,fontsize=10 )
    plt.tight_layout()

    #plt.savefig(f"{out_dir}/combined_image_event_{eve}_{pt}.pdf",facecolor='w',dpi=300,)
    plt.savefig(f"{out_dir}/onlyECALHCAL_image_event_{eve}_{pt}.png",facecolor='w',dpi=300,)

    plt.show()

start =time.time()
imageArray = getImageArrays(rhTree, event_id)
stop = time.time()
print(imageArray.shape, "It took ", stop-start)
img =imageArray
print("Jet pt:", np.array(rhTree['jetPt'])[event_id], "iphi:", np.array(rhTree['jetSeed_iphi'])[event_id], "ieta:", np.array(rhTree['jetSeed_ieta'])[event_id])
plotImage(img, event_id, np.array(rhTree['jetPt'])[event_id])  


