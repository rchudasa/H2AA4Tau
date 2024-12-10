#datasets used
#WJets
#/WtoLNu-2Jets_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/Run3Summer23BPixNanoAODv12-130X_mcRun3_2023_realistic_postBPix_v6-v2/NANOAODSIM
#QCD
#/QCD_PT-15to7000_TuneCP5_13p6TeV_pythia8/Run3Summer23NanoAODv12-castor_130X_mcRun3_2023_realistic_v14-v1/NANOAODSIM
#DYToLL
#/DYto2L_M-50_TuneCP5_13p6TeV_pythia8/Run3Summer23BPixNanoAODv12-KeepSi_130X_mcRun3_2023_realistic_postBPix_v2-v3/NANOAODSIM
#GluGluHToTauTau
#/GluGluHToTauTau_M-125_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer23BPixNanoAODv12-130X_mcRun3_2023_realistic_postBPix_v2-v2/NANOAODSIM

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
import numpy as np
import matplotlib.pyplot as plt
import mplhep as hep


def contains_any_char(s, chars):
    return any(char in s for char in chars)

#from distributed import Client

#client = Client()

#def plotEfficiency(bins, numerator_hist, denominator_hist, triggerName):
def plotEfficiency(bins, numerator_hist, denominator_hist, numerator_histv2, denominator_histv2, triggerName):

    # Plot the efficiency
    # numerator_hist = numerator_hist.compute()
    # denominator_hist = denominator_hist.compute()
    print("Numerator hist---------------")
    print(numerator_hist.axes[0].centers) 
    print("denominator hist-------------")
    print(denominator_hist.axes[0].centers)
    eff = numerator_hist / denominator_hist
    print("Efficiency", eff)
    print(eff.values(), type(eff.values()))
    bin_centers = bins[:-1] + np.diff(bins) / 2
    eff_values = eff.values()[0]

    print("Bin centers", bin_centers)
    print("Efficiency values", eff_values)
 
    # Plot the efficiency
    # numerator_histv2 = numerator_histv2.compute()
    # denominator_histv2 = denominator_histv2.compute()
    print("Numerator hist type", type(numerator_histv2))
    effv2 = numerator_histv2 / denominator_histv2
    print("Efficiency", effv2)
    print(effv2.values(), type(effv2.values()))
    eff_valuesv2 = effv2.values()[0]

    # plt.figure(figsize=(8, 6))

    # # Plot efficiency with error bars (sqrt(N) approximation)
    plt.errorbar(bin_centers, eff_values, yerr=np.sqrt(eff_values*(1-eff_values)/denominator_hist.values()), fmt='o', color='blue', label='Mass 3.7 GeV')
    plt.errorbar(bin_centers, eff_valuesv2, yerr=np.sqrt(eff_valuesv2*(1-eff_valuesv2)/denominator_histv2.values()), fmt='o', color='red', label='Mass 14 GeV')
    # # Set x and y axis limits
    plt.xlim(0, 200)  # Set x-axis range from 0 to 100
    plt.ylim(0, 1.2)    # Set y-axis range from 0 to 1

    # Add grid lines on both x and y axes
    plt.grid(True)  # Adds grid lines to both x and y axes
    plt.grid(which='major', linestyle='-', linewidth='0.5', color='gray')  # Customize grid lines
    #plt.grid(which='minor', linestyle=':', linewidth='0.5', color='gray')  # Customize minor grid lines

    # Apply mplhep style
    hep.style.use("CMS")  # or another style like ATLAS, ALICE, LHCb
    hep.cms.label("Preliminary")

    # Add labels and title
    plt.xlabel(r'$p_T$ [GeV]')
    plt.ylabel('Efficiency')
    #plt.title('Efficiency vs $p_T$')

    # Show legend
    plt.legend()

    plt.savefig('figures_run3_triggerEff_check/'+triggerName+'.png')
    # Show plot
    #plt.show()
    plt.close()
    
class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        pass

    def process(self, events):

        bins = np.linspace(20, 200, 18)
        dataset = events.metadata["dataset"]
        dataset_axis = hist.axis.StrCategory(
            [], growth=True, name="dataset", label="Primary dataset"
        )
        pt_axis = hist.axis.Regular(bins.size -1 , bins.min(), bins.max(), name="pt", label=r"$p_{T,\mu}$ [GeV]")

        #print(events.fields)

        count = 0
        triggerBranches = []

        def contains_any_char(s, chars):
            return any(char in s for char in chars)


        for branch_name in events.fields:
            if (branch_name).startswith('HLT'):
                #remove_branch = ['Prescl','ZeroBias','Physics','Calibration','Random','Hcal','Beam']
                remove_branch = ['Prescl','Photon','ZeroBias','Physics','Calibration','Random','Hcal','Beam','Mu','Ele','Photon']
                #keep_branch = ['Jet','Tau', 'PFTau','PFHT','MET']
                keep_branch = ['HLT_PFHT280_QuadPFJet']
                #if 'HLT' in branch_name and 'Jet' in branch_name and not contains_any_char(branch_name,remove_branch):
                if contains_any_char(branch_name,keep_branch) and not contains_any_char(branch_name,remove_branch):
                #if not contains_any_char(branch_name,remove_branch):
                    count+=1
                    #if count==10:
                    #    break
                    triggerBranches.append(branch_name)
                    #print(branch_name)
        print(count)
        #print(triggerBranches)

        jetPt_Hist_dict ={'dataset':dataset}
        jetPt = events.Jet_pt[events.Jet_pt>20]
        denominator_hist = hda.hist.Hist(dataset_axis, pt_axis) 
        denominator_hist.fill(dataset=dataset, pt=ak.flatten(jetPt))
                
        jetPt_triggerPass_dict ={'dataset':dataset}
        jetPt_triggerPassHist_dict ={}

        efficiency = {}
        #triggerBranches = triggerBranches[0:3]
        for t in triggerBranches:
            print("************************ t", t)
            jetPt_triggerPass_dict [t] = jetPt[events[t]==1]
            jetPt_triggered = ak.sum(jetPt_triggerPass_dict[t])
            jetPt_all = ak.sum(jetPt)

            efficiency[t] = jetPt_triggered/jetPt_all

            #if efficiency.compute() > 0.8:
            #print("Efficiency greater than 0.8",efficiency)
            numerator_hist = hda.hist.Hist(dataset_axis, pt_axis) 
            numerator_hist.fill(dataset=dataset, pt=ak.flatten(jetPt_triggerPass_dict[t]))

            jetPt_triggerPassHist_dict[t] = numerator_hist

        
        return {
            "entries": ak.num(events,axis=0),
            "jetPt_triggerPass_dict" : jetPt_triggerPassHist_dict,
            "jetPt_notrigger" : denominator_hist,
            "Efficiency" : efficiency
        }

    def postprocess(self, accumulator):
        pass


def makeFileSet(path):
    files = os.listdir(path)
    root_files = [path+f+":Events" for f in files if f.endswith(".root")]
    root_set = set(root_files)
    return root_set



#path_3p7 = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_M3p7_Run3_2023/3p7_nanoAODSIM/240912_053704/0000/"
path_3p7 = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_nanoAODSIM_hadronic/241001_054550/0000/"
root_3p7_set = makeFileSet(path_3p7)
#path_8 = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_M14_Run3_2023/14_nanoAODSIM/240912_053625/0000/"
path_8 = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/14_nanoAODSIM_hadronic/241001_054627/0000/"
root_8_set = makeFileSet(path_8)

qcd_fileList = []

with open('QCD_PT-15to7000_TuneCP5_13p6TeV_pythia8_Run3Summer23NanoAODv12-castor_130X_mcRun3_2023_realistic_v14-v1_nanoAOD.txt','r') as file:
    for line in file:
        updated_line = 'root://xrootd-cms.infn.it/'+line.replace('\n','')+':Events'
        qcd_fileList.append(updated_line)

qcd_fileSet = set(qcd_fileList)

print(qcd_fileSet)

fileset = {
    "Mass3p7": {
        "files": root_3p7_set
    },
    "Mass8": {
        "files": root_8_set
        #"files": qcd_fileList
    }
    # "QCD":{
    #     "files":qcd_fileSet
    # }
}

print("File set 3p7", root_3p7_set)
print("File set 8", root_8_set)

to_compute = apply_to_fileset(
    MyProcessor(),
    fileset,
    schemaclass=BaseSchema,
)


dak.necessary_columns(to_compute)
import time
tstart = time.time()

(out,) = dask.compute(to_compute)
#print(out)

elapsed = time.time() - tstart
print(elapsed)

print("======================================================")
#print(out["Mass3p7"].items())

print("======================================================")
#print(out["Mass8"].items())

bins = np.linspace(20, 200, 18)

for key,value in out['Mass3p7']['Efficiency'].items():
   # if value > 0.5:
    print("Mass 3.7", key, value)
    plotEfficiency(bins,out['Mass3p7']['jetPt_triggerPass_dict'][key],out['Mass3p7']['jetPt_notrigger'],out['Mass8']['jetPt_triggerPass_dict'][key],out['Mass8']['jetPt_notrigger'],key)


