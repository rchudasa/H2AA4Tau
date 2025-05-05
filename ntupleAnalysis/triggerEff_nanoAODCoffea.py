#datasets used
#WJets
#/WtoLNu-2Jets_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/Run3Summer23BPixNanoAODv12-130X_mcRun3_2023_realistic_postBPix_v6-v2/NANOAODSIM
#QCD
#/QCD_PT-15to7000_TuneCP5_13p6TeV_pythia8/Run3Summer23NanoAODv12-castor_130X_mcRun3_2023_realistic_v14-v1/NANOAODSIM
#DYToLL
#/DYto2L_M-50_TuneCP5_13p6TeV_pythia8/Run3Summer23BPixNanoAODv12-KeepSi_130X_mcRun3_2023_realistic_postBPix_v2-v3/NANOAODSIM
#GluGluHToTauTau
#/GluGluHToTauTau_M-125_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer23BPixNanoAODv12-130X_mcRun3_2023_realistic_postBPix_v2-v2/NANOAODSIM
import csv
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

def plotEfficiency(bins, datasets_dict, triggerName):
    bin_centers = bins[:-1] + np.diff(bins) / 2
    
    plt.figure(figsize=(8, 6))
    
    colors = ['blue', 'red', 'green', 'magenta', 'orange', 'cyan']
    styles = ['o','^','s','v','>','<']
    print("============================================================")
    print("dataset dict items==========================================")
    #print(datasets_dict.items())
    
    for idx, (dataset_name, histograms) in enumerate(datasets_dict.items()):
        numerator_hist, denominator_hist = histograms
            
        eff = numerator_hist / denominator_hist
        eff_values = eff.values()[0]
        
        #print(eff_values)
        plt.errorbar(
            bin_centers, eff_values, 
            yerr=np.sqrt(eff_values * (1 - eff_values) / denominator_hist.values()), 
            fmt=styles[idx], color=colors[idx], 
            label=f'{dataset_name}'
        )
    
    # Set plot limits, labels, and grid
    plt.xlim(0, 200)
    plt.ylim(0, 0.2)
    plt.grid(which='major', linestyle='-', linewidth='0.5', color='gray')
    hep.style.use("CMS")
    hep.cms.label("Preliminary")
    
    plt.xlabel(r'$p_T$ [GeV]')
    plt.ylabel('Efficiency')
    
    # Add a legend and save the plot
    plt.text(150, 1.1, f"Trigger: {triggerName}", fontsize=12, ha='right', color='black')
    plt.legend(prop={'size': 10}, loc='best', frameon=False)
    plt.savefig(f'figures_run3_triggerEff_check/{triggerName}.png')
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
        binary_axis = hist.axis.Regular(2,0,2,name='trigger', label='Passed')
        #print(events.fields)

        count = 0
        triggerBranches = []

        def contains_any_char(s, chars):
            return any(char in s for char in chars)

        with open('scouting_VBFdata_trigger_eff_14.csv',mode='w', newline='') as file:
            writer = csv.writer(file)
            for branch_name in events.fields:
                if (branch_name).startswith('DST'):
                #if (branch_name).startswith('HLT'):
                    #remove_branch = ['Prescl','ZeroBias','Physics','Calibration','Random','Hcal','Beam']
                    #remove_branch = ['Prescl','Photon','ZeroBias','Physics','Calibration','Random','Hcal','Beam']
                    remove_branch = ['Prescl']
                    #keep_branch = ['HLT_PFHT280_QuadPFJet','DoubleMediumDeepTauPFTau']
                    keep_branch = ['HLT_AK8PFJet400_MassSD30']

                    #if 'HLT' in branch_name and 'Jet' in branch_name and not contains_any_char(branch_name,remove_branch):
                    #if contains_any_char(branch_name,keep_branch):
                    if not contains_any_char(branch_name,remove_branch):
                        count+=1
                        #if count==10:
                        #    break
                        triggerBranches.append(branch_name)
                        print(branch_name)
            print(count)
            print(triggerBranches)

            for t in triggerBranches:
                print(t, type(events[t]), ak.sum(events[t].compute()))
                writer.writerow([t, ak.sum(events[t].compute())])
        # jetPt_Hist_dict ={'dataset':dataset}
        # jetPt = events.Jet_pt[events.Jet_pt>20]
        # denominator_hist = hda.hist.Hist(dataset_axis, pt_axis) 
        # denominator_hist.fill(dataset=dataset, pt=ak.flatten(jetPt))
                
        # jetPt_triggerPass_dict ={'dataset':dataset}
        # jetPt_triggerPassHist_dict ={}

        # efficiency = {}
        # triggerEventEff = {}
        # triggerEventHist_dict = {}
        # triggerBranches = triggerBranches[0:3]
        # for t in triggerBranches:
        #     print("************************ t", t, type(events[t]))
        #     jetPt_triggerPass_dict [t] = jetPt[events[t]==1]
        #     jetPt_triggered = ak.sum(jetPt_triggerPass_dict[t])
        #     jetPt_all = ak.sum(jetPt)

        #     efficiency[t] = jetPt_triggered/jetPt_all
        #     triggerEventEff[t] = ak.sum(events[t]==1)/ak.sum(events[t])
            

        #     #if efficiency.compute() > 0.8:
        #     #print("Efficiency greater than 0.8",efficiency)
        #     numerator_hist = hda.hist.Hist(dataset_axis, pt_axis) 
        #     numerator_hist.fill(dataset=dataset, pt=ak.flatten(jetPt_triggerPass_dict[t]))

        #     jetPt_triggerPassHist_dict[t] = numerator_hist

        #     trigger_hist = hda.hist.Hist(dataset_axis,binary_axis)
        #     print(ak.flatten(events[t].compute()))
        #     #triggerEventHist_dict[t] = trigger_hist.fill(dataset=dataset, trigger=ak.flatten(events[t]) )
        
        return {
            "entries": ak.num(events,axis=0),
            # "jetPt_triggerPass_dict" : jetPt_triggerPassHist_dict,
            # "jetPt_notrigger" : denominator_hist,
            # "Efficiency" : efficiency,
            # "Event_efficiency":triggerEventEff
        }
        

    def postprocess(self, accumulator):
        pass


def makeFileSet(path):
    files = os.listdir(path)
    root_files = [path+f+":Events" for f in files if f.endswith(".root")]
    root_set = set(root_files)
    return root_set


# paths = {
#     "HAA4Tau_3p7" : "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_nanoAODSIM_hadronic/241001_054550/0000/",
#     #"HAA4Tau_14" : "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/14_nanoAODSIM_hadronic/241001_054627/0000/",
#     #"QCD" : "/eos/cms/store/group/phys_diffraction/rchudasa/Run3Summer23NanoAODv12/QCD_PT-15to7000_TuneCP5_13p6TeV_pythia8/NANOAODSIM/castor_130X_mcRun3_2023_realistic_v14-v1/2560000/",
#     #"DYLL" :"/eos/cms/store/group/phys_diffraction/rchudasa/Run3Summer23BPixNanoAODv12/DYto2L_M-50_TuneCP5_13p6TeV_pythia8/NANOAODSIM/KeepSi_130X_mcRun3_2023_realistic_postBPix_v2-v3/2560000/",
#     #"WJets":"/eos/cms/store/mc/Run3Summer23BPixNanoAODv12/WtoLNu-2Jets_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_postBPix_v6-v2/2530000/",
#     #"HTauTau":"/eos/cms/store/group/phys_diffraction/rchudasa/Run3Summer23BPixNanoAODv12/GluGluHToTauTau_M-125_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_postBPix_v2-v2/2560000/"
# }

# fileset = {dataset:{"files":makeFileSet(path)} for dataset,path in paths.items()}

#commonPath = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_nanoAODSIM_hadronic/241001_054550/0000/"
#commonPath = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/14_nanoAODSIM_hadronic/241001_054627/0000/"

commonPath = "/eos/uscms/store/group/lpcml/rchudasa/MCGenerationRun3/"

fileset = {
    #"3p7": {"files": {commonPath+"nanoAOD_3p7.root":"Events"}},
    #"14" : {"files": {commonPath+"nanoAOD_14.root":"Events"}}
    #"3p7": {"files": {commonPath+"NANOAOD_VBFH_HToAATo4Tau_M3p7_all.root":"Events"}},
    "14" : {"files": {commonPath+"NANOAOD_VBFH_HToAATo4Tau_M14.root":"Events"}}
}

import time
tstart = time.time()


to_compute = apply_to_fileset(
    MyProcessor(),
    fileset,
    schemaclass=BaseSchema,
)


dak.necessary_columns(to_compute)
(out,) = dask.compute(to_compute)
#print(out)

elapsed = time.time() - tstart
print(elapsed)

#print("OUT KEYS", out['HAA4Tau_3p7']['Efficiency'].keys())
print("======================================================")
#print(out["Mass3p7"].items())

print("======================================================")
#print(out["Mass8"].items())

bins = np.linspace(20, 200, 18)

# start = time.time()
# # Loop through triggers and plot efficiencies for each
# for trigger_name,eff_value in out["HAA4Tau_3p7"]["Efficiency"].items():
#     #if eff_value > 0.01:
#     # if eff_value < 0.1:
#     #     continue
#     datasets_dict = {}
#     for dataset_name in paths.keys():
#         print("Dataset:",dataset_name, " Trigger name:",trigger_name, " Efficiency:", out[dataset_name]["Efficiency"][trigger_name], "Event efficiency",out[dataset_name]["Event_efficiency"][trigger_name] )
#         if trigger_name in out[dataset_name]["jetPt_triggerPass_dict"]:
#             datasets_dict[dataset_name] = (
#                 out[dataset_name]["jetPt_triggerPass_dict"][trigger_name],
#                 out[dataset_name]["jetPt_notrigger"],
#             )

#     # Only plot if trigger is relevant for this dataset
#     if datasets_dict:
#         plotEfficiency(bins, datasets_dict, trigger_name)
# end = time.time()
# print("Plotting took", end-start)
