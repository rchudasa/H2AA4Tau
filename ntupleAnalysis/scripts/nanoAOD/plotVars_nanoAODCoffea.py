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

    plt.savefig('variables/'+triggerName+'.png')
    # Show plot
    #plt.show()
    plt.close()
    
class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        pass

    def process(self, events):

        bins = np.linspace(0, 200, 200)
        dataset = events.metadata["dataset"]
        dataset_axis = hist.axis.StrCategory(
            [], growth=True, name="dataset", label="Primary dataset"
        )
        pt_axis = hist.axis.Regular(bins.size -1 , bins.min(), bins.max(), name="pt", label=r"$p_{T}$ [GeV]")

        h_reco_tauPt = hda.hist.Hist(
            dataset_axis, pt_axis, storage="weight", label="Counts"
        )

        h_reco_jetPt = hda.hist.Hist(
            dataset_axis, pt_axis, storage="weight", label="Counts"
        )
        
        h_gen_tauPt = hda.hist.Hist(
            dataset_axis, pt_axis, storage="weight", label="Counts"
        )


        for e in events.fields:
            if e.startswith('Gen'):
                print(e)

        count = 0
        triggerBranches = []

        def contains_any_char(s, chars):
            return any(char in s for char in chars)

        genHiggs_pt  = events.GenPart_pt[events.GenPart_pdgId==35]
        genHiggs_mass = events.GenPart_mass[events.GenPart_pdgId==35]
        genA_mass = events.GenPart_mass[events.GenPart_pdgId==25]
        genA_pt = events.GenPart_pt[events.GenPart_pdgId==25]

        t = 'HLT_PFHT280_QuadPFJet30'
        jetPt  = events.Jet_pt[events[t]==1]
        #jetEta = events.Jet_eta[events[t]==1]
        tauPt  = events.Tau_pt[events[t]==1]
        #tauEta = events.Tau_eta[events[t]==1]
        genTauPt = events.GenPart_pt[(abs(events.GenPart_pdgId)==15) & (events.GenPart_status==2)] 
        # genTaus = ak.zip(
        #     {
        #         "pt": events.GenPart_pt[(abs(events.GenPart_pdgId)==15) & (events.GenPart_status==2)],
        #         "eta": events.GenPart_eta[(abs(events.GenPart_pdgId)==15) & (events.GenPart_status==2)] ,
        #         "phi": events.GenPart_phi[(abs(events.GenPart_pdgId)==15) & (events.GenPart_status==2)] ,
        #         "mass": events.GenPart_mass[(abs(events.GenPart_pdgId)==15) & (events.GenPart_status==2)]
        #     },
        #     with_name="PtEtaPhiMCandidate",
        #     behavior=candidate.behavior,
        # )
        # numgenTaus = genTaus.compute()
        # print(ak.num(numgenTaus,axis=1))
        
        h_reco_tauPt.fill(
            dataset=dataset,
            pt=ak.flatten(tauPt),
        )
        h_reco_jetPt.fill(
            dataset=dataset,
            pt=ak.flatten(jetPt),
        )
        h_gen_tauPt.fill(dataset=dataset,pt=ak.flatten(genTauPt))
        
        return {
            "entries": ak.num(events,axis=0),
            # "Gen_higgs_pt" : genHiggs_pt,
            # "Gen_higgs_mass":genHiggs_mass,
            "Gen_tau_pt":h_gen_tauPt,
            # "Gen_a_mass":genA_mass,
            # "Gen_a_pt":genA_pt,
            "Reco_jet_pt":h_reco_jetPt,
            #"Reco_jet_eta":jetEta,
            "Reco_tau_pt":h_reco_tauPt,
            #"Reco_tau_eta":tauEta, 
            "numPt_trigger_":h_reco_jetPt.sum()
        }

    def postprocess(self, accumulator):
        pass


def makeFileSet(path):
    files = sorted(os.listdir(path))
    root_files = [path+f+":Events" for f in files if f.endswith(".root")][0:2]
    root_set = set(root_files)
    return root_set


paths = {
    "HAA4Tau_3p7" : "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_nanoAODSIM_hadronic/241001_054550/0000/",
    "HAA4Tau_14" : "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/14_nanoAODSIM_hadronic/241001_054627/0000/",
    # "QCD" : "/eos/cms/store/group/phys_diffraction/rchudasa/Run3Summer23NanoAODv12/QCD_PT-15to7000_TuneCP5_13p6TeV_pythia8/NANOAODSIM/castor_130X_mcRun3_2023_realistic_v14-v1/2560000/",
    # "DYLL" :"/eos/cms/store/group/phys_diffraction/rchudasa/Run3Summer23BPixNanoAODv12/DYto2L_M-50_TuneCP5_13p6TeV_pythia8/NANOAODSIM/KeepSi_130X_mcRun3_2023_realistic_postBPix_v2-v3/2560000/",
    # "WJets":"/eos/cms/store/mc/Run3Summer23BPixNanoAODv12/WtoLNu-2Jets_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_postBPix_v6-v2/2530000/",
    # "HTauTau":"/eos/cms/store/group/phys_diffraction/rchudasa/Run3Summer23BPixNanoAODv12/GluGluHToTauTau_M-125_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_postBPix_v2-v2/2560000/"
}

fileset = {dataset:{"files":makeFileSet(path)} for dataset,path in paths.items()}



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


print(out['HAA4Tau_3p7']['entries'])
scaled={}
for (name1, h1), (name2, h2), (name3, h3), (name4, h4), (name5, h5), (name6, h6) in zip(
    out["HAA4Tau_3p7"].items(), out["HAA4Tau_14"].items(),out["QCD"].items(), out["DYLL"].items(),out["WJets"].items(),out["HTauTau"].items()
):
    if isinstance(h1, hist.Hist) and isinstance(h2, hist.Hist) and isinstance(h3, hist.Hist) and isinstance(h4, hist.Hist) and isinstance(h5, hist.Hist) and isinstance(h6, hist.Hist):
        scaled[name1] = h1.copy() + h2.copy() + h3.copy() + h4.copy() + h5.copy() + h6.copy()
        print(type(h1), h1.sum())

fig, ax = plt.subplots()
scaled["Gen_tau_pt"].plot1d(ax=ax, overlay="dataset")
ax.set_yscale("log")
ax.set_ylim(1, None)
ax.legend(title="dataset")
plt.savefig('variables/GenPt_trigger.png')

fig, ax = plt.subplots()
scaled["Reco_tau_pt"].plot1d(ax=ax, overlay="dataset")
ax.set_yscale("log")
ax.set_ylim(1, None)
ax.legend(title="dataset")
plt.savefig('variables/RecoTauPt_trigger.png')

fig, ax = plt.subplots()
scaled["Reco_jet_pt"].plot1d(ax=ax, overlay="dataset")
ax.set_yscale("log")
ax.set_ylim(1, None)
ax.legend(title="dataset")
plt.savefig('variables/RecoJetPt_trigger.png')

elapsed = time.time() - tstart
print(elapsed)