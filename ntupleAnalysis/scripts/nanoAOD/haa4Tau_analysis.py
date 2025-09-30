import os
import numpy as np
import awkward as ak
import dask
import dask_awkward as dak
import hist
import hist.dask as hda
from coffea import processor
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema, BaseSchema
from coffea.nanoevents.methods import candidate
from coffea.dataset_tools import apply_to_fileset, preprocess, max_chunks
#from TauPOG.TauIDSFs.TauIDSFTool import TauIDSFTool
import uproot
import matplotlib.pyplot as plt
import mplhep as hep
from collections import OrderedDict
import time

#print(dir(hda.Hist), type(hda.Hist))

print("-------------------------------")
#print(dir(hda.hist), type(hda.hist))
class HToAA4TauProcessor(processor.ProcessorABC):
    def __init__(self, year=2018, ismc=True):
        self.year = year
        self.ismc = ismc
        
        # TAU SELECTION CRITERIA
        self.tau_pt_min = 20.0
        self.tau_eta_max = 2.3
        self.tau_dz_max = 0.2
        self.tau_dm_allowed = [0, 1, 10, 11]
        self.tau_vsjet_wp = "Medium"
        
        # TAU ID SCALE FACTORS
        #if self.ismc:
        #    self.tauSFs = TauIDSFTool(f"20{year}", "DeepTau2017v2p1VSjet", self.tau_vsjet_wp, dm=True)
        
        # CUTFLOW
        # self.cutflow = OrderedDict([
        #     ("none", "No cut"),
        #     ("four_taus", "≥2 taus passing ID and kinematics"),
        #     ("pair_selection", "Two di-tau pairs with ΔR > 0.5"),
        #     ("weight", "Weighted events (MC only)")
        # ])
        
        # HISTOGRAM AXES
        bins = np.linspace(0, 200, 50)
        self.pt_axis = hist.axis.Regular(bins.size - 1, bins.min(), bins.max(), name="pt", label=r"$p_T$ [GeV]")
        bins_eta = np.linspace(-2.5, 2.5, 50)
        self.eta_axis = hist.axis.Regular(bins_eta.size - 1, bins_eta.min(), bins_eta.max(), name="eta", label=r"$\eta$")
        bins_phi = np.linspace(-np.pi, np.pi, 50)
        self.phi_axis = hist.axis.Regular(bins_phi.size - 1, bins_phi.min(), bins_phi.max(), name="phi", label=r"$\phi$")
        bins_mass = np.linspace(0, 100, 50)
        self.mass_axis = hist.axis.Regular(bins_mass.size - 1, bins_mass.min(), bins_mass.max(), name="mass", label=r"Mass [GeV]")
        bins_dR = np.linspace(0, 5, 50)
        self.dR_axis = hist.axis.Regular(bins_dR.size - 1, bins_dR.min(), bins_dR.max(), name="dR", label=r"$\Delta R$")
        bins_fourtau = np.linspace(0, 500, 50)
        self.fourtau_mass_axis = hist.axis.Regular(bins_fourtau.size - 1, bins_fourtau.min(), bins_fourtau.max(), name="fourtau_mass", label=r"$m_{4\tau}$ [GeV]")
        self.dataset_axis = hist.axis.StrCategory([], name="dataset", growth=True)
        
        # HISTOGRAMS
        self.histos = {
            # "cutflow": hda.Hist(
            #     hist.axis.StrCategory(self.cutflow.keys(), name="cut", label="Selection"),
            #     hist.axis.Regular(1, 0, 1, name="events", label="Events"),
            #     storage=hist.storage.Double()
            # ),
            "tau1_pt": hda.Hist(self.dataset_axis, self.pt_axis, storage=hist.storage.Weight()),
            "tau1_eta": hda.Hist(self.dataset_axis, self.eta_axis, storage=hist.storage.Weight()),
            # "tau1_phi": hda.Hist(self.dataset_axis, self.phi_axis, storage=hist.storage.Weight()),
            # "tau1_mass": hda.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            # "tau2_pt": hda.Hist(self.dataset_axis, self.pt_axis, storage=hist.storage.Weight()),
            # "tau2_eta": hda.Hist(self.dataset_axis, self.eta_axis, storage=hist.storage.Weight()),
            # "tau2_phi": hda.Hist(self.dataset_axis, self.phi_axis, storage=hist.storage.Weight()),
            # "tau2_mass": hda.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            # "tau3_pt": hda.Hist(self.dataset_axis, self.pt_axis, storage=hist.storage.Weight()),
            # "tau3_eta": hda.Hist(self.dataset_axis, self.eta_axis, storage=hist.storage.Weight()),
            # "tau3_phi": hda.Hist(self.dataset_axis, self.phi_axis, storage=hist.storage.Weight()),
            # "tau3_mass": hda.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            # "tau4_pt": hda.Hist(self.dataset_axis, self.pt_axis, storage=hist.storage.Weight()),
            # "tau4_eta": hda.Hist(self.dataset_axis, self.eta_axis, storage=hist.storage.Weight()),
            # "tau4_phi": hda.Hist(self.dataset_axis, self.phi_axis, storage=hist.storage.Weight()),
            # "tau4_mass": hda.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            # "ditau1_mass": hda.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            # "ditau2_mass": hda.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            # "fourtau_mass": hda.Hist(self.dataset_axis, self.fourtau_mass_axis, storage=hist.storage.Weight()),
            # "ditau1_dR": hda.Hist(self.dataset_axis, self.dR_axis, storage=hist.storage.Weight()),
            # "ditau2_dR": hda.Hist(self.dataset_axis, self.dR_axis, storage=hist.storage.Weight()),
        }

    def process(self, events):
        dataset = events.metadata["dataset"]
        output = {k: h.copy() for k, h in self.histos.items()}
        
        # CUTFLOW: NO CUT
        #output["cutflow"].fill(cut="none", events=dak.ones_like(events.run))
        
        # TAU SELECTION
        taus = events.Tau
        print("Number of events :", ak.num(events, axis=0).compute())
        # Number of taus per event
        total_taus = ak.num(taus, axis=1).compute().to_list()
        print("Total number of taus :", sum(total_taus))

        #print(ak.num(taus, axis=1).compute().to_list())
        tau_mask = (
            (taus.pt > self.tau_pt_min) &
            (abs(taus.eta) < self.tau_eta_max) #&
            #(abs(taus.dz) < self.tau_dz_max)
            #(dak.map(lambda x: x.decayMode in self.tau_dm_allowed, taus)) &
            # (taus.idDeepTau2017v2p1VSjet >= 4) &  # Medium WP
            # (taus.idDeepTau2017v2p1VSe >= 1) &    # VVVLoose
            # (taus.idDeepTau2017v2p1VSmu >= 1) &   # VLoose
            # (abs(taus.charge) == 1)
        )

        selected_taus = taus[tau_mask]
        print("Number of events with kin cuts:", ak.num(selected_taus,axis=0).compute())
        print("Number of total taus after kin cuts:", sum(ak.num(selected_taus, axis=1).compute().to_list()))
  
        # # REQUIRE AT LEAST 4 TAUS
        # four_taus_mask = dak.num(selected_taus, axis=1) >= 2
        # events = events[four_taus_mask]
        # selected_taus = selected_taus[four_taus_mask]

        # print("tau_mask type:", (selected_taus.pt).compute())
        
        # # Number of taus per event
        # seltaus_per_event = ak.num(selected_taus, axis=1).compute().to_list()
        # print("Selected number of taus per event after applying cuts:", sum(seltaus_per_event))

        #output["cutflow"].fill(cut="four_taus", events=dak.ones_like(events.run))
        
        #if ak.num(events, axis=0) == 0:
            #return output
 

        # # PAIR SELECTION: FORM TWO DI-TAU PAIRS
        # def make_pairs(taus):
        #     pairs = []
        #     for i in range(dak.num(taus)):
        #         for j in range(i + 1, dak.num(taus)):
        #             tau1 = taus[i]
        #             tau2 = taus[j]
        #             dR = tau1.delta_r(tau2)
        #             if dR > 0.5:
        #                 pairs.append((tau1, tau2))
        #     return ak.Array(pairs)
        
        # tau_pairs = dak.map_partitions(make_pairs, selected_taus)
        
        # # SELECT FIRST TWO VALID PAIRS
        # pair_mask = dak.num(tau_pairs, axis=1) >= 2
        # events = events[pair_mask]
        # selected_taus = selected_taus[pair_mask]
        # tau_pairs = tau_pairs[pair_mask]
        
        # if ak.num(events, axis=0) == 0:
        #     return output
        
        #output["cutflow"].fill(cut="pair_selection", events=dak.ones_like(events.run))
        
        # # SELECT FIRST TWO PAIRS
        # tau1, tau2 = tau_pairs[:, 0]
        # tau3, tau4 = tau_pairs[:, 1]
        
        # WEIGHTS
        # if self.ismc:
        #     weight = events.genWeight
        #     for tau in [tau1, tau2, tau3, tau4]:
        #         #tau_sf = dak.map(lambda x: self.tauSFs.getSFvsDM(x.pt, x.decayMode), tau)
        #         tau_sf  = 1 # Placeholder for actual SF calculation
        #         weight = weight * tau_sf
        # else:
        #     weight = dak.ones_like(events.run)
        
        # output["cutflow"].fill(cut="weight", events=dak.ones_like(events.run), weight=weight)
        
        # FILL HISTOGRAMS
        output["tau1_pt"].fill(dataset=dataset, pt=dak.flatten(taus.pt))
        output["tau1_eta"].fill(dataset=dataset, eta=dak.flatten(taus.eta))
        # output["tau1_phi"].fill(dataset=dataset, phi=dak.flatten(tau1.phi), weight=weight)
        # output["tau1_mass"].fill(dataset=dataset, mass=dak.flatten(tau1.mass), weight=weight)
        # output["tau2_pt"].fill(dataset=dataset, pt=dak.flatten(tau2.pt), weight=weight)
        # output["tau2_eta"].fill(dataset=dataset, eta=dak.flatten(tau2.eta), weight=weight)
        # output["tau2_phi"].fill(dataset=dataset, phi=dak.flatten(tau2.phi), weight=weight)
        # output["tau2_mass"].fill(dataset=dataset, mass=dak.flatten(tau2.mass), weight=weight)
        # output["tau3_pt"].fill(dataset=dataset, pt=dak.flatten(tau3.pt), weight=weight)
        # output["tau3_eta"].fill(dataset=dataset, eta=dak.flatten(tau3.eta), weight=weight)
        # output["tau3_phi"].fill(dataset=dataset, phi=dak.flatten(tau3.phi), weight=weight)
        # output["tau3_mass"].fill(dataset=dataset, mass=dak.flatten(tau3.mass), weight=weight)
        # output["tau4_pt"].fill(dataset=dataset, pt=dak.flatten(tau4.pt), weight=weight)
        # output["tau4_eta"].fill(dataset=dataset, eta=dak.flatten(tau4.eta), weight=weight)
        # output["tau4_phi"].fill(dataset=dataset, phi=dak.flatten(tau4.phi), weight=weight)
        # output["tau4_mass"].fill(dataset=dataset, mass=dak.flatten(tau4.mass), weight=weight)
        
        # # DI-TAU MASSES
        # ditau1_p4 = tau1 + tau2
        # ditau2_p4 = tau3 + tau4
        # output["ditau1_mass"].fill(dataset=dataset, mass=dak.flatten(ditau1_p4.mass), weight=weight)
        # output["ditau2_mass"].fill(dataset=dataset, mass=dak.flatten(ditau2_p4.mass), weight=weight)
        # output["ditau1_dR"].fill(dataset=dataset, dR=dak.flatten(tau1.delta_r(tau2)), weight=weight)
        # output["ditau2_dR"].fill(dataset=dataset, dR=dak.flatten(tau3.delta_r(tau4)), weight=weight)
        
        # # FOUR-TAU MASS
        # fourtau_p4 = ditau1_p4 + ditau2_p4
        # output["fourtau_mass"].fill(dataset=dataset, fourtau_mass=dak.flatten(fourtau_p4.mass), weight=weight)
        print("Output type:", type(output))
        print("-----------------------------------------------")
        return {
            "entries": ak.num(events, axis=0),
            **output
        }

    def postprocess(self, accumulator):
        return accumulator

def make_fileset(paths):
    fileset = {}
    for dataset, path in paths.items():
        files = sorted(os.listdir(path))
        root_files = [os.path.join(path, f) + ":Events" for f in files if f.endswith(".root")]
        fileset[dataset] = {"files": {f: "Events" for f in root_files}}
    return fileset

def plot_histograms(scaled, output_dir="variables"):
    os.makedirs(output_dir, exist_ok=True)
    hep.style.use("CMS")
    
    for name, histo in scaled.items():
        if isinstance(histo, hist.Hist):
            fig, ax = plt.subplots()
            histo.plot1d(ax=ax, overlay="dataset")
            ax.set_yscale("log")
            ax.set_ylim(1, None)
            ax.legend(title="dataset")
            hep.cms.label("Preliminary")
            plt.savefig(f"{output_dir}/{name}.png")
            plt.close()

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", default=2018, type=int, help="Data-taking year")
    parser.add_argument("--ismc", action="store_true", help="Is Monte Carlo")
    parser.add_argument("--output", default="../../outputs/output_4tau.root", help="Output ROOT file")
    args = parser.parse_args()
    
    commonPath = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/"
    commonPath = "/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/"

    #commonPath = "/eos/uscms/store/group/lpcml/rchudasa/MCGenerationRun3/"

    fileset = {
        "3p7": {"files": {commonPath+"HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_nanoAODSIM_hadronic/241001_054550/0000/step5_nanoAOD_coffea_75.root":"Events"}},
        "14" : {"files": {commonPath+"HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/14_nanoAODSIM_hadronic/241001_054627/0000/step5_nanoAOD_coffea_65.root":"Events"}}
        #"3p7": {"files": {commonPath+"NANOAOD_VBFH_HToAATo4Tau_M3p7_all.root":"Events"}},
        #"14" : {"files": {commonPath+"NANOAOD_VBFH_HToAATo4Tau_M14.root":"Events"}}
    }
    
    # PREPROCESS AND RUN
    to_compute = apply_to_fileset(
        HToAA4TauProcessor(year=args.year, ismc=args.ismc),
        fileset,
        #max_chunks(preprocess(fileset), 2),
        schemaclass=NanoAODSchema
    )
    dak.necessary_columns(to_compute)
    import time
    tstart = time.time()

    (out,) = dask.compute(to_compute)
    #print(out)
    
    # # PRINT CUTFLOW
    # print("\nCutflow:")
    # for dataset, content in out.items():
    #     print(f"\nDataset: {dataset}")
    #     print(f"Entries: {content['entries']}")
    #     for cut, label in HToAA4TauProcessor().cutflow.items():
    #         count = content["cutflow"].values().get((cut,), 0)
    #         print(f"{label}: {count}")
    
    # SCALE AND PLOT HISTOGRAMS
    scaled = {}
    for name in out["3p7"].keys():
        if name != "entries" and isinstance(out["3p7"][name], hist.Hist):
            scaled[name] = sum(out[ds][name] for ds in out)
    
    #plot_histograms(scaled)
    
    # SAVE HISTOGRAMS
    with uproot.recreate(args.output) as f:
        for dataset, content in out.items():
            for hname, histo in content.items():
                if hname == "cutflow":
                    for cut, _ in HToAA4TauProcessor().cutflow.items():
                        f[f"{dataset}/cutflow_{cut}"] = histo[cut]
                elif hname != "entries":
                    f[f"{dataset}/{hname}"] = histo
    
    elapsed = time.time() - tstart
    print(f"\nExecution time: {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()