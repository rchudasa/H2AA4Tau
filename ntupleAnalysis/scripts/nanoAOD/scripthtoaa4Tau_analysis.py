import os
import numpy as np
import awkward as ak
from coffea import processor
import hist
import dask
import awkward as ak
import hist.dask as hda
import dask_awkward as dak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from coffea.analysis_tools import PackedSelection
#from TauPOG.TauIDSFs.TauIDSFTool import TauIDSFTool
import uproot
from collections import OrderedDict
import uuid

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
            #self.tauSFs = TauIDSFTool(f"20{year}", "DeepTau2017v2p1VSjet", self.tau_vsjet_wp, dm=True)
        
        # CUTFLOW
        self.cutflow = OrderedDict([
            ("none", "No cut"),
            ("four_taus", "≥4 taus passing ID and kinematics"),
            ("pair_selection", "Two di-tau pairs with ΔR > 0.5"),
            ("weight", "Weighted events (MC only)")
        ])
        
        # HISTOGRAM AXES
        self.pt_axis = hist.axis.Regular(50, 0, 200, name="pt", label=r"$p_T$ [GeV]")
        self.eta_axis = hist.axis.Regular(50, -2.5, 2.5, name="eta", label=r"$\eta$")
        self.phi_axis = hist.axis.Regular(50, -np.pi, np.pi, name="phi", label=r"$\phi$")
        self.mass_axis = hist.axis.Regular(50, 0, 100, name="mass", label=r"Mass [GeV]")
        self.dR_axis = hist.axis.Regular(50, 0, 5, name="dR", label=r"$\Delta R$")
        self.fourtau_mass_axis = hist.axis.Regular(50, 0, 500, name="fourtau_mass", label=r"$m_{4\tau}$ [GeV]")
        self.dataset_axis = hist.axis.StrCategory([], name="dataset", growth=True)
        
        # HISTOGRAMS
        self.histos = {
            "cutflow": hist.Hist(
                hist.axis.StrCategory(self.cutflow.keys(), name="cut", label="Selection"),
                hist.axis.Regular(1, 0, 1, name="events", label="Events"),
                storage=hist.storage.Double()
            ),
            "tau1_pt": hist.Hist(self.dataset_axis, self.pt_axis, storage=hist.storage.Weight()),
            "tau1_eta": hist.Hist(self.dataset_axis, self.eta_axis, storage=hist.storage.Weight()),
            "tau1_phi": hist.Hist(self.dataset_axis, self.phi_axis, storage=hist.storage.Weight()),
            "tau1_mass": hist.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            "tau2_pt": hist.Hist(self.dataset_axis, self.pt_axis, storage=hist.storage.Weight()),
            "tau2_eta": hist.Hist(self.dataset_axis, self.eta_axis, storage=hist.storage.Weight()),
            "tau2_phi": hist.Hist(self.dataset_axis, self.phi_axis, storage=hist.storage.Weight()),
            "tau2_mass": hist.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            "tau3_pt": hist.Hist(self.dataset_axis, self.pt_axis, storage=hist.storage.Weight()),
            "tau3_eta": hist.Hist(self.dataset_axis, self.eta_axis, storage=hist.storage.Weight()),
            "tau3_phi": hist.Hist(self.dataset_axis, self.phi_axis, storage=hist.storage.Weight()),
            "tau3_mass": hist.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            "tau4_pt": hist.Hist(self.dataset_axis, self.pt_axis, storage=hist.storage.Weight()),
            "tau4_eta": hist.Hist(self.dataset_axis, self.eta_axis, storage=hist.storage.Weight()),
            "tau4_phi": hist.Hist(self.dataset_axis, self.phi_axis, storage=hist.storage.Weight()),
            "tau4_mass": hist.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            "ditau1_mass": hist.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            "ditau2_mass": hist.Hist(self.dataset_axis, self.mass_axis, storage=hist.storage.Weight()),
            "fourtau_mass": hist.Hist(self.dataset_axis, self.fourtau_mass_axis, storage=hist.storage.Weight()),
            "ditau1_dR": hist.Hist(self.dataset_axis, self.dR_axis, storage=hist.storage.Weight()),
            "ditau2_dR": hist.Hist(self.dataset_axis, self.dR_axis, storage=hist.storage.Weight()),
        }

    def process(self, events):
        dataset = events.metadata["dataset"]
        output = {k: h.copy() for k, h in self.histos.items()}       
         
        # CUTFLOW: NO CUT
        output["cutflow"].fill(cut="none", events=np.ones(len(events)), dataset=dataset)
        
        # TAU SELECTION
        taus = events.Tau
        tau_mask = (
            (taus.pt > self.tau_pt_min) &
            (abs(taus.eta) < self.tau_eta_max) &
            (abs(taus.dz) < self.tau_dz_max) &
            (ak.Array([t.decayMode in self.tau_dm_allowed for t in taus])) &
            (taus.idDeepTau2017v2p1VSjet >= 4) &  # Medium WP
            (taus.idDeepTau2017v2p1VSe >= 1) &    # VVVLoose
            (taus.idDeepTau2017v2p1VSmu >= 1) &   # VLoose
            (abs(taus.charge) == 1)
        )
        selected_taus = taus[tau_mask]
        
        # REQUIRE AT LEAST 4 TAUS
        four_taus_mask = ak.num(selected_taus) >= 4
        events = events[four_taus_mask]
        selected_taus = selected_taus[four_taus_mask]
        output["cutflow"].fill(cut="four_taus", events=np.ones(len(events)), dataset=dataset)
        
        if len(events) == 0:
            return output
        
        # PAIR SELECTION: FORM TWO DI-TAU PAIRS
        tau_pairs = []
        for i in range(ak.num(selected_taus)):
            for j in range(i + 1, ak.num(selected_taus)):
                tau1 = selected_taus[:, i]
                tau2 = selected_taus[:, j]
                dR = tau1.delta_r(tau2)
                if ak.all(dR > 0.5):
                    tau_pairs.append((tau1, tau2))
        
        # SELECT FIRST TWO VALID PAIRS
        pair_mask = ak.num(tau_pairs) >= 2
        events = events[pair_mask]
        selected_taus = selected_taus[pair_mask]
        tau_pairs = tau_pairs[pair_mask]
        
        if len(events) == 0:
            return output
        
        output["cutflow"].fill(cut="pair_selection", events=np.ones(len(events)), dataset=dataset)
        
        # SELECT FIRST TWO PAIRS (SIMPLIFIED)
        tau1, tau2 = tau_pairs[:, 0]
        tau3, tau4 = tau_pairs[:, 1]
        
        # WEIGHTS
        if self.ismc:
            weight = events.genWeight
            for tau in [tau1, tau2, tau3, tau4]:
                #tau_sf = self.tauSFs.getSFvsDM(tau.pt, tau.decayMode)
                tau_sf = np.ones(len(tau))  # Placeholder for actual SF calculation
                weight = weight * tau_sf
        else:
            weight = np.ones(len(events))
        
        output["cutflow"].fill(cut="weight", events=np.ones(len(events)), weight=weight, dataset=dataset)
        
        # FILL HISTOGRAMS
        output["tau1_pt"].fill(dataset=dataset, pt=tau1.pt, weight=weight)
        output["tau1_eta"].fill(dataset=dataset, eta=tau1.eta, weight=weight)
        output["tau1_phi"].fill(dataset=dataset, phi=tau1.phi, weight=weight)
        output["tau1_mass"].fill(dataset=dataset, mass=tau1.mass, weight=weight)
        output["tau2_pt"].fill(dataset=dataset, pt=tau2.pt, weight=weight)
        output["tau2_eta"].fill(dataset=dataset, eta=tau2.eta, weight=weight)
        output["tau2_phi"].fill(dataset=dataset, phi=tau2.phi, weight=weight)
        output["tau2_mass"].fill(dataset=dataset, mass=tau2.mass, weight=weight)
        output["tau3_pt"].fill(dataset=dataset, pt=tau3.pt, weight=weight)
        output["tau3_eta"].fill(dataset=dataset, eta=tau3.eta, weight=weight)
        output["tau3_phi"].fill(dataset=dataset, phi=tau3.phi, weight=weight)
        output["tau3_mass"].fill(dataset=dataset, mass=tau3.mass, weight=weight)
        output["tau4_pt"].fill(dataset=dataset, pt=tau4.pt, weight=weight)
        output["tau4_eta"].fill(dataset=dataset, eta=tau4.eta, weight=weight)
        output["tau4_phi"].fill(dataset=dataset, phi=tau4.phi, weight=weight)
        output["tau4_mass"].fill(dataset=dataset, mass=tau4.mass, weight=weight)
        
        # DI-TAU MASSES
        ditau1_p4 = tau1.p4 + tau2.p4
        ditau2_p4 = tau3.p4 + tau4.p4
        output["ditau1_mass"].fill(dataset=dataset, mass=ditau1_p4.mass, weight=weight)
        output["ditau2_mass"].fill(dataset=dataset, mass=ditau2_p4.mass, weight=weight)
        output["ditau1_dR"].fill(dataset=dataset, dR=tau1.delta_r(tau2), weight=weight)
        output["ditau2_dR"].fill(dataset=dataset, dR=tau3.delta_r(tau4), weight=weight)
        
        # FOUR-TAU MASS
        fourtau_p4 = ditau1_p4 + ditau2_p4
        output["fourtau_mass"].fill(dataset=dataset, fourtau_mass=fourtau_p4.mass, weight=weight)
        
        return output

    def postprocess(self, accumulator):
        return accumulator

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("inputfile", help="Input NanoAOD file")
    parser.add_argument("--year", default=2018, type=int, help="Data-taking year")
    parser.add_argument("--ismc", action="store_true", help="Is Monte Carlo")
    parser.add_argument("--output", default="output_4tau.root", help="Output ROOT file")
    args = parser.parse_args()
    
    # LOAD EVENTS
    events = NanoEventsFactory.from_root(
        {args.inputfile: "Events"}, 
        schemaclass=NanoAODSchema
        ).events()
    
    # RUN PROCESSOR
    processor_instance = HToAA4TauProcessor(year=args.year, ismc=args.ismc)
    output = processor.run_uproot_job(
        {args.inputfile: "Events"},
        treename="Events",
        processor_instance=processor_instance,
        executor=processor.futures_executor,
        executor_args={"workers": 4}
    )
    
    # PRINT CUTFLOW
    print("\nCutflow:")
    for cut, label in processor_instance.cutflow.items():
        count = output["cutflow"].values()[(cut,)][0]
        print(f"{label}: {count}")
    
    # SAVE HISTOGRAMS
    with uproot.recreate(args.output) as f:
        for hname, histo in output.items():
            if hname == "cutflow":
                for cut, _ in processor_instance.cutflow.items():
                    f[f"cutflow_{cut}"] = histo[cut].to_hist()
            else:
                f[hname] = histo.to_hist()

if __name__ == "__main__":
    main()