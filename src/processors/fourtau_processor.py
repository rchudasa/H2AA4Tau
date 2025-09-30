import coffea.processor as processor
from hist import Hist
from hist.axis import Regular, StrCategory
import awkward as ak
import numpy as np
import logging
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FourTauProcessor(processor.ProcessorABC):
    def __init__(self, year="2023"):
        self.year = year
        self._accumulator = processor.dict_accumulator({
            "ntaus": Hist(
                StrCategory([], name="category", label="Sample Category", growth=True),
                Regular(11, -0.5, 10.5, name="ntaus", label="Number of Taus")
            ),
            "jet_pt": Hist(
                StrCategory([], name="category", label="Sample Category", growth=True),
                Regular(50, 20, 500, name="pt", label=r"Jet $p_T$ [GeV]")
            ),
            "dijet_mass": Hist(
                StrCategory([], name="category", label="Sample Category", growth=True),
                Regular(50, 0, 1000, name="m_dijet", label=r"$m_{jj}$ [GeV]")
            )
        })
        logger.info(f"Initialized FourTauProcessor for year {year}")

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator
        dataset = events.metadata['dataset']
        logger.info(f"Processing dataset: {dataset}")
        filename = events.metadata.get('filename', 'unknown')
        logger.info(f"Processing dataset: {dataset}, file: {filename}")

        # Jet selection
        jets = events.Jet
        jet_sel = (
            (jets.pt > 20) &
            (abs(jets.eta) < 2.3)
        )
        good_jets = jets[jet_sel]
        njets = ak.num(good_jets, axis=1)

        # Event selection: >=2 jets (for dijet mass and tau analysis)
        sel_events = njets >= 2
        good_jets_sel = good_jets[sel_events]
        njets_sel = njets[sel_events]

        # Weights
        weights = ak.ones_like(njets_sel, dtype=np.float32)

        # Calculate dijet invariant mass for the leading two jets
        leading_jets = good_jets_sel[:, :2]  # Take first two jets
        dijet_mass = (leading_jets[:, 0] + leading_jets[:, 1]).mass
        dijet_mass = ak.fill_none(dijet_mass, 0)  # Handle cases with <2 jets

        # Fill histograms
        output["ntaus"].fill(category=dataset, ntaus=njets_sel, weight=weights)
        output["jet_pt"].fill(category=dataset, pt=ak.flatten(good_jets_sel.pt), weight=ak.flatten(ak.ones_like(good_jets_sel.pt)))
        output["dijet_mass"].fill(category=dataset, m_dijet=dijet_mass, weight=weights)

        return output

    def postprocess(self, accumulator):
        return accumulator