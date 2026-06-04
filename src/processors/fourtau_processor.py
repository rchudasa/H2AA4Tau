import coffea.processor as processor
from hist import Hist
from hist.axis import Regular, StrCategory
import awkward as ak
import numpy as np
import logging
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from coffea.processor import IterativeExecutor, Runner

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# Configure logging with a console handler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class FourTauProcessor(processor.ProcessorABC):
    def __init__(self, pd ="Tau"):
        self.pd = pd
        self.trigger_paths = {
            "JetMET": [
                "PFJet40",  # Single jet pT > 40 GeV
                "DiPFJetAve60",  # Di-jet average pT > 60 GeV
                "PFHT180",  # Scalar HT > 180 GeV
                "PFHT250",  # Scalar HT > 250 GeV
                "AK8PFJet400",  # Single fat jet pT > 400 GeV
                # Add more as needed, e.g., "PFMET110_PFMHT110_IDTight"
            ],
            "Tau": [
                "DoubleMediumDeepTauPFTauHPS35_L2NN_eta2p1", 
                "DoubleMediumDeepTauPFTauHPS30_L2NN_eta2p1_PFJet75",  
                "DoubleMediumDeepTauPFTauHPS30_L2NN_eta2p1_PFJet60",
                "DoubleMediumDeepTauPFTauHPS30_L2NN_eta2p1_OneProng_M5to80",
                "DoubleMediumChargedIsoDisplacedPFTauHPS32_Trk1_eta2p1",
                "LooseDeepTauPFTauHPS180_L2NN_eta2p1"
            ],
            # Extend for other PDs, e.g., "SingleMuon": ["IsoMu24", "IsoMu27"]
        }     
        self._accumulator = processor.dict_accumulator({
            "ntaus": Hist(
                StrCategory([], name="category", label="Sample Category", growth=True),
                Regular(20, 0, 21, name="ntaus", label="Number of Taus")
            ),
            "jet_pt": Hist(
                StrCategory([], name="category", label="Sample Category", growth=True),
                Regular(100, 20, 2500, name="pt", label=r"Jet $p_T$ [GeV]")
            ),
            "dijet_mass": Hist(
                StrCategory([], name="category", label="Sample Category", growth=True),
                Regular(50, 0, 1000, name="m_dijet", label=r"$m_{jj}$ [GeV]")
            )
        })
        logger.info(f"Initialized FourTauProcessor for pd {pd}")

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator
        dataset = events.metadata['dataset']
        logger.info(f"Processing dataset: {dataset}")
        filename = events.metadata.get('filename', 'unknown')
        logger.info(f"Processing dataset: {dataset}, file: {filename}")

        # Apply trigger cut
        active_pd = self.pd
        trigger_mask = None
        if active_pd in self.trigger_paths:
            for trigger in self.trigger_paths[active_pd]:
                logger.info(f"Checking trigger: {trigger}")
                try:
                    trigger_decision = getattr(events.HLT, trigger)
                    if trigger_mask is None:
                        trigger_mask = trigger_decision
                    else:
                        trigger_mask = trigger_mask | trigger_decision
                except Exception as e:
                    logger.warning(f"Trigger {trigger} not found in events.HLT for dataset {dataset}: {str(e)}")
                    continue
        else:
            logger.warning(f"No valid triggers found for dataset {active_pd}; processing all events (no trigger cut)")
            trigger_mask = ak.ones_like(events.event, dtype=bool)

        #trigger_mask = ak.ones_like(events.event, dtype=bool)
        # Apply trigger selection to events
        events = events[trigger_mask]

        # Jet selection
        jets = events.Jet
        jet_sel = (
            (jets.pt > 20) &
            (abs(jets.eta) < 2.3)
        )
        good_jets = jets[jet_sel]
        njets = ak.num(good_jets, axis=1)

        # # Event selection: >=2 jets (for dijet mass and tau analysis)
        # sel_events = njets >= 2
        # good_jets_sel = good_jets[sel_events]
        # njets_sel = njets[sel_events]

        # Weights
        #weights = ak.ones_like(njets_sel, dtype=np.float32)

        # Calculate dijet invariant mass for the leading two jets
        # leading_jets = good_jets_sel[:, :2]  # Take first two jets
        # dijet_mass = (leading_jets[:, 0] + leading_jets[:, 1]).mass
        # dijet_mass = ak.fill_none(dijet_mass, 0)  # Handle cases with <2 jets

        # Fill histograms
        output["ntaus"].fill(category=dataset, ntaus=njets)
        output["jet_pt"].fill(category=dataset, pt=ak.flatten(good_jets.pt))
        #logger.info(f"Invalid jets (NaN/inf): {(~np.isfinite(good_jets.pt)).sum()}")
        #logger.info(f"Pt number of jets: {len(ak.flatten(good_jets.pt))}")
        #logger.info("Pt number of jets: %d", len(ak.flatten(good_jets.pt)))
        print("Pt number of jets: %d", len(ak.flatten(good_jets.pt)))
        #output["dijet_mass"].fill(category=dataset, m_dijet=dijet_mass)

        return output

    def postprocess(self, accumulator):
        return accumulator