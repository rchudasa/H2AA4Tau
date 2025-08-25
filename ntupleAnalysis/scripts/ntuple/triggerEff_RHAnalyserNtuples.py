import awkward as ak
import uproot
import vector
import numpy as np
import mplhep as hep
from hist import Hist
import matplotlib.pyplot as plt
import mplhep as hep

infile = uproot.open('/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/withTrigger/HToAATo4Tau_M_3p7_pythia8_2018UL_AOD/3p7_RHEvent-Trigger_ntuples_v2/240813_103752/0000/output_31.root')

hltTree = infile['hltanalysis/HltTree']
rhTree = infile['fevt/RHTree']

def checkSameEvent(tree1,tree2):
    eventID1 = tree1['Event'].array()
    lumiBlock1 = tree1['LumiBlock'].array()

    eventID2 = tree2['eventId'].array()
    lumiBlock2 = tree2['lumiId'].array()

    if len(eventID1) != len(eventID2) or len(lumiBlock1) != len(lumiBlock2):
        print("The trees have different number of events")
    else:
        matching_events = (eventID1 == eventID2) & (lumiBlock1 == lumiBlock2)

        if ak.all(matching_events):
            print("All events match between the two trees.")
        else:
            print(f"Not all events match. {ak.sum(matching_events)} out of {len(eventID1)} events match.")

            # Identify mismatching event indices
            mismatch_indices = ak.where(~matching_events)[0]
            print("Mismatching event indices:", mismatch_indices)

            # Cross-check specific events
            for i in mismatch_indices:
                print(f"Tree1 - EventID: {eventID1[i]}, LuminosityBlock: {lumiBlock1[i]}")
                print(f"Tree2 - EventID: {eventID2[i]}, LuminosityBlock: {lumiBlock2[i]}")


checkSameEvent(hltTree,rhTree)

def contains_any_char(s, chars):
    return any(char in s for char in chars)

count = 0
triggerBranches = []
for branch_name in hltTree.keys():
    #remove_branch = ['Prescl','Photon','Ele','Mu','Fwd','Triple','Quad']
    remove_branch = ['Prescl','Photon','ZeroBias','Physics','Calibration','Random','Hcal']

    #if 'HLT' in branch_name and 'Jet' in branch_name and not contains_any_char(branch_name,remove_branch):
    if 'HLT' in branch_name and not contains_any_char(branch_name,remove_branch):
        count+=1
        #if count==10:
        #    break
        triggerBranches.append(branch_name)
        #print(branch_name)
print(count)

def plotEfficiency(bins, numerator_hist, denominator_hist, triggerName):
    # Plot the efficiency
    eff = numerator_hist / denominator_hist

    bin_centers = bins[:-1] + np.diff(bins) / 2
    eff_values = eff.values()

    plt.figure(figsize=(8, 6))

    # Plot efficiency with error bars (sqrt(N) approximation)
    plt.errorbar(bin_centers, eff_values, yerr=np.sqrt(eff_values*(1-eff_values)/denominator_hist.values()), fmt='o', color='blue', label='Efficiency')

    # Set x and y axis limits
    plt.xlim(0, 100)  # Set x-axis range from 0 to 100
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

    plt.savefig('figures/'+triggerName+'.png')
    # Show plot
    #plt.show()
    plt.close()

tauPt_triggerPass_dict ={}
tauPt_triggerHist_dict ={}

tauEta_triggerPass_dict ={}
tauEta_triggerHist_dict ={}

jetPt_triggerPass_dict ={}
jetPt_triggerHist_dict ={}

jetEta_triggerPass_dict ={}
jetEta_triggerHist_dict ={}

for t in triggerBranches:
    jetPt  = rhTree['jetPt'].array()
    jetEta = rhTree['jetEta'].array()
    jetPt = jetPt[jetPt>20]
    jetEta = jetEta[jetPt>20]


    triggarray = hltTree[t].array()
    jetPt_triggerPass_dict[t]  = jetPt[triggarray==1]
    jetEta_triggerPass_dict[t] = jetEta[triggarray==1]

    jetPt_triggered = ak.sum(jetPt_triggerPass_dict[t])
    jetPt_all = ak.sum(jetPt)


    efficiency = jetPt_triggered/jetPt_all
    #if efficiency>0.2 and 'PFTau' in t:
    if efficiency>0.2:

        bins = np.linspace(20, 100, 8)

        denominator_hist = Hist.new.Reg(bins.size - 1, bins.min(), bins.max(), name="pt").Double()
        numerator_hist = Hist.new.Reg(bins.size - 1, bins.min(), bins.max(), name="pt").Double()

        # Fill histograms
        denominator_hist.fill(ak.flatten(jetPt))
        numerator_hist.fill(ak.flatten(jetPt_triggerPass_dict[t]))
        #efficiency = numerator_hist / denominator_hist
        print(t, round((efficiency*100),2),"%")

        plotEfficiency(bins, numerator_hist, denominator_hist, t)
