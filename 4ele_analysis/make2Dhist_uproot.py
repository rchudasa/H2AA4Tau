import awkward as ak
import uproot
import vector
import numpy as np
import mplhep as hep
from hist import Hist
import matplotlib.pyplot as plt
import mplhep as hep

masspoints = ['0p01','0p05','0p1','0p15','0p2','0p25','0p3','0p35','0p4','0p45','0p6','0p8','1','1p2']

for mass in masspoints:
    infile = uproot.open(f'GenInfo_only_H2AA4Ele_{mass}GeV.root')
    print(infile)

    rhTree = infile['fevt/RHTree']
    deta_ele  = rhTree['Ele1_Ele2_deta'].array()
    dphi_ele  = rhTree['Ele1_Ele2_dphi'].array()
    deta_ele2  = rhTree['Ele3_Ele4_deta'].array()
    dphi_ele2  = rhTree['Ele3_Ele4_dphi'].array()


    h2 = Hist.new.Reg(10, 0, 0.174, name="deta").Reg(10, 0, 0.174, name="dphi").Double()
    h2.fill(deta=ak.flatten(deta_ele), dphi=ak.flatten(dphi_ele))
    h2.fill(deta=ak.flatten(deta_ele2), dphi=ak.flatten(dphi_ele2))

    print(np.arange(0,0.174,0.0174))

    import mplhep
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.set_xticks(np.arange(0,0.174,0.0174))
    ax.set_xticklabels([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    ax.set_yticks(np.arange(0,0.174,0.0174))
    ax.set_yticklabels([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    h2.plot2d(ax=ax, cmap="seismic")
    #plt.show()
    plt.title(f'H->AA->4electrons A ({mass} GeV)')
    plt.savefig(f'2dfigures/2d_plot_{mass}.png')
# bins = np.linspace(20, 100, 8)

# denominator_hist = Hist.new.Reg(bins.size - 1, bins.min(), bins.max(), name="pt").Double()
# numerator_hist = Hist.new.Reg(bins.size - 1, bins.min(), bins.max(), name="pt").Double()

# Fill histograms
# denominator_hist.fill(ak.flatten(jetPt))
# numerator_hist.fill(ak.flatten(jetPt))

# eff = numerator_hist / denominator_hist

# bin_centers = bins[:-1] + np.diff(bins) / 2
# eff_values = eff.values()

# plt.figure(figsize=(8, 6))

# # Plot efficiency with error bars (sqrt(N) approximation)
# plt.errorbar(bin_centers, eff_values, yerr=np.sqrt(eff_values*(1-eff_values)/denominator_hist.values()), fmt='o', color='blue', label='Efficiency')

# # Set x and y axis limits
# plt.xlim(0, 100)  # Set x-axis range from 0 to 100
# plt.ylim(0, 1.2)    # Set y-axis range from 0 to 1

# # Add grid lines on both x and y axes
# plt.grid(True)  # Adds grid lines to both x and y axes
# plt.grid(which='major', linestyle='-', linewidth='0.5', color='gray')  # Customize grid lines
# #plt.grid(which='minor', linestyle=':', linewidth='0.5', color='gray')  # Customize minor grid lines

# # Apply mplhep style
# hep.style.use("CMS")  # or another style like ATLAS, ALICE, LHCb
# hep.cms.label("Preliminary")

# # Add labels and title
# plt.xlabel(r'$p_T$ [GeV]')
# plt.ylabel('Efficiency')
# #plt.title('Efficiency vs $p_T$')

# # Show legend
# plt.legend()

# plt.savefig('Eff.png')
# # Show plot
# #plt.show()
# plt.close()


count = 0
triggerBranches = []

# def plotEfficiency(bins, numerator_hist, denominator_hist, triggerName):
#     # Plot the efficiency
#     eff = numerator_hist / denominator_hist

#     bin_centers = bins[:-1] + np.diff(bins) / 2
#     eff_values = eff.values()

#     plt.figure(figsize=(8, 6))

#     # Plot efficiency with error bars (sqrt(N) approximation)
#     plt.errorbar(bin_centers, eff_values, yerr=np.sqrt(eff_values*(1-eff_values)/denominator_hist.values()), fmt='o', color='blue', label='Efficiency')

#     # Set x and y axis limits
#     plt.xlim(0, 100)  # Set x-axis range from 0 to 100
#     plt.ylim(0, 1.2)    # Set y-axis range from 0 to 1

#     # Add grid lines on both x and y axes
#     plt.grid(True)  # Adds grid lines to both x and y axes
#     plt.grid(which='major', linestyle='-', linewidth='0.5', color='gray')  # Customize grid lines
#     #plt.grid(which='minor', linestyle=':', linewidth='0.5', color='gray')  # Customize minor grid lines

#     # Apply mplhep style
#     hep.style.use("CMS")  # or another style like ATLAS, ALICE, LHCb
#     hep.cms.label("Preliminary")

#     # Add labels and title
#     plt.xlabel(r'$p_T$ [GeV]')
#     plt.ylabel('Efficiency')
#     #plt.title('Efficiency vs $p_T$')

#     # Show legend
#     plt.legend()

#     plt.savefig('figures/'+triggerName+'.png')
#     # Show plot
#     #plt.show()
#     plt.close()