import coffea.util
import matplotlib.pyplot as plt
from hist import Hist

# Load the saved histograms from .coffea file
input_file = 'output/histos_v1.coffea'
result = coffea.util.load(input_file)

# List of datasets (must match those in run_analysis.py)
datasets = ['QCD', 'Data']

# Plot settings
plt.rcParams.update({'font.size': 12})

# Plot ntaus histogram
plt.figure(figsize=(10, 6))
for dataset in datasets:
    hist_data = result['ntaus'][dataset, :]
    plt.hist(
        hist_data.axes[0].centers,
        weights=hist_data.values(),
        bins=hist_data.axes[0].edges,
        histtype='step',
        label=dataset,
        linewidth=1.5
    )
plt.xlabel('Number of Jets')
plt.ylabel('Events')
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig('ntaus.png')
plt.close()

# Plot jet_pt histogram
plt.figure(figsize=(10, 6))
for dataset in datasets:
    hist_data = result['jet_pt'][dataset, :]
    plt.hist(
        hist_data.axes[0].centers,
        weights=hist_data.values(),
        bins=hist_data.axes[0].edges,
        histtype='step',
        label=dataset,
        linewidth=1.5
    )
plt.xlabel(r'Jet $p_T$ [GeV]')
plt.ylabel('Events')
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig('jet_pt.png')
plt.close()

# Plot dijet_mass histogram
plt.figure(figsize=(10, 6))
for dataset in datasets:
    hist_data = result['dijet_mass'][dataset, :]
    plt.hist(
        hist_data.axes[0].centers,
        weights=hist_data.values(),
        bins=hist_data.axes[0].edges,
        histtype='step',
        label=dataset,
        linewidth=1.5
    )
plt.xlabel(r'Dijet Mass $m_{jj}$ [GeV]')
plt.ylabel('Events')
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig('dijet_mass.png')
plt.close()