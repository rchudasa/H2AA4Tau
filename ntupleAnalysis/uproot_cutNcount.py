import uproot
import matplotlib.pyplot as plt
import os
import mplhep as hep
import time

def makeFileSet(path):
    files = os.listdir(path)
    root_files = [path+f+":Events" for f in files if f.endswith(".root")]
    root_set = set(root_files)
    return root_set

def makeFileList(path):
    files = os.listdir(path)
    root_files = [path+f for f in files if f.endswith(".root")]
    return root_files[0:2]

commonMCPath = '/eos/uscms/store/group/lpcml/rchudasa/MCGenerationRun3/'

paths = {
    "4Tau_3p7" : commonMCPath+"HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_MLAnalyzer_ntuples_v1/241209_155112/0000/",
    "4Tau_14" : commonMCPath+"HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/14_MLAnalyzer_ntuples_v1/241209_155230/0000/",
    "QCD":commonMCPath+"GEN_SIM_QCD_pt15to7000_Run3Summer23GS/QCD_MLAnalyzer_ntuples_v1/241209_155310/0000/"
    
}

#fileset = {dataset:{"files":makeFileList(path)} for dataset,path in paths.items()}
fileList = {dataset:makeFileList(path) for dataset,path in paths.items()}

print(fileList)

start = time.time()

for dataset,fList in fileList.items():
    for f in fList:
        with uproot.open(f) as ff:
            print(f)
            hist = ff['fevt/hNpassed_hlt']
            hep.histplot(hist,label=dataset)
    break
    
plt.xlabel("X-axis label")
plt.ylabel("Counts")
plt.title("Histograms from Multiple Files")
plt.legend()
plt.savefig('hlt_passed.png')
end = time.time()
print("It took:", end-start)
