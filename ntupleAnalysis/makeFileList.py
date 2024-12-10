import os

file_path = "new_file_m3p7.txt"

def makeFileSet(path):
    rootFiles = os.listdir(path)

    with open(file_path, "w") as file:
        for f in rootFiles:
            if f.endswith(".root"):
                file.write('file:'+path+'/'+f+',')

#makeFileSet('/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/crab_GEN_SIM_HToAATo4Tau_hadronic_tauDecay_M14_v2/240927_102000/0000')
makeFileSet('/eos/cms/store/group/phys_diffraction/rchudasa/MCGeneration/HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/crab_GEN_SIM_HToAATo4Tau_hadronic_tauDecay_M3p7/240927_094611/0000')