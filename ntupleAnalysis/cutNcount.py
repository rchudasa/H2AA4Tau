import os
from glob import glob

import numpy as np

import ROOT
from ROOT import TCanvas, TPad, TFile, TPaveText, TLegend
from ROOT import gBenchmark, gStyle, gROOT, TStyle
from ROOT import TH1D, TF1, TGraphErrors, TMultiGraph

from math import sqrt

from array import array

import CMS_lumi

#change the CMS_lumi variables (see CMS_lumi.py)
CMS_lumi.lumi_13TeV = '13 TeV'
CMS_lumi.writeExtraText = 1
#CMS_lumi.extraText = 'Preliminary'
CMS_lumi.extraText = 'Simulation'

iPos    = 0
iPeriod = 0

gStyle.SetOptFit(0)

def loadcanvas(name):
    canvas = TCanvas(name,name,400,20,1400,1000)
    canvas.SetFillColor(0)
    canvas.SetBorderMode(0)
    canvas.SetFrameFillStyle(0)
    canvas.SetFrameBorderMode(0)
    canvas.SetTickx(0)
    canvas.SetTicky(0)
    return canvas

def loadlegend(top, bottom, left, right):
    relPosX    = 0.001
    relPosY    = 0.005
    posX = 1 - right - relPosX*(1-left-right)
    posY = 1 - top - relPosY*(1-top-bottom)
    legendOffsetX = 0.0
    legendOffsetY = - 0.05
    textSize   = 0.05
    textFont   = 60
    legendSizeX = 0.4
    legendSizeY = 0.2
    legend = TLegend(posX-legendSizeX+legendOffsetX,posY-legendSizeY+legendOffsetY,posX+legendOffsetX,posY+legendOffsetY)
    legend.SetTextSize(textSize)
    legend.SetLineStyle(0)
    legend.SetBorderSize(0)
    return legend

def makeFileList(path):
    files = os.listdir(path)
    root_files = [path+f for f in files if f.endswith(".root")]
    return root_files
    #return root_files

commonMCPath = '/eos/uscms/store/group/lpcml/rchudasa/MCGenerationRun3/'
# tau_3p7= commonMCPath+"HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_MLAnalyzer_ntuples_v1/241209_155112/0000/"
# tau_3p7fList = makeFileList(tau_3p7)
# fileList = tau_3p7fList[0:2]

        
#print(fileList)

dataDict = {
    '3p7':{
        'totalEve':55152, 
        'path':makeFileList(commonMCPath+'HToAATo4Tau_hadronic_tauDecay_M3p7_Run3_2023/3p7_MLAnalyzer_ntuples_v1/241209_155112/0000/')
    },
    '14':{
        'totalEve': 60662,
        'path':makeFileList(commonMCPath+"HToAATo4Tau_hadronic_tauDecay_M14_Run3_2023/14_MLAnalyzer_ntuples_v1/241209_155230/0000/")
    },
    'QCD':{
        'totalEve': 198000,
        'path':makeFileList(commonMCPath+"GEN_SIM_QCD_pt15to7000_Run3Summer23GS/QCD_MLAnalyzer_ntuples_v1/241209_155310/0000/")
    }
}

selections = ['Trigger','Kinematic cuts','2Tau','Mass','HE edge removal']

for key,value in dataDict.items():
    print('-----------------', key, '----------------------------')
    totalEve = value['totalEve']
    fileList = value['path']
    #print(f"Key: {key}, Total Events: {totalEve}, Path: {fileList}")
    #print(key,value)
    histos={}
    files_ = []

    for path in range(len(fileList)):
        #print(path, fileList[path])
        files_.append( TFile.Open(fileList[path]) )
        tmp_hlt = files_[-1].Get('fevt/hNpassed_hlt')
        tmp_kin = files_[-1].Get('fevt/hNpassed_kin')
        tmp_mva = files_[-1].Get('fevt/hNpassed_2Tau')
        tmp_mass= files_[-1].Get('fevt/hNpassed_mTT')
        tmp_sel = files_[-1].Get('fevt/h_sel')
        #print(path, tmp_hlt.GetBinContent(1), " " , tmp_hlt.GetBinContent(2))
        if path==0:
            histos['hlt']     = tmp_hlt.Clone()
            histos['kin']     = tmp_kin.Clone()
            histos['mva']     = tmp_mva.Clone()
            histos['mass']    = tmp_mass.Clone()
            histos['all_sel'] = tmp_sel.Clone()
        else:
            histos['hlt'].Add(tmp_hlt)
            histos['kin'].Add(tmp_kin)
            histos['mva'].Add(tmp_mva)
            histos['mass'].Add(tmp_mass)
            histos['all_sel'].Add(tmp_sel)
    
    tmp_hlt.Reset()
    tmp_kin.Reset()
    tmp_mva.Reset()
    tmp_mass.Reset()
    tmp_sel.Reset()

    #print("Total events processed: " , histos['all_sel'].GetBinContent(1)+histos['all_sel'].GetBinContent(2))
    print("Trigger: ", histos['hlt'].GetBinContent(2))
    print("Kinematic cuts: ", histos['kin'].GetBinContent(2))
    print("2Tau: ", histos['mva'].GetBinContent(2))
    print("Mass: ", histos['mass'].GetBinContent(2))
    print("Edge HE removed:", histos['all_sel'].GetBinContent(2))