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

histos={}

files_ = []
firstfile = True
fileList = []
for roots,dirs,files in os.walk('/eos/user/r/rchudasa/e2e_project/Tau_data_ntuples/'):
    for name in files:
        file = os.path.join(roots,name)
        fileList.append(file)
        
#print(fileList)

for path in range(len(fileList)):
    print(path, fileList[path])
    files_.append( TFile.Open(fileList[path]) )
    tmp_hlt = files_[-1].Get('fevt/hNpassed_hlt')
    tmp_kin = files_[-1].Get('fevt/hNpassed_kin')
    tmp_mva = files_[-1].Get('fevt/hNpassed_MVA')
    tmp_mass= files_[-1].Get('fevt/hNpassed_mGG')
    tmp_sel = files_[-1].Get('fevt/h_sel')
    print(path, tmp_sel.GetBinContent(1), " " , tmp_sel.GetBinContent(2))
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

   
print("Total events processed: " , histos['all_sel'].GetBinContent(1)+histos['all_sel'].GetBinContent(2))
print("Trigger: ", histos['hlt'].GetBinContent(2))
print("Kinematic cuts: ", histos['kin'].GetBinContent(2))
print("MVA: ", histos['mva'].GetBinContent(2))
print("Mass: ", histos['mass'].GetBinContent(2))
print("Edge HE removed:", histos['all_sel'].GetBinContent(2))
        
