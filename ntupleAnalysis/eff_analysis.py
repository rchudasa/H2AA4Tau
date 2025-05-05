import pandas as pd
import csv

df = pd.read_csv('csv_files/trigger_eff_3p7.csv',names=['Trigger_Name','Events_passed'],header=None)
df = df[df['Events_passed']>10]
print(df.head())
print("Trigger eff shape", df.shape)

#df2 = pd.read_csv('E2Etau_signal_bkg_MC_HLT_2023_menu.csv',usecols=['Trigger_Name', 'Actual_lumi','Effective_lumi','dataset(s)'])
df2 = pd.read_csv('csv_files/E2Etau_signal_bkg_MC_HLT_2023_menu.csv',usecols=['Trigger_Name', 'Actual_lumi','Effective_lumi','datasets'])
print(df2.columns)

print(df2.head())
df2['Trigger_Name'] = df2['Trigger_Name'].str.replace('_v','')
print("HLT menu", df2.shape)


merged_df = pd.merge(df,df2,on='Trigger_Name', how='left')
print("Merged df shape", merged_df.shape)
print(merged_df)

sorted_df = merged_df.sort_values(by='Events_passed',ascending=False)
sorted_df =sorted_df[sorted_df['Effective_lumi']==sorted_df['Actual_lumi']]
sorted_df=sorted_df[(sorted_df['datasets']!='Cosmics') & (sorted_df['datasets']!='Commissioning')]
less_lumi_df = sorted_df[sorted_df['Effective_lumi']>0.27]
#less_lumi_df['total_nEvents'] = 83934 #for 3p7 VBF samples
#less_lumi_df['total_nEvents'] = 54677 #for 14 GeV VBF samples
less_lumi_df['total_nEvents'] = 62717 #for 3p7 GeV ggF samples
#less_lumi_df['total_nEvents'] = 63919 #for 14 GeV ggF samples
less_lumi_df['events_eff(%)'] = less_lumi_df['Events_passed']*100/less_lumi_df['total_nEvents']
less_lumi_df.to_csv('csv_files/ggF_mass3p7_triggerEffAnalysis.csv',index=False)
print(less_lumi_df)
# sorted_df=sorted_df[sorted_df['Effective_lumi']>0.27]
# print(sorted_df[0:20])
# print(sorted_df['datasets'].unique())
# sorted_df.to_csv('VBF_output_file_3p7.csv',index=False)
# sorted_df_jetmet = sorted_df[sorted_df['datasets']=='JetMET[0,1]']
# sorted_df_jetmet.to_csv('output_file_sorted_dfjetmet_14.csv',index=False)
# print("sorted_df_jetmet", sorted_df_jetmet.shape)
# triggerNames_AK8 = sorted_df_jetmet[sorted_df_jetmet['Trigger_Name'].str.contains('AK8', case=False, na=False)].sort_values(by='Trigger_Name').reset_index()

# print(triggerNames_AK8['Trigger_Name'])

# triggerNames_MET = sorted_df_jetmet[sorted_df_jetmet['Trigger_Name'].str.contains('PFMET', case=False, na=False)].sort_values(by='Trigger_Name').reset_index()
# print(triggerNames_MET['Trigger_Name'])