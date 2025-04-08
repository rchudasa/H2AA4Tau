import pandas as pd
import csv

df = pd.read_csv('trigger_eff_14.csv',names=['Trigger_Name','Events_passed'],header=None)
df = df[df['Events_passed']>10]
print(df.head())
print("Trigger eff shape", df.shape)

#df2 = pd.read_csv('E2Etau_signal_bkg_MC_HLT_2023_menu.csv',usecols=['Trigger_Name', 'Actual_lumi','Effective_lumi','dataset(s)'])
df2 = pd.read_csv('E2Etau_signal_bkg_MC_HLT_2023_menu.csv',usecols=['Trigger_Name', 'Actual_lumi','Effective_lumi','datasets'])
print(df2.columns)

print(df2.head())
df2['Trigger_Name'] = df2['Trigger_Name'].str.replace('_v','')
print("HLT menu", df2.shape)


merged_df = pd.merge(df,df2,on='Trigger_Name', how='left')
print("Merged df shape", merged_df.shape)
print(merged_df)

sorted_df = merged_df.sort_values(by='Events_passed',ascending=False)
less_lumi_df = sorted_df[sorted_df['Effective_lumi']<0.27]
less_lumi_df.to_csv('output_file_14_less_lumi.csv',index=False)
sorted_df=sorted_df[sorted_df['Effective_lumi']>0.27]
sorted_df =sorted_df[sorted_df['Effective_lumi']==sorted_df['Actual_lumi']]
sorted_df=sorted_df[(sorted_df['datasets']!='Cosmics') & (sorted_df['datasets']!='Commissioning')]
print(sorted_df[0:20])
print(sorted_df['datasets'].unique())
sorted_df.to_csv('output_file_14.csv',index=False)
sorted_df_jetmet = sorted_df[sorted_df['datasets']=='JetMET[0,1]']
sorted_df_jetmet.to_csv('output_file_sorted_dfjetmet_14.csv',index=False)
print("sorted_df_jetmet", sorted_df_jetmet.shape)
triggerNames_AK8 = sorted_df_jetmet[sorted_df_jetmet['Trigger_Name'].str.contains('AK8', case=False, na=False)].sort_values(by='Trigger_Name').reset_index()

print(triggerNames_AK8['Trigger_Name'])

triggerNames_MET = sorted_df_jetmet[sorted_df_jetmet['Trigger_Name'].str.contains('PFMET', case=False, na=False)].sort_values(by='Trigger_Name').reset_index()
print(triggerNames_MET['Trigger_Name'])