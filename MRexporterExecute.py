from MRfirestoreExporter import MRexporter
from MRfirestoreExporter import *
#import simplejson as json
import datetime
import pandas as pd
import itertools
import streamlit as st
import json
import re
import requests
import warnings
import numpy as np
warnings.filterwarnings("ignore")
from load_css import local_css
#import plotly.graph_objects as go
# from st_aggrid import AgGrid, JsCode, GridOptionsBuilder
import time
local_css("style.css")
 
gcpCredentials = "/Users/athena.brisse/Downloads/dsf-firecamp-portal-bck-dev-firecamp-import-appaccount-a32a041d76f9.json"
# appInstanceName = 'demoapp-lorealsa-laprovencalebio-dmi-pos-dev-std'
appInstanceName = 'haircarediag-lorealsa-kerastase-dmi-web-dev-std'
#appInstanceName = 'demoapp-lorealsa-firecamptest-chl-web-dev-canary'
# prescriptionId = '0jLQVjoU0nJvjy41KRSm'
 
criteriasExporter = MRexporter()
criterias = None
 
#Vérifier impact
 
# verifier création de table
# start = time.time()
# prescrTable = criteriasExporter.createPrescriptionTable(appInstanceName, gcpCredentials)
# end = time.time()
# prescrTable.to_csv('prescrTable.csv')
# print('create prescription table took ', round(end-start,1), ' seconds for 3 routines. \n')
# print(prescrTable)
 
#Obtenir liste des critères de l'instancex
start = time.time()
criterias = criteriasExporter.getCriteriasForCombination(appInstanceName, gcpCredentials)
end = time.time()
print('Get criterias took ', round(end-start,1), ' seconds. \n')
# print('criterias : ', criterias, '\n')


# #start = time.time()
# #test = compare_organisations(prescrTable, '5yHPXX4Fosc5iroh7cQl', 'ZgtXKejzXnaT94wVkwdM')
# #end = time.time()
# #print('compare_organisations took ', round(end-start,1), ' seconds. \n')
# #print(test)
 
 
 
# #Faire des combinaisons
start = time.time()
combinations = combination(criterias)
end = time.time()
print('Combinations took ', round(end-start,1), ' seconds for ', len(criterias),' criterias. \n')
print('There are', len(combinations), 'combinations for this instance. \n')

 
# # combinations = [('gea1', 'acne', '18-34', 'female', 'low', 'dry'), ('gea4', 'homogeneity', '55-99', 'others', 'no', 'dry')]
# # Obtenir prescriptions pour une combinaison
# #start =  time.time()
# #prescrTable = pd.read_csv('prescrTable.csv')
# #prescriptions = scoring(combinations[9:10], prescrTable)
# #end =  time.time()
# #print('Doing scoring took ', round(end-start,1), ' seconds for one combination. \n')
# # print('prescriptions: ',prescriptions, '\n')


st.write('''
# Migration Monitoring
''')
st.subheader("Towards the NoWeight scoring system")
    

    
    
# Créer la table des mismatch
start =  time.time()
table = criteriasExporter.storeMismatchs(combinations, appInstanceName, gcpCredentials)
end =  time.time()
table[0].to_csv('mismatchTableKerastaseDmi.csv')
print('Creating mismatch table took ', round(end-start,1), ' seconds. \n')


my_bar = st.progress(0)
for percent_complete in range(100):
    time.sleep(2)
    my_bar.progress(percent_complete+0.1)
#print('table: ',table[0], '\n')
 
 # Donner résultats
# print('There are ', len(table[0]), 'mismatchs out of',len(combinations), 'combinations. \n')
# print('Change percentage : ', round(len(table[0])*100/len(combinations),2), '%. \n\n\n')
# print('Default profile ratio Weighted in mismatch set:', defaultRatio_MismatchSet(table[0])[0], '%. \n\n')
# print('Default profile ratio No Weight in mismatch set:', defaultRatio_MismatchSet(table[0])[1], '%. \n\n')
# print('Default profile ratio Weighted in whole set :', table[1], '%. \n\n')
# print('Default profile ratio No Weight in whole set :', table[2], '%. \n\n')
   
# # #Enter appInstanceName
# # appInstanceName = st.sidebar.text_input(label ="Enter an instance key")
 
st.write('There are ', len(table[0]), 'mismatchs out of',len(combinations), 'combinations. \n\n')
st.write('Change percentage : ', round(len(table[0])*100/len(combinations),2), '%. \n')
st.write('Default profile ratio Weighted :', defaultRatio_MismatchSet(table[0])[0], '%. \n\n')
st.write('Default profile ratio No Weight :', defaultRatio_MismatchSet(table[0])[1], '%. \n\n')
st.write('Default profile ratio Weighted in whole set :', table[1], '%. \n\n')
st.write('Default profile ratio No Weight in whole set :', table[2], '%. \n\n')
 
st.write('''\n\n''')
 
 
 #Afficher le tableau sur streamlit
st.dataframe(table[0][["Combination","Unchanged","Must to Should","Should to Must","Added to No Weight", "Removed from Weighted"]])
 
#print(weightedMustScoring(['gea0', 'acne', '18-34', 'male', 'verylow', 'dry'],['normal', 'lackoffirmness'] ,['18-34', '35-54', '55-99', 'low', 'high', 'verylow', 'male', 'others'], [], ['female']))


# lnkW = create_link("https://firecamp.modiface.com/app/skindrv2-lorealsa-biotherm-deu-web-production-std/prescription/profile/"+Wei)
#             lnknW = create_link("https://firecamp.modiface.com/app/skindrv2-lorealsa-biotherm-deu-web-production-std/prescription/profile/"+noWei)
#     links.loc[j] = [lnkW,lnknW]