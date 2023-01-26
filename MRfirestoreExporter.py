#import simplejson as json
import re
import os
import datetime
import threading
import itertools
from time import sleep
from collections import OrderedDict
from google.cloud import firestore
import pandas as pd
import math
 
class MRexporter:
    products = None
 
    def __init__(self):
        self.products = None
        self.products = dict()
 
    pass
  
    def getImpactedRoutineRatio(self, gcpCredentials):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcpCredentials
 
        # Project ID is determined by the GCLOUD_PROJECT environment variable
        db = firestore.Client()
        nbInstancesTestees = 0
        results = {}
        for instanceName in db.collection('approutes').list_documents() :
            print(instanceName.id, '\n')
            for appprescrptn00n in instanceName.collections():
                if appprescrptn00n.id == 'appprescriptions':
                    for dkjgn in appprescrptn00n.list_documents():
                        if dkjgn.get().get('meta.active') == True:
                            for profiles in dkjgn.collections():
                                if profiles.id == 'profiles':
                                    nbRoutinesTestees = 0
                                    nbRoutinesConcernees = 0
                                    for profile in profiles.list_documents():
                                        nbRoutinesTestees +=1
                                        try:
                                            if ((len(profile.get().get('inclusiveCriterias')) > 1) and
                                                (len(profile.get().get('inclusiveStrictCriterias')) > 1)):
                                                print('condition verifiée\n')
                                                nbRoutinesConcernees +=1
                                        except:
                                            print('no existing criteria category\n')
                                            pass
                                    results[instanceName.id] = round(nbRoutinesConcernees*100/nbRoutinesTestees,1)
                                    results = {x:y for x,y in results.items() if y!=0}
            nbInstancesTestees+=1
            if nbInstancesTestees%10 == 0:
                print(nbInstancesTestees, " instances testées\n")
                print(results)
                       
        return dict(sorted(results.items(), key=lambda item: item[1], reverse = True))
   

   
    def getImpactForOneInstance(self, appInstanceName, gcpCredentials):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcpCredentials
 
        # Project ID is determined by the GCLOUD_PROJECT environment variable
        db = firestore.Client()
        collections = db.collections()
       
        nbRoutinesTestees = 0
        nbConditionsVerifiees = 0
 
        for coll in collections:
            if coll.id == 'approutes':
                for appprescrptn00n in coll.document(appInstanceName).collection('appprescriptions').list_documents():
                    if appprescrptn00n.get().get('meta.active') == True:
                        try:
                            listProfiles = []
                            boolean = 0
                            for profile in appprescrptn00n.collection('profiles').list_documents():
                                nbRoutinesTestees +=1
                                try:
                                    if ((len(profile.get().get('inclusiveCriterias')) > 1) and
                                        (len(profile.get().get('inclusiveStrictCriterias')) > 1)):
                                        nbConditionsVerifiees +=1
                                    result = round(nbConditionsVerifiees*100/nbRoutinesTestees,1)
                                except :
                                    print('Criteria category do not exist\n')
                        except:
                            print('No profile section\n')
        print(result,'% de routines potentiellement affectées par la migration')
       
        
    def getCriteriasForCombination(self, appInstanceName, gcpCredentials):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcpCredentials
 
        # Project ID is determined by the GCLOUD_PROJECT environment variable
        db = firestore.Client()
        collections = db.collections()
       
        listCriterias = []
        for coll in collections:
            if coll.id == 'approutes':
                try :
                    for appprescrptn00n in coll.document(appInstanceName).collection('appprescriptions').list_documents():
                        if appprescrptn00n.get().get('meta.active') == True:
                            try:
                                for criteria in appprescrptn00n.collection('criteria').list_documents():
                                    listCriterias.append(criteria.id)
                            except:
                                print('No criteria section\n')
                except :
                    print('No appprescription section\n')
        return listCriterias
   
    def createPrescriptionTable(self, appInstanceName, gcpCredentials):
        routine_table = pd.DataFrame(columns = ['profileId','profilesMust', 'profilesShould', 'profilesMustNot', 'profilesMustItem', 'isDefault'])
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcpCredentials
        db = firestore.Client()
        for coll in db.collection('approutes').list_documents():
            if coll.id == appInstanceName:
                for appprescrptn00n in coll.collection('appprescriptions').list_documents():
                    if appprescrptn00n.get().get('meta.active') == True:
                        for i, profile in enumerate(appprescrptn00n.collection('profiles').list_documents()):
                            try:
                                routine_table.loc[i] = [profile.id,
                                                        [extract_data(x)[1] for x in profile.get().get('inclusiveStrictCriterias')],
                                                        [extract_data(x)[1] for x in profile.get().get('inclusiveCriterias')],
                                                        [extract_data(x)[1] for x in profile.get().get('exclusiveCriterias')],
                                                        [extract_data(x)[1] for x in profile.get().get('inclusiveRootItemCriterias')],
                                                        profile.get().get('isDefault')]
                            except:
                                print('error with ', profile.id)
                                pass
        return routine_table
 
    def storeMismatchs(self, combinations, appInstanceName, gcpCredentials):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcpCredentials
        table = pd.DataFrame(columns = ['Combination', 'Unchanged', 'Must to Should', 'Should to Must',
                                    'Added to No Weight', 'Removed from Weighted', 'Wei', 'noWei', 'defaultWei', 'defaultNoWei'])
       
       
        #Garder en memoire la liste des élements pour routine 
        prescr_table = self.createPrescriptionTable(appInstanceName, gcpCredentials)
        print('prescr_table created !\n')
  
        i, defaultNoWei_WholeSet, defaultWei_WholeSet = 0, 0, 0
 
        for count, cb in enumerate(combinations):
            if count %500 ==0:
                print('Nous sommes à la combinaison numero : ', count, '\n')
            #faire le scoring sur le tableau en memoire
            Wei, noWei, defaultWei, defaultNoWei = scoring(cb, prescr_table)
     
            if defaultWei.bool() == True:
                defaultWei_WholeSet +=1
            if defaultNoWei.bool() == True:
                defaultNoWei_WholeSet +=1

            match = (Wei == noWei)
            if match == False:
                comp = compare_organisations(prescr_table, Wei, noWei)

                unchanged, must2Should, should2Must,added,removed  = comp[0], comp[1], comp[2], comp[3], comp[4]

                table.loc[i] = [cb, unchanged, must2Should, should2Must, added, removed, Wei, noWei, defaultWei.bool(), defaultNoWei.bool()]
                i+=1
 
            defaultWei_WholeSet = defaultWei_WholeSet*100/len(cb)
            defaultNoWei_WholeSet = defaultNoWei_WholeSet*100/len(cb)
        return table, defaultWei_WholeSet, defaultNoWei_WholeSet

def weightedMustScoring(combination, must, should, mustItem, mustNot):
    nbMust = len(common_elements(combination,must))
    nbShould = len(common_elements(combination,should))
    nbMustItem = len(common_elements(combination,mustItem))
    return nbMust*10 + nbShould + nbMustItem*10
  
def noWeightScoring(combination,must,should, mustItem, mustNot):
    nbMust = len(common_elements(combination,must))
    nbShould = len(common_elements(combination,should))
    nbMustItem = len(common_elements(combination,mustItem))
    return nbMust + nbShould + nbMustItem

def extract_data(s):
    pattern = re.compile(r'(?P<categorie>[\w\.-]+)_(?P<criteria>[\w\-]+)')
    match = pattern.match(s)
    if match == None:
        pattern = re.compile(r'(?P<categorie>[\w\.-]+):(?P<criteria>[\w\-]+)')
        match = pattern.match(s)
    name = match.groups()
    return name
 
def common_elements_extracted(list1, list2):
    a =  [element for element in list1 if element in list2 if element !=0]
    return [extract_data(x)[1] for x in a]
 
def common_elements(list1, list2):
    return [element for element in list1 if element in list2 if element !=0]
 
def parseCriterias(s : str):
    if s == 0:
        return s
    return s.split(",")
 

def combination(rawCrit):
    allCrit = allCriterias(rawCrit)
    criterias, criteriaCategory = allCrit[0], allCrit[1]

   
    def lazy_product(*iterables):
        if not iterables:
            yield()
            return
        it0 = iterables[0]
        for x in it0:
            for rest in lazy_product(*iterables[1:]):
                yield(x,) + rest
                
    iterator = lazy_product(*criterias)

    combin = []
    for count, it in enumerate(iterator):
        combin.append(it)
        if count == 1000:
            break
   
    print("Combination number : ", len(combin))
    if len(combin) >= 5000:
        sample = pd.DataFrame(data =combin)
        a = sample.sample(frac = 0.1)
        records = a.to_records(index=False)
        sampl = list(records)
        print("Combination number reduced : ", len(sampl))
        return sampl
    return combin
 

def allCriterias(rawCrit):
    criteriaCategory = []
    allCrit = []

    splittedCrit = [extract_data(rawCrit[i]) for i in range(len(rawCrit))]

    iterator = itertools.groupby(splittedCrit, lambda x:x[0])
    groupedCrit = [list(group) for element, group in iterator]

    for _ in range(len(groupedCrit)):
        if len(groupedCrit[_]) >1:
            criteriaCategory.append(groupedCrit[_][0][0])

    for x in groupedCrit:
        if len(x)>1:
            l = [x[j][-1] for j in range(len(x))]
            allCrit.append(l)
            
    return(allCrit,criteriaCategory)

   
def defaultRatio_MismatchSet(table): #in mismtachs ensemble
    ratioWei= (len(table.loc[table["defaultWei"] == True])*100)/len(table)
    ratioNoWei= (len(table.loc[table["defaultWei"] == True])*100)/len(table)
    return(ratioWei, ratioNoWei)
   
def compare_organisations(df, id1, id2):
    
    r1 = df.loc[df['profileId'] == id1]['profilesMust'].tolist()+df.loc[df['profileId'] == id1]['profilesShould'].tolist()+df.loc[df['profileId'] == id1]['profilesMustNot'].tolist()+df.loc[df['profileId'] == id1]['profilesMustItem'].tolist()
    
    r2 = df.loc[df['profileId'] == id2]['profilesMust'].tolist()+df.loc[df['profileId'] == id2]['profilesShould'].tolist()+df.loc[df['profileId'] == id2]['profilesMustNot'].tolist()+df.loc[df['profileId'] == id2]['profilesMustItem'].tolist()
    
    sameCat = [common_elements(r1[i],r2[i]) for i in range(len(r1))]
    sameCat = [item for sublist in sameCat for item in sublist]
        
    must2Should = [x for x in common_elements(r1[0], r2[1])]
    should2Must = [x for x in common_elements(r1[1], r2[0])]
        
    #flatten r1 and r2
    listr1 = [item for sublist in r1 for item in sublist if item!=0]
    listr2 = [item for sublist in r2 for item in sublist if item!=0]

    added = [ele for ele in listr2 if ele not in common_elements(listr1,listr2)]
    removed = [ele for ele in listr1 if ele not in listr2]
    return (sameCat, must2Should, should2Must, added, removed)

def scoring(cb, dataframe):      
    wmScores = {}
    nwScores = {}
    wmIsDefault, nwIsDefault = 0, 0

    for i in range(len(dataframe)):
        try:
            if common_elements(cb,dataframe.iloc[i]['profilesMustNot']) == []:
                profilesMust, profilesShould, profilesMustNot, profilesMustItem = dataframe.iloc[i]['profilesMust'], dataframe.iloc[i]['profilesShould'], dataframe.iloc[i]['profilesMustNot'], dataframe.iloc[i]['profilesMustItem']

                wmScores[dataframe.iloc[i]['profileId']] = weightedMustScoring(cb, profilesMust, profilesShould, profilesMustNot, profilesMustItem)
                nwScores[dataframe.iloc[i]['profileId']] = noWeightScoring(cb, profilesMust, profilesShould, profilesMustNot, profilesMustItem)


                wmSelectedRoutine, nwSelectedRoutine = list({k: v for k, v in sorted(wmScores.items(), key=lambda item:item[1], reverse = True)})[0], list({k: v for k, v in sorted(nwScores.items(), key=lambda item:item[1], reverse = True)})[0]
        except:
            print("error with dataframe.iloc[i]['profilesMustNot'] : ", dataframe.iloc[i]['profilesMustNot'])
            pass
    wmIsDefault = dataframe.loc[dataframe['profileId'] == wmSelectedRoutine]['isDefault']
    nwIsDefault = dataframe.loc[dataframe['profileId'] == nwSelectedRoutine]['isDefault']

    return wmSelectedRoutine, nwSelectedRoutine, wmIsDefault, nwIsDefault

    