
import pandas as pd
import json
import os
import glob
import psycopg2  
import streamlit as st



def get_files(filepath):
    all_files=[]
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.fspath(f))
    return all_files

def get_df(file_lst,seq):
    f_data=[]
    cols=[]
    df_lst = []
    
    
    for i in file_lst:
        #reading the json files
        f = open(i)
        j_data = json.load(f)
        f.close
        
        #extracting year , state/country names
        state = i.split('\\')[-3]
        year = i.split('\\')[-2]
        file = i.split('\\')[-1]
        
        # creating dataframes according to files
        if seq == 0: 
            #aggregated transaction data
           
            for i in range(len(j_data['data']['transactionData'])):
                f_data.append([state,year,file,j_data['data']['from'],j_data['data']['to'],
                          j_data['data']['transactionData'][i]['name'],
                          j_data['data']['transactionData'][i]['paymentInstruments'][0]['type'],
                          j_data['data']['transactionData'][i]['paymentInstruments'][0]['count'],
                          j_data['data']['transactionData'][i]['paymentInstruments'][0]['amount']
                          ])
            cols = ['Country/State','Year','File','From','To','Name','Type','Count','Amount']

                
        if seq == 1:
           
            for i in range(0,11):
                #aggregated user data
                f_data.append([state,year,file,j_data['data']['aggregated']['registeredUsers'],
                              j_data['data']['aggregated']['appOpens'],
                              j_data['data']['usersByDevice'][i]['brand'],
                              j_data['data']['usersByDevice'][i]['count'],
                              j_data['data']['usersByDevice'][i]['percentage']
                              ])
                cols = ['Country/State','Year','File','Registered Users','App Opens','Brand','Count','Percentage']
                

                
                
        if  seq == 2:
            #map transaction data
            for i in range(len(j_data['data']['hoverDataList'])):
                
                f_data.append([state,year,file,j_data['data']['hoverDataList'][i]['name'],
                             j_data['data']['hoverDataList'][i]['metric'][0]['type'],
                             j_data['data']['hoverDataList'][i]['metric'][0]['count'],
                             j_data['data']['hoverDataList'][i]['metric'][0]['amount'],
                             ])
                cols = ['Country/State','Year','File','Name','Type','Count','Amount']

                
                
        if seq == 3:
            #map user data
            y = j_data['data']['hoverData']
            k = list(y.items())
            for i in range(len(k)):
                f_data.append([year,file,k[i][0],k[i][1]['registeredUsers'],
                               k[i][1]['appOpens']])
                cols = ['Year','File','Country/State','Registered_Users','App Opens']

                
        if seq == 4:#top transaction data
            #statewise
            for i in range(len(j_data['data']['states'])):
                f_data.append([year, file,j_data['data']['states'][i]['entityName'],
                             j_data['data']['states'][i]['metric']['type'],
                             j_data['data']['states'][i]['metric']['count'],
                             j_data['data']['states'][i]['metric']['amount']
                             ])
                
                cols=['Year','File','Entity_Name','Type','Count','Amount']

        
            #districtwise
            for i in range(len(j_data['data']["districts"])):
                f_data.append([year,file,j_data['data']['districts'][i]['entityName'],
                                 j_data['data']['districts'][i]['metric']['type'],
                                 j_data['data']['districts'][i]['metric']['count'],
                                 j_data['data']['districts'][i]['metric']['amount']])
                

        
            #pincode wise
            for i in range(len(j_data['data']['pincodes'])):
                f_data.append([year,file,j_data['data']['pincodes'][i]['entityName'],
                               j_data['data']['pincodes'][i]['metric']['type'],
                               j_data['data']['pincodes'][i]['metric']['count'],
                               j_data['data']['pincodes'][i]['metric']['amount']])
                

            
            
        if seq==5:
            #districtwise
            for i in range(len(j_data['data']['districts'])):
                f_data.append([year,file,j_data['data']['districts'][i]['name'],
                              j_data['data']['districts'][i]['registeredUsers']])
                
            

            #pincodewise
            for i in range(len(j_data['data']['pincodes'])):
                f_data.append([year,file,j_data['data']['pincodes'][i]['name'],
                            j_data['data']['pincodes'][i]['registeredUsers']])
                
                cols = ['Year','File','District/Pincode','registeredusers']
                



        df = pd.DataFrame(f_data, columns = cols)
        
        return df
    

def get_dframe_lst():
    init_path = os.fspath("G:\\GUVI_ZEN_PY\\pYTHON\\PhonePe\\pulse-master\\data")
    p1 = ['aggregated', 'map', 'top']
    p2 = ['transaction','user']
    file_lst = []
    for i in p1:
        for j in p2:
            
            new_path = f"{str(init_path)}\\{i}\\{j}"
            file_lst.append(get_files(new_path))
    dframe_lst=[]
    #print(file_lst[0])
    for i in range(len(file_lst)):
        dframe_lst.append(get_df(file_lst[i],i))
    return dframe_lst


get_dframe_lst()





