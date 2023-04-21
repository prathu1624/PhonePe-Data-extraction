import pandas as pd
import os
import json
import psycopg2
from sqlalchemy import create_engine
import streamlit as st
import plotly.express as px





#Aggregated transaction state
path = "G:/GUVI_ZEN_PY/pYTHON/PhonePe/pulse-master/data/aggregated/transaction/country/india/state"
state_lst = os.listdir(path)

cols = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [],
       'Transaction_count': [], 'Transaction_amount': []}
for i in state_lst:
    p_1 = path + "/" +i
    yr_list = os.listdir(p_1)
    for y in yr_list:
        p_y = p_1+"/"+y
        agg_y = os.listdir(p_y)
        for file in agg_y:
            ag_file = p_y+"/"+file
            f = open(ag_file,'r')
            j_data = json.load(f)
            for f in j_data['data']['transactionData']:
                
                name = f['name']
                count = f['paymentInstruments'][0]['count']
                amount = f['paymentInstruments'][0]['amount']
                cols['Transaction_type'].append(name)
                cols['Transaction_count'].append(count)
                cols['Transaction_amount'].append(amount)
                cols['State'].append(i)
                cols['Year'].append(y)
                cols['Quarter'].append(int(file.strip('.json')))
                
agg_tr = pd.DataFrame(cols)
# path_csv = 'G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv'
# c_path = os.chdir(path_csv)
# agg_tr.to_csv("agg_tr.csv")
                





#Aggregated transaction Country
#path = "G:/GUVI_ZEN_PY/pYTHON/PhonePe/pulse-master/data/aggregated/transaction/country/india"
#year_lst = os.listdir(path)

#cols = {'Country': [], 'Year': [], 'Quarter': [], 'Transaction_type': [],
 #      'Transaction_count': [], 'Transaction_amount': []}
#for i in range(0,5):
  #  p_1 = path + "/" + year_lst[i]
   # file_list = os.listdir(p_1)
    #for file in file_list:
     #   j_file = p_1 + '/' + file
      #  f = open(j_file,'r')
       # j_data = json.load(f)
        #for f in j_data['data']['transactionData']:
                
         #       name = f['name']
          #      count = f['paymentInstruments'][0]['count']
           #     amount = f['paymentInstruments'][0]['amount']
            #    cols['Transaction_type'].append(name)
             #   cols['Transaction_count'].append(count)
              #  cols['Transaction_amount'].append(amount)
               # cols['Country'].append('India')
                #cols['Year'].append(year_lst[i])
                #cols['Quarter'].append(int(file.strip('.json')))
                
#agg_transIND = pd.DataFrame(cols)





#aggregated user state
path = "G:/GUVI_ZEN_PY/pYTHON/PhonePe/pulse-master/data/aggregated/user/country/india/state"
ustate_lst = os.listdir(path)

u_col = {'State':[],'Year':[],'Quarter':[],'Brand':[],'Count':[],'Percentage':[]}

for i in ustate_lst:
    p_1 = path + "/" +i
    yr_list = os.listdir(p_1)
    for y in yr_list:
        p_y = p_1+"/"+y
        aggu_y = os.listdir(p_y)
        for file in aggu_y:
            agu_file = p_y+"/"+file
            f = open(agu_file,'r')
            uj_data = json.load(f)
            try:
                for g in uj_data['data']['usersByDevice']:
                    brand = g['brand']
                    count = g['count']
                    percentage = g['percentage']
                    u_col['Brand'].append(brand)
                    u_col['Count'].append(count)
                    u_col['Percentage'].append(percentage)
                    u_col['State'].append(i)
                    u_col['Year'].append(y)
                    u_col['Quarter'].append(int(file.strip('.json')))
            except:
                pass
agg_user = pd.DataFrame(u_col)
# path_csv = 'G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv'
# c_path = os.chdir(path_csv)
# agg_user.to_csv("agg_user.csv")
                





# map transaction data state
path = "G:/GUVI_ZEN_PY/pYTHON/PhonePe/pulse-master/data/map/transaction/hover/country/india/state"
m_state_lst = os.listdir(path)
m_col = {'State':[],'Year':[],'Quarter':[],'district':[],'Count':[],'Amount':[]}
for i in m_state_lst:
    p_1 = path + "/" +i
    myr_list = os.listdir(p_1)
    for y in myr_list:
        p_y = p_1+"/"+y
        mapt_y = os.listdir(p_y)
        for file in mapt_y:
            mapt_file = p_y+"/"+file
            f = open(mapt_file,'r')
            mt_data = json.load(f)
            try:
                for h in mt_data['data']['hoverDataList']:
                    district = h['name']
                    count = h['metric'][0]['count']
                    amount = h['metric'][0]['amount']
                    m_col['district'].append(district)
                    m_col['Count'].append(count)
                    m_col['Amount'].append(amount)
                    m_col['State'].append(i)
                    m_col['Year'].append(y)
                    m_col['Quarter'].append(int(file.strip('.json')))
            except:
                pass
                    
map_df = pd.DataFrame(m_col)
# path_csv = 'G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv'
# c_path = os.chdir(path_csv)
# map_df.to_csv("map_df.csv")
                    





#map user data state
path = "G:/GUVI_ZEN_PY/pYTHON/PhonePe/pulse-master/data/map/user/hover/country/india/state"
mu_stlst = os.listdir(path)
mu_cl = {"State":[],"year":[],"Quarter":[],"District":[],"Registeredusers":[],"Appopens":[]}

for i in mu_stlst:
    p_1 = path + "/" +i
    uyr_list = os.listdir(p_1)
    for y in uyr_list:
        p_y = p_1+"/"+y
        mapu_y = os.listdir(p_y)
        for file in mapu_y:
            mapu_file = p_y+"/"+file
            f = open(mapu_file,'r')
            mu_data = json.load(f)
            try:
                for j in mu_data['data']['hoverData']:
                    district = j
                    registered_users = mu_data['data']['hoverData'][j]['registeredUsers']
                    appopens = mu_data['data']['hoverData'][j]['appOpens']
                    mu_cl["District"].append(district)
                    mu_cl['Registeredusers'].append(registered_users)
                    mu_cl['Appopens'].append(appopens)
                    mu_cl['State'].append(i)
                    mu_cl['year'].append(y)
                    mu_cl['Quarter'].append(int(file.strip('.json')))
            except:
                pass
            
mapu_df = pd.DataFrame(mu_cl)
# path_csv = 'G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv'
# c_path = os.chdir(path_csv)
# mapu_df.to_csv("mapu.csv")





#top transaction data district wise 
path = "G:/GUVI_ZEN_PY/pYTHON/PhonePe/pulse-master/data/top/transaction/country/india/state"
topst_list = os.listdir(path)
top_cl = {'State':[],'Year':[],'Quarter':[],'District':[],'Count':[],'Amount':[]}

for i in topst_list:
    p_1 = path + "/" +i
    tyr_list = os.listdir(p_1)
    for y in tyr_list:
        p_y = p_1+"/"+y
        topt_y = os.listdir(p_y)
        for file in topt_y:
            topt_file = p_y+"/"+file
            f = open(topt_file,'r')
            topt_data = json.load(f)
            try:
                for k in topt_data['data']['districts']:
                    dist = k['entityName']
                    count= k['metric']['count']
                    amount = k['metric']['amount']
                    top_cl['District'].append(dist)
                    top_cl['Count'].append(count)
                    top_cl['Amount'].append(amount)
                    top_cl['State'].append(i)
                    top_cl['Year'].append(y)
                    top_cl['Quarter'].append(int(file.strip('.json')))
            except:
                pass
topdf = pd.DataFrame(top_cl)
# path_csv = 'G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv'
# c_path = os.chdir(path_csv)
# topdf.to_csv("topdf.csv")





#top transaction pincode wise
top_cl = {'State':[],'Year':[],'Quarter':[],'Pincode':[],'Count':[],'Amount':[]}

for i in topst_list:
    p_1 = path + "/" +i
    tyr_list = os.listdir(p_1)
    for y in tyr_list:
        p_y = p_1+"/"+y
        topt_y = os.listdir(p_y)
        for file in topt_y:
            topt_file = p_y+"/"+file
            f = open(topt_file,'r')
            topt_data = json.load(f)
            try:
                for k in topt_data['data']['pincodes']:
                    dist = k['entityName']
                    count= k['metric']['count']
                    amount = k['metric']['amount']
                    top_cl['Pincode'].append(dist)
                    top_cl['Count'].append(count)
                    top_cl['Amount'].append(amount)
                    top_cl['State'].append(i)
                    top_cl['Year'].append(y)
                    top_cl['Quarter'].append(int(file.strip('.json')))
            except:
                pass
topdf_p = pd.DataFrame(top_cl)
# path_csv = 'G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv'
# c_path = os.chdir(path_csv)
# topdf_p.to_csv("topdf_p.csv")





#top user data district wise
path = "G:/GUVI_ZEN_PY/pYTHON/PhonePe/pulse-master/data/top/user/country/india/state"
user_lst = os.listdir(path)
user_cl = {'State':[],'Year':[],'Quarter':[],'Name':[],'RegisteredUsers':[]}

for i in user_lst:
    p_1 = path + "/" +i
    tu_list = os.listdir(p_1)
    for y in tu_list:
        p_y = p_1+"/"+y
        topu_y = os.listdir(p_y)
        for file in topu_y:
            topu_file = p_y+"/"+file
            f = open(topu_file,'r')
            topu_data = json.load(f)
            try:
                for l in topu_data['data']['districts']:
                    dist = l['name']
                    users= l['registeredUsers']
                    user_cl['Name'].append(dist)
                    user_cl['RegisteredUsers'].append(users)
                    user_cl['State'].append(i)
                    user_cl['Year'].append(y)
                    user_cl['Quarter'].append(int(file.strip('.json')))
            except:
                pass
topu_df = pd.DataFrame(user_cl)
# path_csv = 'G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv'
# c_path = os.chdir(path_csv)
# topu_df.to_csv("topu_df.csv")





#top user data pincode wise
path = "G:/GUVI_ZEN_PY/pYTHON/PhonePe/pulse-master/data/top/user/country/india/state"
user_lst = os.listdir(path)
pin_cl = {'State':[],'Year':[],'Quarter':[],'Pincode':[],'RegisteredUsers':[]}

for i in user_lst:
    p_1 = path + "/" +i
    tu_list = os.listdir(p_1)
    for y in tu_list:
        p_y = p_1+"/"+y
        topu_y = os.listdir(p_y)
        for file in topu_y:
            topu_file = p_y+"/"+file
            f = open(topu_file,'r')
            topu_data = json.load(f)
            try:
                for l in topu_data['data']['pincodes']:
                    pin = l['name']
                    users= l['registeredUsers']
                    pin_cl['Pincode'].append(pin)
                    pin_cl['RegisteredUsers'].append(users)
                    pin_cl['State'].append(i)
                    pin_cl['Year'].append(y)
                    pin_cl['Quarter'].append(int(file.strip('.json')))
            except:
                pass
pin_df = pd.DataFrame(pin_cl)
# path_csv = 'G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv'
# c_path = os.chdir(path_csv)
# pin_df.to_csv("pin_df.csv")



#Inserting csv files into postgresql


conn_string = conn_string = "postgresql://postgres:prathu123@localhost:5432/guvi_phonepe"




db = create_engine(conn_string)
conn = db.connect()



# #agg_trans table
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/agg_tr.csv")
# df.to_sql('agg_trans', con = conn, if_exists='replace', index = False)


# In[ ]:


# #agg_users table
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/agg_user.csv")
# df.to_sql('agg_user', con = conn, if_exists='replace', index = False)


# In[ ]:


# # map_trans table
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/map_df.csv")
# df.to_sql('map_trans', con = conn, if_exists='replace', index = False)


# In[ ]:


# #map users table
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/mapu.csv")
# df.to_sql('map_user', con = conn, if_exists='replace', index = False)


# In[ ]:


# #top transaction district
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/topdf.csv")
# df.to_sql('topt_dst', con = conn, if_exists='replace', index = False)


# In[ ]:


# #top transaction pincode
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/topdf_p.csv")
# df.to_sql('topt_pin', con = conn, if_exists='replace', index = False)


# In[ ]:


# #top user district
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/topu_df.csv")
# df.to_sql('topu_dist', con = conn, if_exists='replace', index = False)


# In[ ]:


# #top user pincode
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/pin_df.csv")
# df.to_sql('topu_pin', con = conn, if_exists='replace', index = False)


# In[ ]:


# #states latitude and longitude 
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/state_lat_lon1.csv")
# df.to_sql('state_co', con = conn, if_exists='replace', index = False)


# In[ ]:


# # districts latitude longitude
# df = pd.read_csv("G:/GUVI_ZEN_PY/pYTHON/PhonePe/csv/dist_latlon1.csv")
# df.to_sql('districts', con = conn, if_exists='replace', index = False)


# In[ ]:


st.title(":violet[PhonePe Data Visualization]")
st.header(':blue[transaction and user device analysis]')


    # In[4]:


    #queries to fetch data
q1 = 'select * from agg_trans'
ag_trans = pd.read_sql(q1, con = conn)

q2 = 'select * from agg_user'
ag_user = pd.read_sql(q2, con =conn)

q3 = 'select * from map_trans'
map_tr = pd.read_sql(q3, con = conn)

q4 = 'select * from map_user'
map_us = pd.read_sql(q4, con = conn)

q5 = 'select * from state_co'
state = pd.read_sql(q5, con = conn)

q6 = 'select * from districts'
dist = pd.read_sql(q6, con = conn)


    # In[5]:


    #data preparation
state = state.sort_values(by='state')
state = state.reset_index(drop='True')

agg_tr = ag_trans.groupby(['State']).sum()[['Transaction_count','Transaction_amount']]
agg_tr = agg_tr.reset_index()

ag_trans.rename(columns={'State':'state'},inplace=True)

    # ch_data = state.copy()

    # for column in agg_tr:
    #     ch_data[column]=agg_tr[column]
    # ch_data=ch_data.drop(labels='state', axis=0)


    # In[6]:


map_tr.rename(columns={'district':'District'},inplace=True)


    # In[7]:


state_lt = ['andaman-&-nicobar-islands',
     'andhra-pradesh',
     'arunachal-pradesh',
     'assam',
     'bihar',
     'chandigarh',
     'chhattisgarh',
     'dadra-&-nagar-haveli-&-daman-&-diu',
     'delhi',
     'goa',
     'gujarat',
     'haryana',
     'himachal-pradesh',
     'jammu-&-kashmir',
     'jharkhand',
     'karnataka',
     'kerala',
     'ladakh',
     'lakshadweep',
     'madhya-pradesh',
     'maharashtra',
     'manipur',
     'meghalaya',
     'mizoram',
     'nagaland',
     'odisha',
     'puducherry',
     'punjab',
     'rajasthan',
     'sikkim',
     'tamil-nadu',
     'telangana',
     'tripura',
     'uttar-pradesh',
     'uttarakhand',
     'west-bengal']


    # In[10]:


agg_tr['state'] = pd.Series(data=state_lt)
states_f = pd.merge(ag_trans, state,how='outer',on='state')



dist_f = pd.merge(map_tr, dist, how='outer', on=['State','District'])



    #Streamlit tabs for transaction and user analysis
transaction_analysis, user_analysis = st.tabs(['Transaction_analysis','User_analysis'])


    # In[15]:


    #transaction analysis
with transaction_analysis:
    st.subheader(':violet[Transaction analysis: state and district wise]')
        
    st.write('')
    Year=st.radio('Please select the year',('2018','2019','2020','2021','2022'), horizontal=True)
        
    st.write('')
    Quarter=st.radio('Please select the quarter',('1','2','3','4'),horizontal=True)
        
    st.write('')
    Year=int(Year)
    Quarter=int(Quarter)
        
    state_plot= states_f[(states_f['Year']==Year)&(states_f['Quarter']==Quarter)]
    dist_plot = dist_f[(dist_f['Year']==Year)&(dist_f['Quarter']==Quarter)]
        
        


    # In[14]:


    state_plot_f = state_plot.groupby(['state','Year','Quarter','Latitude','Longitude']).sum()
    state_plot_f = state_plot_f.reset_index()


    # In[12]:


    st_codes = ['AN', 'AD', 'AR', 'AS', 'BR', 'CH', 'CG', 'DNHDD', 'DL', 'GA',
                      'GJ', 'HR', 'HP', 'JK', 'JH', 'KA', 'KL', 'LA', 'LD', 'MP', 'MH',
                      'MN', 'ML', 'MZ', 'NL', 'OD', 'PY', 'PB', 'RJ', 'SK', 'TN', 'TS',
                      'TR', 'UP', 'UK', 'WB']
    state_plot_f['codes']=pd.Series(data = st_codes)


    # In[ ]:


    #transaction plot
    fig = px.scatter_geo(state_plot_f,lon=state_plot_f['Longitude'], lat=state_plot_f['Latitude'], hover_name ='state', hover_data=['state', 'Year','Quarter','Transaction_count', 'Transaction_amount'], title="state", size_max=22)
    st.plotly_chart(fig)             
                             


