


import pandas as pd
import os
import json




#Aggregated transaction state
path = "PhonePe/pulse-master/data/aggregated/transaction/country/india/state"
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
# path_csv = 'PhonePe/csv'
# c_path = os.chdir(path_csv)
# agg_tr.to_csv("agg_tr.csv")
                





#Aggregated transaction Country
path = "PhonePe/pulse-master/data/aggregated/transaction/country/india"
year_lst = os.listdir(path)

cols = {'Country': [], 'Year': [], 'Quarter': [], 'Transaction_type': [],
       'Transaction_count': [], 'Transaction_amount': []}
for i in range(0,5):
    p_1 = path + "/" + year_lst[i]
    file_list = os.listdir(p_1)
    for file in file_list:
        j_file = p_1 + '/' + file
        f = open(j_file,'r')
        j_data = json.load(f)
        for f in j_data['data']['transactionData']:
                
                name = f['name']
                count = f['paymentInstruments'][0]['count']
                amount = f['paymentInstruments'][0]['amount']
                cols['Transaction_type'].append(name)
                cols['Transaction_count'].append(count)
                cols['Transaction_amount'].append(amount)
                cols['Country'].append('India')
                cols['Year'].append(year_lst[i])
                cols['Quarter'].append(int(file.strip('.json')))
                
agg_transIND = pd.DataFrame(cols)
agg_transIND




#aggregated user state
path = "PhonePe/pulse-master/data/aggregated/user/country/india/state"
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
# path_csv = 'PhonePe/csv'
# c_path = os.chdir(path_csv)
# agg_user.to_csv("agg_user.csv")
                





# map transaction data state
path = "PhonePe/pulse-master/data/map/transaction/hover/country/india/state"
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
# path_csv = 'PhonePe/csv'
# c_path = os.chdir(path_csv)
# map_df.to_csv("map_df.csv")
                    





#map user data state
path = "PhonePe/pulse-master/data/map/user/hover/country/india/state"
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
# path_csv = 'PhonePe/csv'
# c_path = os.chdir(path_csv)
# mapu_df.to_csv("mapu.csv")





#top transaction data district wise 
path = "PhonePe/pulse-master/data/top/transaction/country/india/state"
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
# path_csv = 'PhonePe/csv'
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
# path_csv = 'PhonePe/csv'
# c_path = os.chdir(path_csv)
# topdf_p.to_csv("topdf_p.csv")





#top user data district wise
path = "PhonePe/pulse-master/data/top/user/country/india/state"
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
# path_csv = 'PhonePe/csv'
# c_path = os.chdir(path_csv)
# topu_df.to_csv("topu_df.csv")





#top user data pincode wise
path = "PhonePe/pulse-master/data/top/user/country/india/state"
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
# path_csv = 'PhonePe/csv'
# c_path = os.chdir(path_csv)
# pin_df.to_csv("pin_df.csv")

