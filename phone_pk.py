import pandas as pd
import os
import json
import psycopg2
from sqlalchemy import create_engine
import streamlit as st
import plotly.express as px





#Aggregated transaction state
path = #file_path
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
# path_csv = 'csv_path
# c_path = os.chdir(path_csv)
# agg_tr.to_csv("agg_tr.csv")
                





#Aggregated transaction Country
#path = file_path
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
path = #file_path
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
# path_csv = 'csv_path'
# c_path = os.chdir(path_csv)
# agg_user.to_csv("agg_user.csv")
                





# map transaction data state
path = #file_path
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
# path_csv = csv_path
# c_path = os.chdir(path_csv)
# map_df.to_csv("map_df.csv")
                    





#map user data state
path = #file_path
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
# path_csv = csv_path
# c_path = os.chdir(path_csv)
# mapu_df.to_csv("mapu.csv")





#top transaction data district wise 
path = #file_path
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
# path_csv = csv_path
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
# path_csv = csv_path
# c_path = os.chdir(path_csv)
# topdf_p.to_csv("topdf_p.csv")





#top user data district wise
path = #file_path
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
# path_csv = csv_path
# c_path = os.chdir(path_csv)
# topu_df.to_csv("topu_df.csv")





#top user data pincode wise
path =#file_path
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
# path_csv = csv_path
# c_path = os.chdir(path_csv)
# pin_df.to_csv("pin_df.csv")



#Inserting csv files into postgresql

conn_string = #sql_string



db = create_engine(conn_string)
conn = db.connect()



# #agg_trans table
# df = pd.read_csv("agg_tr.csv")
# df.to_sql('agg_trans', con = conn, if_exists='replace', index = False)



# #agg_users table
# df = pd.read_csv("agg_user.csv")
# df.to_sql('agg_user', con = conn, if_exists='replace', index = False)



# # map_trans table
# df = pd.read_csv("map_df.csv")
# df.to_sql('map_trans', con = conn, if_exists='replace', index = False)



# #map users table
# df = pd.read_csv("mapu.csv")
# df.to_sql('map_user', con = conn, if_exists='replace', index = False)


# #states latitude and longitude 
# df = pd.read_csv("state_lat_lon1.csv")
# df.to_sql('state_co', con = conn, if_exists='replace', index = False)



# # districts latitude longitude
# df = pd.read_csv("dist_latlon1.csv")
# df.to_sql('districts', con = conn, if_exists='replace', index = False)



st.title(":violet[PhonePe Data Visualization]")
st.header(':blue[transaction and user device analysis]')


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
states = pd.read_sql(q5, con = conn)

q6 = 'select * from districts'
dist = pd.read_sql(q6, con = conn)



ag_trans.rename(columns={'State':'state'},inplace=True)
ag_user.rename(columns={'State':'state'}, inplace=True)



states = states.sort_values(by='state')
states = states.reset_index()



#data preparation
agg_tr = ag_trans.groupby(['state','Year']).sum()[['Transaction_count','Transaction_amount']]
agg_tr = agg_tr.reset_index()

agg_usr = ag_user.groupby (['state','Year','Brand']).sum() [['Count','Percentage']]
agg_usr = agg_usr.reset_index()


state_lst = ['andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
            'uttarakhand', 'west-bengal']


states['state']=pd.Series(data=state_lst)




state_f = pd.merge(ag_trans,states, how='outer',on='state')

userst_f = pd.merge(agg_usr,states, how='outer', on='state')



map_tr.rename(columns={'district':'District'},inplace=True)
state_lt = ['Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
            'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
            'Dadra & Nagar Haveli & Daman & Diu', 'Delhi', 'Goa', 'Gujarat',
            'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir', 'Jharkhand',
            'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh',
            'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland',
            'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
            'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
            'Uttarakhand', 'West Bengal']






dist_f = pd.merge(map_tr, dist, how='outer', on=['State','District'])
user_dist = pd.merge(map_us, dist, how = 'outer', on=['State','District'])



#Streamlit tabs for transaction and user analysis
transaction_analysis, user_analysis , payment_types, device_analysis = st.tabs(['Transaction_analysis','User_analysis','Payment types','Device Analysis'])



# transaction analysis
with transaction_analysis:

    st.subheader(':violet[Transaction analysis: state and district wise]')

    st.write('')
    Year=st.radio('Please select the year',('2018','2019','2020','2021','2022'), horizontal=True)

    st.write('')
    Quarter=st.radio('Please select the quarter',('1','2','3','4'),horizontal=True)

    st.write('')
    Year=int(Year)
    Quarter=int(Quarter)
    state_plot = state_f[(state_f['Year']==Year)&(state_f['Quarter']==Quarter)]
    dist_plot = dist_f[(dist_f['Year']==Year)&(dist_f['Quarter']==Quarter)]

    state_plotf = state_plot.groupby(['state','Year','Quarter','Latitude','Longitude']).sum()
    state_plotf = state_plotf.reset_index()

    
    state_plotf['state'] = state_plotf['state'].replace(['andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
            'uttarakhand', 'west-bengal'], ['Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
            'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
            'Dadra & Nagar Haveli & Daman & Diu', 'Delhi', 'Goa', 'Gujarat',
            'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir', 'Jharkhand',
            'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh',
            'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland',
            'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
            'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
            'Uttarakhand', 'West Bengal'])
    
    
    
    

    indian_st = json.load(open('ind_st.json','r'))
    
    fig_1 = px.scatter_geo(state_plotf, lon=state_plotf['Longitude'],
                           lat=state_plotf['Latitude'],color=state_plotf['Transaction_amount'],
                           size=state_plotf['Transaction_count'], hover_name="state",
                           hover_data=['state','Transaction_amount','Transaction_count','Year','Quarter'],
                           title='state',size_max=22)
    fig_1.update_traces(marker={'color':"violet", 'line_width':1})

    fig_2 = px.scatter_geo(dist_plot, lon=dist_plot['Longitude'], lat = dist_plot['Latitude'],
                           color=dist_plot['Amount'],size=dist_plot['Count'],hover_name='District',
                           hover_data=['District','Amount','Count','Year','Quarter'], title='District', size_max=22)
                           

    fig = px.choropleth(
          state_plotf,
          #geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
          geojson=indian_st,
          featureidkey='properties.ST_NM',
          locations='state',
          scope='asia',
          color='Transaction_amount',
          color_continuous_scale='agsunset',
          hover_data=['Transaction_count', 'Transaction_amount'])



    fig.update_geos(fitbounds="locations", visible=False)
    fig.add_trace(fig_1.data[0])
    fig.add_trace(fig_2.data[0])
    fig.update_layout(height=1000,width=1000)
    st.plotly_chart(fig)

with user_analysis:
    st.subheader(':violet[User Analysis : Registered Users and App opens]')
    st.write('')
    Year=st.radio('Please select the year',('2022','2021','2020','2019','2018'), horizontal=True)

    st.write('')
    Quarter=st.radio('Please select the quarter',('4','3','2','1'),horizontal=True)
    

    st.write('')
    Year=int(Year)
    #Quarter=int(Quarter)
    userst_plot = userst_f[(userst_f['Year']==Year)]#&(userst_f['Quarter']==Quarter)]
    userdt_plot = user_dist[(user_dist['year']==Year)]#&(user_dist['Quarter']==Quarter)]

    userst_plotf = userst_plot.groupby(['state','Year','Brand','Latitude','Longitude']).sum()
    userst_plotf = userst_plotf.reset_index()


    userst_plotf['state'] = userst_plotf['state'].replace(['andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
            'uttarakhand', 'west-bengal'], ['Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
            'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
            'Dadra & Nagar Haveli & Daman & Diu', 'Delhi', 'Goa', 'Gujarat',
            'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir', 'Jharkhand',
            'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh',
            'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland',
            'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
            'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
            'Uttarakhand', 'West Bengal'])
    
    indian_st = json.load(open('G:/GUVI_ZEN_PY/pYTHON/PhonePe/ind_st.json','r'))

    fig_3 = px.scatter_geo(userst_plotf, lon=userst_plotf['Longitude'],
                           lat=userst_plotf['Latitude'],color=userst_plotf['Count'],
                           size=userst_plotf['Percentage'], hover_name="state",
                           hover_data=['state','Brand','Count','Percentage','Year'],
                           title='state',size_max=22)
    fig_3.update_traces(marker={'color':"violet", 'line_width':1})

    fig_4 = px.scatter_geo(userdt_plot, lon=userdt_plot['Longitude'], lat = userdt_plot['Latitude'],
                           color=userdt_plot['Registeredusers'],size=userdt_plot['Registeredusers'],hover_name='District',
                           hover_data=['District','Registeredusers','Appopens','year','Quarter'], title='District', size_max=22)
    

    fig_5 = px.choropleth(
          userst_plotf,
          #geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
          geojson=indian_st,
          featureidkey='properties.ST_NM',
          locations='state',
          scope='asia',
          color='Count',
          color_continuous_scale='viridis',
          hover_data=['Count', 'Percentage'])

    
    fig_5.update_geos(fitbounds="locations", visible=False)
    fig_5.add_trace(fig_3.data[0])
    fig_5.add_trace(fig_4.data[0])
    fig_5.update_layout(height=1000,width=1000)
    st.plotly_chart(fig_5)

with payment_types:
    st.subheader(':violet[Analysis according to transaction_types]')
    pay_query = 'select * from agg_trans'
    t_type = pd.read_sql(pay_query, con=conn)
    st.write('')
    pay_state = st.selectbox('Please select state',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
            'uttarakhand', 'west-bengal'))

    st.write('')
    pay_year = int(st.radio('Please select the year',('2022','2021','2020','2019','2018'), horizontal=True, key = 'pay_year'))
    pay_qtr = int(st.radio('Please select the quarter',('4','3','2','1'),horizontal=True, key = 'pay_qtr'))

    pay_values = st.selectbox('Select values for visualization',('Transaction_count','Transaction_amount'))

    pay_mode = t_type[(t_type['Year']==pay_year)&(t_type['Quarter']==pay_qtr)&(t_type['State']==pay_state)]

    pie_chart = px.pie(pay_mode,values=pay_values, names = 'Transaction_type',hover_data=['Year'])

    bar_chart = px.bar(pay_mode, x='Transaction_type', y=pay_values,color = 'Transaction_type')
    
    st.plotly_chart(pie_chart)
    st.plotly_chart(bar_chart)

with device_analysis:
    st.subheader(':violet[Users by device analysis]')
    user_query = 'select * from agg_user'
    user_dev = pd.read_sql(user_query, con=conn)
    
    st.write('')
    user_state = st.selectbox('Please select state',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
            'uttarakhand', 'west-bengal'), key='user_state')
    
    st.write('')
    user_yr = int(st.radio('Please select the year',('2022','2021','2020','2019','2018'), horizontal=True, key = 'user_yr'))
    user_qt = int(st.radio('Please select the quarter',('4','3','3','2','1'), horizontal=True, key = 'user_qt'))

    user_values = st.selectbox('Select values for visualization',('Count','Percentage'))

    u_device = user_dev[(user_dev['Year']==user_yr)&(user_dev['Quarter']==user_qt)&(user_dev['State']==user_state)]

    user_pie = px.pie(u_device,values=user_values, names = 'Brand',hover_data=['Year'])

    user_bar = px.bar(u_device, x='Brand', y=user_values,color = 'Brand')
    
    st.plotly_chart(user_pie)
    st.plotly_chart(user_bar)
    
                 
    
                           

    
    

    
    
    

                             

