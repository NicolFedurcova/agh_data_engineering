#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import json


# In[2]:


with open('proj5_params.json', encoding="utf-8") as json_file:
    params = json.load(json_file)


# In[3]:


#TASK 1
df = pd.read_csv('proj5_timeseries.csv')
print(df.shape)
print(df.columns)
print(params["original_frequency"])
#df.iloc[[2051, 2052, 2999]]


# In[4]:


new_names = []
for col in df.columns:
    new_name=""
    for letter in col:
        ascii_code = ord(letter)
        if ascii_code in range(65, 91) or ascii_code in range(97, 123) or ascii_code in range(48, 58):
            new_name += letter.lower()
        else:
            new_name+="_"
    new_names.append(new_name)

df.columns = new_names

df[df.columns[0]] = pd.to_datetime(df[df.columns[0]], format='mixed')
df.set_index(df.columns[0], inplace=True)
dfn = df.asfreq(params["original_frequency"])
dfn.to_pickle("proj5_ex01.pkl")
dfn.iloc[[2051, 2052, 2999]]


# In[5]:


dfn.head()


# In[6]:


#TASK 2
df_target_freq = dfn.asfreq(params["target_frequency"])
df_target_freq.to_pickle("proj5_ex02.pkl")
df_target_freq.head()


# In[7]:


#TASK 3
how = f"{params['downsample_periods']}{params['downsample_units']}"
print(how)
df_downsampled = dfn.resample(how).sum(min_count=int(params["downsample_periods"]))
df_downsampled.to_pickle("proj5_ex03.pkl")
df_downsampled


# In[8]:


#TASK 4

how = f"{params['upsample_periods']}{params['upsample_units']}"
print(how)
df_upsampled = dfn.resample(how)

df_upsampled = df_upsampled.interpolate(
        method=params['interpolation'],
        order=params['interpolation_order']
    )

original_timedelta = pd.Timedelta(f'1{params["original_frequency"]}')
print(original_timedelta)
new_timedelta = pd.Timedelta(how)
print(new_timedelta)
scaling_factor =  new_timedelta/original_timedelta 
print(scaling_factor)

df_upsampled = df_upsampled * scaling_factor
df_upsampled.to_pickle('proj5_ex04.pkl')
df_upsampled


# In[9]:


print(df_upsampled.iloc[[0,1,2,3,4,5,6,7,8,9,10,11],0].sum())
print(df_upsampled.iloc[[12,13,14,15,16,17,18,19,20,21,22,23],0].sum())


# In[16]:


#TASK 5
df_sensor = pd.read_pickle('proj5_sensors.pkl')
df_sensor


# In[11]:


# df_sensor = df_sensor.reset_index()
# df_sensor


# In[23]:


#dfp_sensor = df_sensor.pivot(index='timestamp', columns='device_id', values='value')
dfp_sensor = df_sensor.pivot(columns='device_id', values='value')
dfp_sensor


# In[24]:


freq = f"{params['sensors_periods']}{params['sensors_units']}"
print(freq)
new_index = pd.date_range(dfp_sensor.index.round(freq).min(), dfp_sensor.index.round(freq).max(), freq=freq)
#dfr_sensor = dfp_sensor.resample(freq).mean().interpolate(method='linear')
dfp_sensor.reindex(new_index)


# In[25]:


dfp2 = dfp_sensor.reindex(new_index.union(dfp_sensor.index)).interpolate(method="linear")
dfp2


# In[26]:


dfp3 = dfp2.reindex(new_index)
dfp3.head()


# In[27]:


complete_cases = dfp3.dropna()
complete_cases.to_pickle('proj5_ex05.pkl')
complete_cases.head()

