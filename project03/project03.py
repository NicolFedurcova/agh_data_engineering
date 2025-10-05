#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import json


# In[2]:


#TASK 1
file1 = pd.read_json("proj3_data1.json")
file2 = pd.read_json("proj3_data2.json")
file3 = pd.read_json("proj3_data3.json")

length = len(file1)+len(file2)+len(file3)
index_to_be = [i for i in range(length)]
frames = [file1,file2,file3]
df_all = pd.concat(frames)
df_all = df_all.set_index(pd.Series(index_to_be))
df_all.to_json("proj3_ex01_all_data.json", orient="columns")
df_all


# In[3]:


#TASK 2
counts = df_all.isna().sum()[df_all.isna().sum() > 0] # condition sum>0
counts.to_csv("proj3_ex02_no_nulls.csv", header=False)


# In[4]:


# TASK 3
df_desc = df_all.copy()
with open("proj3_params.json") as params:
    params_dict = json.load(params)

cols_to_concat = params_dict["concat_columns"]
df_desc["description"] = df_desc[cols_to_concat].astype(str).agg(" ".join, axis=1) #joins each row value wth " "
df_desc.to_json("proj3_ex03_descriptions.json", orient="columns")
df_desc


# In[5]:


params_dict


# In[6]:


# TASK 4
df_add = pd.read_json("proj3_more_data.json")
join_col = params_dict["join_column"]
df_merged = df_desc.merge(df_add, on=join_col,how="left")
df_merged.to_json("proj3_ex04_joined.json", orient="columns")
df_merged


# In[7]:


# TASK 5
for row in df_merged.iterrows():
    #row = tuple, row[1] = series
    desc = row[1]["description"].lower().replace(" ","_")
    row_for_json = row[1].drop("description") 
    row_for_json.to_json(f"proj3_ex05_{desc}.json", orient="columns")


for row in df_merged.iterrows():
    #row = tuple, row[1] = series
    desc = row[1]["description"].lower().replace(" ","_")
    row_for_json = row[1].drop("description") 
    for int_col in params_dict["int_columns"]:
        val = row_for_json[int_col]
        row_for_json[int_col] = "null" if pd.isna(val) else int(val)
    row_for_json.fillna("null")
    row_for_json.to_json(f"proj3_ex05_int_{desc}.json", orient="columns")


# In[8]:


# TASK 6
df_agg = df_merged.copy()
agg = params_dict["aggregations"]
json_agg_data = {}
for elem in agg:
    json_agg_data[f"{elem[1]}_{elem[0]}"]=float(df_agg[elem[0]].aggregate(elem[1]))
with open("proj3_ex06_aggregations.json", "w") as f:
    json.dump(json_agg_data, f, indent=4)
json_agg_data


# In[9]:


#TASK 7
gr_col = params_dict["grouping_column"]

df_numeric = df_merged.select_dtypes(include=['int64', 'float64']).copy()
df_numeric[gr_col] = df_merged[gr_col]


grouped = df_numeric.groupby(gr_col)
counts = grouped.size()
valid_groups = counts[counts > 1].index
mean_of_groups = grouped.mean().loc[valid_groups]
mean_of_groups.to_csv("proj3_ex07_groups.csv", header=True, index=True)
mean_of_groups


# In[10]:


# TASK 8
pivot_ind = params_dict["pivot_index"]
pivot_cols = params_dict["pivot_columns"]
pivot_vals = params_dict["pivot_values"]

df_for_pivot = df_merged.copy()

df_pivoted = df_for_pivot.pivot_table(index=pivot_ind, columns=pivot_cols, values=pivot_vals,aggfunc="max")
df_pivoted.to_pickle("proj3_ex08_pivot.pkl")
df_pivoted



# In[11]:


df_for_long = df_merged.copy()
rec_identif = params_dict["id_vars"]

df_long = pd.melt(df_merged, 
                  id_vars=rec_identif,    
                  var_name="variable",    
                  value_name="value")     

df_long.to_csv("proj3_ex08_melt.csv", header=True, index=False)
df_long


# In[22]:


stat_df = pd.read_csv("proj3_statistics.csv")
grouping_var = stat_df.columns[0]
pivot_index = params_dict["pivot_index"]

possible_prefixes = set(df_merged[pivot_index])

stat_df_melted = pd.melt(stat_df, id_vars=[grouping_var], var_name='prefsuf', value_name='value')

def split_prefsuf(row, prefixes):
    for prefix in prefixes:
        if row['prefsuf'].startswith(prefix):
            return pd.Series({
                'prefix': prefix,
                'suffix': row['prefsuf'][len(prefix):].strip("_")
            })
    return pd.Series({'prefix': None, 'suffix': None})

stat_df_melted[['prefix', 'suffix']] = stat_df_melted.apply(split_prefsuf, args=(possible_prefixes,), axis=1)

stat_df_melted['suffix'] = stat_df_melted['suffix'].astype(int)

stat_df_melted['multi_index'] = list(zip(stat_df_melted[grouping_var], stat_df_melted['suffix']))

result = stat_df_melted.pivot_table(
    index='multi_index',
    columns='prefix',
    values='value'
)

result.index = pd.MultiIndex.from_tuples(result.index)
result.columns.name = None

result.to_pickle("proj3_ex08_stats.pkl")


