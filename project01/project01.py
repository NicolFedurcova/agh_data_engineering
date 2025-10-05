
import pandas as pd
import numpy as np
import json

#TASK 1  
df = pd.read_csv("proj1_ex01.csv")


arr = []

for col in df.columns:
  temp_dict={}

  temp_dict["name"]=col
  if "int" in str(df[col].dtypes):
    temp_dict["type"]="int"
  elif "float" in str(df[col].dtypes):
    temp_dict["type"]="float"
  else:
    temp_dict["type"]="other"

  temp_dict["missing"]=int(df[col].isna().sum())/int(len(df[col]))

  arr.append(temp_dict)

new_df = pd.DataFrame(arr)
new_df.to_json("proj1_ex01_fields.json",indent=4, orient="records")

#TASK 2
info_df =  df.describe(include="all")
info_dict = {}

for info_col in info_df.columns:
  info_dict[info_col] = {}
  col_type = str(df[info_col].dtypes)
  if (("float" in col_type) or ("int" in col_type)):
    info_dict[info_col]["count"] = float(info_df[info_col]["count"])
    info_dict[info_col]["mean"] = float(info_df[info_col]["mean"])
    info_dict[info_col]["std"] = float(info_df[info_col]["std"])
    info_dict[info_col]["min"] = float(info_df[info_col]["min"])
    info_dict[info_col]["25%"] = float(info_df[info_col]["25%"])
    info_dict[info_col]["50%"] = float(info_df[info_col]["50%"])
    info_dict[info_col]["75%"] = float(info_df[info_col]["75%"])
    info_dict[info_col]["max"] = float(info_df[info_col]["max"])
  else:
    info_dict[info_col]["count"] = int(info_df[info_col]["count"])
    info_dict[info_col]["unique"] = int(info_df[info_col]["unique"])
    info_dict[info_col]["top"] = info_df[info_col]["top"]
    info_dict[info_col]["freq"] = float(info_df[info_col]["freq"])

info_df = pd.DataFrame(info_dict)
info_df.to_json("proj1_ex02_stats.json",indent=4, orient="columns")

#TASK 3
new_names = []
for col in df.columns:
  new_name=""
  for letter in col:
    ascii_code = ord(letter)
    if ascii_code in range(65, 91) or ascii_code in range(97, 123) or ascii_code in range(48, 58) or ascii_code in {95, 32}:
      new_name += "_" if ascii_code == 32 else letter.lower()
  new_names.append(new_name)

df.columns = new_names
df.to_csv("proj1_ex03_columns.csv", index=False)

#TASK4
df_list = list(df)
# df_only_headers = pd.DataFrame(columns=df_list)
# df_only_headers.to_excel("proj1_ex04_excel.xlsx",index=False)
df.to_excel("proj1_ex04_excel.xlsx",index=False)

arr_row_dicts = []
for row in df.iterrows():
  temp_dict = {}
  for col in df_list:
    temp_dict[col] = row[1][col]
  arr_row_dicts.append(temp_dict)

df_rows = pd.DataFrame(arr_row_dicts)
df_rows.to_json("proj1_ex04_json.json",indent=4, orient="records")

df.to_pickle("proj1_ex04_pickle.pkl")

#TASK 5
df_from_pkl = pd.read_pickle("proj1_ex05.pkl")


dict_df_from_pkl = {}
index_values = []

for j, col in enumerate(df_from_pkl.columns):
    if j in {1, 2}:  
        dict_df_from_pkl[col] = []

for i, row in df_from_pkl.iterrows():
    if i[0] == "v":  
        index_values.append(i) 
        for j, col in enumerate(df_from_pkl.columns):
            if j in {1, 2}:  
                dict_df_from_pkl[col].append(row[col])

df_new_from_pkl = pd.DataFrame(dict_df_from_pkl)
df_new_from_pkl.insert(0, "", index_values)

md_table = df_new_from_pkl.fillna("").to_markdown(index=False)

with open("proj1_ex05_table.md", "w") as file:
    file.write(md_table)

#TASK 6

with open("proj1_ex06.json", "r") as file:
    data = json.load(file)

df_from_jsn = pd.json_normalize(data, sep='.', record_path=None, meta=None)
df_from_jsn.to_pickle("proj1_ex06_pickle.pkl")