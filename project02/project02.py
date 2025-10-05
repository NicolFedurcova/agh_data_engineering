
import pandas as pd
import numpy as np
import re

#ADDED COMMENT JUST TO TRY OUT THE NOTEBOOK

#prefix = "/content/drive/MyDrive/Colab Notebooks/data_engineering/lab_02/"
#prefix="project02/"
prefix=""

#TASK 1
#input_path = f"{prefix}proj2_data – comma float.csv"
input_path = f"{prefix}proj2_data.csv"

def is_at_least_one_float_col(df):
  for col_type in df.dtypes:
    if ("float" in str(col_type)):
      return True
  return False

#df read csv: decimal= str (length 1), default ‘.’

  #delim | and float delim "."
delim = "|"
df = pd.read_csv(input_path, delimiter=delim)
#print("|", len(df.columns))
has_float = is_at_least_one_float_col(df)
  #delim | and float delim ","
if(not has_float): #no float col was found - delim is not ok
  df = pd.read_csv(input_path, delimiter=delim, decimal=",")

  #delim ; and float delim "."
if(len(df.columns)==1):
  delim = ";"
  df = pd.read_csv(input_path, delimiter=delim)
  #print(";",len(df.columns))
  has_float = is_at_least_one_float_col(df)
    #delim ; and float delim ","
  if(not has_float): #no float col was found - delim is not ok
    df = pd.read_csv(input_path, delimiter=delim, decimal=",")

  if(len(df.columns)==1): #if we get to third case, it means no number will be separated by , as it is col separator
    delim = ","
    df = pd.read_csv(input_path, delimiter=delim)
    #print(",",len(df.columns))

df.to_pickle(f"{prefix}proj2_ex01.pkl")
init_df = df.copy()

#init_df

#TASK 2
scale = []
scale_dict = {}

# counter = 1
# with open(f"{prefix}proj2_scale.txt") as f:
#     line = f.readline().strip()
#     while line:
#         scale.append(line)
#         scale_dict[line] = counter
#         counter +=1
#         line = f.readline().strip()

with open(f"{prefix}proj2_scale.txt") as f:
    content = f.read()

scale = content.split('\n')  
#print(scale)
for i,val in enumerate(scale):
  scale_dict[val] = i+1
# print(scale)



scale_df = init_df.copy()
categorical_columns = set()

def replace_if_scale(row):
    for col in row.index:
        for key, val in scale_dict.items():
            if key == str(row[col]):
                row[col] = val
                categorical_columns.add(col)
                
    return row

scale_df = scale_df.apply(replace_if_scale, axis=1) #apply goes through rows 0/columns 1
#print(categorical_columns)

scale_df.to_pickle(f"{prefix}proj2_ex02.pkl")
#scale_df


#TASK 3
cat_df = init_df.copy()

for col_name in categorical_columns:
  cat_df[col_name] = cat_df[col_name].astype("category")
  cat_df[col_name] = cat_df[col_name].cat.set_categories(scale)

cat_df.to_pickle(f"{prefix}proj2_ex03.pkl")

#print(cat_df['language'].cat.categories)
#cat_df.dtypes

#TASK 4

extracted_num_df = init_df.copy()



new_df = pd.DataFrame()

# #-? optional -  \d+ one or more digits  (?:[.,]\d+)?  optional dot or comma + digits
# def extract_number (input):
#   pattern = r'-?\d+(?:[.,]\d+)?'
#   match = re.search(pattern, str(input))
#   if match:
#       #print(type(match.group()))
#       return match.group()
#   return None

def extract_number(input):
    res = ''
    caught_some_number = False

    for i,char in enumerate(input):
        if char.isdigit() or char in ['.', ',', '-']:
            if char=="-":
                if i!=(len(input)-1) and input[i+1].isdigit():
                    res+="-"
            elif char=="." or char==",":
                if i!=(0) and i!=(len(input)-1) and input[i-1].isdigit() and input[i+1].isdigit():
                    res+="."
            else:
                res += char
                caught_some_number = True
        elif not char.isdigit() and char not in ['.', ',', '-'] and caught_some_number:
            break
            
    if any(char_check.isdigit() for char_check in res):
        try:
            return float(res)
        except ValueError:
            return None
    return None

for i,col in extracted_num_df.items(): #we go column by column
  if col.dtype=="object":
    was_extracted=False
    new_col = []
    for val in col:
      extracetd = extract_number(val)
      if extracetd is not None:
        was_extracted=True
      new_col.append(extracetd)
    if was_extracted:
      new_df[col.name] = new_col


new_df.to_pickle(f"{prefix}proj2_ex04.pkl")
#new_df



#TASK 5

one_hot_df = init_df.copy()

to_be_encoded = []

for i,col in one_hot_df.items(): #we go column by column
  if col.dtype=="object":
    vals = set()
    good_vals = True
    for val in col:
      if val is None or not val.isalpha() or not val.islower() or val in scale:
          good_vals=False
      else:
        vals.add(val)
    #print(col.name, ":", vals, "is good:", good_vals)
    if len(vals)<=10 and good_vals:
      to_be_encoded.append(col.name)

#print(to_be_encoded)
ord=0
for col_name in to_be_encoded:
  ord+=1
  one_hot_df_col = pd.get_dummies(one_hot_df[col_name])
  one_hot_df_col.to_pickle(f"{prefix}proj2_ex05_{ord}.pkl")
  #print(one_hot_df_col)