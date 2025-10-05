#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sqlite3


# In[2]:


# db_file = "proj6_readings.sqlite"
# csv_file = "detectors_names_traffic_s_small.csv"

# # Create SQLite connection
# conn = sqlite3.connect(db_file)
# cursor = conn.cursor()

# # Create table
# cursor.execute("""
# CREATE TABLE IF NOT EXISTS readings (
#     detector_id INTEGER,
#     shortname TEXT,
#     name TEXT,
#     starttime TEXT,  
#     endtime TEXT,
#     count INTEGER
# )
# """)

# # Batch insert (adjust batch_size as needed)
# batch_size = 10_000
# with open(csv_file, 'r') as f:
#     reader = csv.reader(f)
#     next(reader)  # Skip header
#     batch = []
#     for row in reader:
#         batch.append(row)
#         if len(batch) >= batch_size:
#             cursor.executemany("""
#                 INSERT INTO readings 
#                 VALUES (?, ?, ?, ?, ?, ?)
#             """, batch)
#             conn.commit()
#             batch = []
#     # Insert remaining rows
#     if batch:
#         cursor.executemany("INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?)", batch)
#         conn.commit()

# conn.close()


# In[3]:


# db_file = "readings.sqlite"
# df = pd.read_csv("detectors_names_traffic_s_small.csv")
# conn = sqlite3.connect(db_file)
# df.to_sql("readings", conn, if_exists="replace", index=False)
# conn.close()


# In[4]:


# con = sqlite3.connect("proj6_readings.sqlite")
# cur = con.cursor()
# result = cur.execute("SELECT count(*) from readings;").fetchall()
# df = pd.DataFrame(result)

# con.close()
# df


# In[5]:


# con = sqlite3.connect("proj6_readings.sqlite")
# cur = con.cursor()
# cur.execute("""
# CREATE INDEX detector_id ON readings (detector_id);
# """).fetchall()
# cur.execute("""
# CREATE INDEX starttime ON readings (starttime);
# """).fetchall()


# In[6]:


#TASK 1
con = sqlite3.connect("proj6_readings.sqlite")
cur = con.cursor()
result = cur.execute("SELECT COUNT(DISTINCT detector_id) from readings;").fetchall()
df = pd.DataFrame(result)
df.to_pickle("proj6_ex01_detector_no.pkl")
con.close()
df


# In[7]:


#TASK 2
con = sqlite3.connect("proj6_readings.sqlite")
cur = con.cursor()

query = """
SELECT 
    detector_id,
    COUNT(count) AS measurement_count,
    MIN(starttime) AS min_starttime,
    MAX(starttime) AS max_starttime
FROM 
    readings
WHERE 
    count IS NOT NULL
GROUP BY 
    detector_id
ORDER BY 
    detector_id;
"""
result = cur.execute(query).fetchall()
df = pd.DataFrame(result)
df.to_pickle("proj6_ex02_detector_stat.pkl")
con.close()
df


# In[8]:


#TASK 3
con = sqlite3.connect("proj6_readings.sqlite")
cur = con.cursor()
query = """
WITH detector_146 AS (
    SELECT 
        detector_id,
        count,
        starttime,
        LAG(count, 1) OVER (PARTITION BY detector_id ORDER BY starttime) AS prev_count
    FROM 
        readings
    WHERE 
        detector_id = 146
    ORDER BY 
        starttime
    LIMIT 500
)
SELECT 
    detector_id,
    count,
    prev_count
FROM 
    detector_146;
"""

result = cur.execute(query).fetchall()
df = pd.DataFrame(result)

df.to_pickle("proj6_ex03_detector_146_lag.pkl")

con.close()

df


# In[19]:


#TASK 4

con = sqlite3.connect("proj6_readings.sqlite")
cur = con.cursor()
query = """
WITH ordered AS (
    SELECT 
        detector_id,
        count,
        ROW_NUMBER() OVER (ORDER BY starttime) as row_num
    FROM readings
    WHERE detector_id = 146
    ORDER BY starttime
)
SELECT 
    a.detector_id,
    a.count,
    (SELECT SUM(b.count) 
     FROM ordered b
     WHERE b.row_num BETWEEN a.row_num AND a.row_num + 10
    ) as window_sum
FROM ordered a
LIMIT 500;
"""

result = cur.execute(query).fetchall()
df = pd.DataFrame(result)

df.to_pickle("proj6_ex04_detector_146_sum.pkl")

con.close()

df

