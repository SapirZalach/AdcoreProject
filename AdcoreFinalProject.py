#!/usr/bin/env python
# coding: utf-8

# In[3]:


import csv
import pandas as pd
import schedule
import time
import datetime
import os
import re


# In[4]:


def add_date_and_time(file_path):
    file=pd.read_csv(file_path) 
    header_string = re.sub(r'[0-9]', '', file.keys()[0]).replace('-','').replace(' ','').replace(':','').replace('.','')
    headers = header_string.split()
    current_time = datetime.datetime.now()
    new_header = ''
    for header in headers:
        new_header = new_header + header +' '+ str(current_time)+'\t'

    file.rename(columns={file.keys()[0]: new_header}, inplace=True)
    return file


# In[5]:


def write_to_database(current_status):
    if database_file=='':
        database_file = "C:/Users/Sapir/Desktop/project_database.csv"
        database = pd.DataFrame(columns=['time','number_of_rows'])
    else:
        database = pd.read_csv(database_file)
        
    database.loc[len(database_file)] = datetime.datetime.now(),len(current_status)
    database.to_csv(database_file,index=False)
    return database_file


# In[ ]:


rows_threshold = 10000000
time_schedule = 60*15
double_rows = []
database_file = ''
while len(double_rows)<rows_threshold:
    time.sleep(time_schedule)
    file_path = "C:/Users/Sapir/Desktop/tree_data.csv"
    file_with_date = add_date_and_time(file_path)
    double_rows = pd.concat([file_with_date,file_with_date])
    if len(double_rows)>rows_threshold:
        write_to_database(double_rows.head(rows_threshold),database_file)
        database_file = double_rows.head(rows_threshold).to_csv(file_path,index=False,sep='\t')
    else:
        database_file = write_to_database(double_rows,database_file)
        double_rows.to_csv(file_path,index=False,sep='\t')


# In[ ]:




