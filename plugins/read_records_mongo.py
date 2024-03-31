#!/usr/bin/env python
# coding: utf-8

# In[4]:


from pymongo import MongoClient
import pandas as pd

def read_data_from_mongodb(uri, database_name, collection_name):
    client = MongoClient(uri)
    db = client[database_name]
    collection = db[collection_name]

    cursor = collection.find({})
    data = list(cursor)
    
    df = pd.DataFrame(data)

    return df

