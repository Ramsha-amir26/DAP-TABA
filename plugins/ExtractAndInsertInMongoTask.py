#!/usr/bin/env python
# coding: utf-8

# In[2]:


import luigi
import pandas as pd
from pymongo import MongoClient
import os

class ExtractAndInsertTask(luigi.Task):
    file_path = luigi.Parameter(default=None)
    data = luigi.Parameter(default=None)
    uri = luigi.Parameter()
    db_name = luigi.Parameter()
    collection_name = luigi.Parameter()
    batch_size = luigi.IntParameter(default=1000)  
    
    def run(self):
        # Read data based on file extension
        if self.file_path:
            _, file_extension = os.path.splitext(self.file_path)
            if file_extension.lower() == '.csv':
                data = pd.read_csv(self.file_path)
            elif file_extension.lower() == '.xlsx':
                data = pd.read_excel(self.file_path)
            elif file_extension.lower() == '.json':
                with open(self.file_path, 'r') as f:
                    data = pd.read_json(f)
        elif self.data:
            data = pd.DataFrame(self.data)
        else:
            raise ValueError("Either file_path or data parameter must be provided.")

        client = MongoClient(self.uri)
        db = client[self.db_name]
        collection = db[self.collection_name]

        # Insert data in batches
        records = data.to_dict(orient='records')
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i+self.batch_size]
            collection.insert_many(batch)

    def output(self):
        return luigi.LocalTarget("mongo_insertion_complete.txt")

