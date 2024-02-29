# -*- coding: utf-8 -*-
from query_procesing import QueryProcessor
import pandas as pd
df = pd.read_csv("./dataset/STL_QUERY/sample.csv")

cols = [f'column{i}'  for i in range(len(df.columns))]
df.columns = cols

time_column_name = df.columns[7]

# column for start time=5 on aws website, but in head its column 7

df[time_column_name] = pd.to_datetime(df[time_column_name])

df.set_index(time_column_name, inplace=True)

#  Group by 1-hour intervals
groups = df.resample('H')

first_batch_printed = False

# The for loop below is producer part
for name, group in groups:
# Reciver (Consumer)
    if len(group) > 0:
        print(group)
        qp = QueryProcessor(csv_file_path='./dataset/STL_QUERY/sample.csv', df=group)
        num_unique_queries = qp.get_unique_query_counts()

