#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from pyspark.sql.functions import split, concat,col
from sklearn.svm import SVR

test = True


# In[ ]:


dbutils.widgets.removeAll()

dbutils.widgets.text("input_path", "Not found", "input_path")
input_path = dbutils.widgets.get("input_path")

dbutils.widgets.text("model_path", "Not found", "model_path")
model_path = dbutils.widgets.get("model_path")

if test:
  print(dbutils.widgets.get("input_path"))
  print(dbutils.widgets.get("model_path"))
  
  if input_path == 'Not found':
    input_path = '/mnt/<mount-name>/<path>/temperature/data/*.csv'
  if model_path == 'Not found':
    model_path = '/dbfs/mnt/<mount-name>/<path>/temperature/model/temperature-model.pkl'


# In[ ]:


input_df = spark.read.option("inferSchema","true").option("header", "true").csv(input_path)

if test:
  display(input_df)


# In[ ]:


input_df = input_df.withColumn('Year_Month', concat(col('Year'), col('Month')))
cols = ['Year_Month','Day','Mean_Temperature']
input_df = input_df[cols]

if test:
  display(input_df)


# In[ ]:


input_pivot_df = input_df.groupBy("Year_Month").pivot("Day").sum("Mean_Temperature")


# In[ ]:


div_data = np.asarray(input_pivot_df.select([c for c in input_pivot_df.columns if c not in {'Year_Month'}]).collect())

X = None; y = None
for i in range(div_data.shape[1]-6):
    if X is None:
        X = div_data[:, i:i+3]
        y = div_data[:, i+3]
    else:
       if None not in div_data[:, i:i+3] or None not in div_data[:, i+3]:
          X = np.concatenate((X, div_data[:, i:i+3]), axis=0)
          y = np.concatenate((y, div_data[:, i+3]), axis=0)
        
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


# In[ ]:


if test:
  print(X_train)


# In[ ]:


clf = SVR(gamma='auto', C=0.1, epsilon=0.2)
clf.fit(X_train, y_train) 
y_pred = clf.predict(X_test)
mean_absolute_error(y_test, y_pred)


# In[ ]:


pickle.dump(clf, open(model_path, 'wb'))

