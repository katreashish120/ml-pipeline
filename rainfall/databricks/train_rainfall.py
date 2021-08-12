#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Run a Linear Model on the data
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from pyspark.sql.functions import split
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn import linear_model

test = True

# 
models = ['random_forest', 'linear', 'svm']
model = models[0]


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
    input_path = '/mnt/<mount-name>/<path>/rainfall/data/*.csv'
  if input_path == 'Not found':
    model_path = '/dbfs/mnt/<mount-name>/<path>/rainfall/model/rainfall-model.pkl'


# In[ ]:


input_df = spark.read.option("inferSchema","true").option("header", "true").csv(input_path)

if test:
  display(input_df)


# In[ ]:


# Plit column using - to seporate year and month
split_col = split(input_df['month'], '-')
input_split_df = input_df.withColumn('year', split_col.getItem(0))
input_split_df = input_split_df.withColumn('month', split_col.getItem(1))

# restructure columns
input_split_df = input_split_df.select("year","month","total_rainfall")

# transpose the table
input_pivot_df = input_split_df.groupBy("year").pivot("month").sum("total_rainfall")

if test:
  display(input_pivot_df)


# In[ ]:


div_data = np.asarray(input_pivot_df.select([c for c in input_pivot_df.columns if c not in {'year'}]).collect())

X = None; y = None
for i in range(div_data.shape[1]-3):
  if None not in div_data[:, i:i+3] or None not in div_data[:, i+3]:
    if X is None:
        X = div_data[:, i:i+3]
        y = div_data[:, i+3]
    else:
          X = np.concatenate((X, div_data[:, i:i+3]), axis=0)
          y = np.concatenate((y, div_data[:, i+3]), axis=0)
        
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


# In[ ]:


if test:
  print(len(X_train))


# In[ ]:


if model == 'linear':
  
  if test: 
    print(model)
 
  model = linear_model.ElasticNet(alpha=0.5)
  model.fit(X_train, y_train)
  y_pred = model.predict(X_test)
  
  if test: 
    print('mean_absolute_error: ', mean_absolute_error(y_test, y_pred))
    
elif model == 'svm':
  
  if test: 
    print(model)
    
  model = SVR(gamma='auto', C=0.5, epsilon=0.2)
  model.fit(X_train, y_train) 
  y_pred = model.predict(X_test)
  
  if test: 
    print('mean_absolute_error: ', mean_absolute_error(y_test, y_pred))
  
else:
  
  if test: 
    print('random_forest')
    
  model = RandomForestRegressor(n_estimators = 100, max_depth=10, n_jobs=1, verbose=2)
  model.fit(X_train, y_train)
  y_pred = model.predict(X_test)
  
  if test: 
    print('mean_absolute_error: ', mean_absolute_error(y_test, y_pred))


# In[ ]:


if test:
  print(y_pred)


# In[ ]:


if test:
  print(model.predict([[34.3, 118.4, 41.6]]))


# In[ ]:


pickle.dump(model, open(model_path, 'wb'))

