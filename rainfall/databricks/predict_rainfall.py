#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pickle
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, from_json

test = True
kafka = False


# In[ ]:


dbutils.widgets.removeAll()

dbutils.widgets.text("model_path", "Not found", "model_path")
model_file = dbutils.widgets.get("model_path")

if test:
  print('model path: ',model_file)
  
if model_file == 'Not found':
  # temp
  model_file = '/dbfs/mnt/<mount-name>/<path>/rainfall/model/rainfall-model.pkl'
  
if test:
  print('model path: ',model_file)


# In[ ]:


loaded_model = pickle.load(open(model_file, 'rb'))

kafka_url = "host:port"
kafka_input_topic = "rainfall_predict"
kafka_output_topic = "rainfall_results"

data_schema = StructType().add("input1", "float").add("input2", "float").add("input3", "float")

result_schema = StructType().add("key","string").add("value","float")


# In[ ]:


if not kafka:
  input_schema = StructType().add("value", "binary")
  read_df = spark.readStream.format('parquet').schema(input_schema).option("path", "/mnt/<mount-name>/<path>/rainfall/input/request.parquet").load()
else:
  read_df = spark     .readStream     .format("kafka")     .option("kafka.bootstrap.servers", kafka_url)     .option("subscribe", kafka_input_topic)     .option("startingOffsets", "earliest")     .load()


# In[ ]:


input_data_df = read_df.select(from_json(col("value").cast("string"), data_schema).alias("input"))


# In[ ]:


from pyspark.sql.functions import udf
@udf("float")
def predict_udf(input1,input2, input3):
  output = loaded_model.predict([[input1, input2, input3]])[0]
  return float(output)


# In[ ]:


# x = input_data_df.select(predict_udf(col('input.input1'), col('input.input2'), col('input.input3')))
# x = input_data_df.select(predict_udf(col('input.input1', 'input.input2')))

result_df = input_data_df.withColumn('rainfall', predict_udf('input.input1','input.input2','input.input3'))
if test:
  display(result_df)


# In[ ]:


if not kafka:
  result_df.select(col('rainfall')).writeStream.format("parquet").option("checkpointLocation",'/mnt/<mount-name>/<path>/rainfall/output/checkpoint').option("path", "/mnt/<mount-name>/<path>/rainfall/output/reponse.parquet").start()
    
else:
  ds = result_df     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")     .write     .format("kafka")     .option("kafka.bootstrap.servers", kafka_url)     .option("topic", kafka_output_topic)     .save()

