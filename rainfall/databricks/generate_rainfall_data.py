#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql.types import StructType
from pyspark.sql.functions import col, from_json
import json
import pandas as pd
import random

test = True
output_path = '/mnt/<mount-name>/<path>/rainfall/input/request.parquet'


# In[ ]:


input_data_dict = {'input1': round((random.randrange(100, 300) + random.random()), 1), 'input2': round((random.randrange(100, 300) + random.random()), 1), 'input3': round((random.randrange(100, 300) + random.random()), 1)}

data_schema = StructType().add("value", "binary")
data = json.dumps(input_data_dict).encode('utf-8')

data_df = pd.DataFrame(list([data,]))

result_df = spark.createDataFrame(data_df,schema=data_schema)


# In[ ]:


result_df.write.mode('overwrite').format("parquet").save(output_path)

