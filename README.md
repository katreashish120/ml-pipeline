## ml-pipeline

There are 2 pipelines:
	1. Rainfall prediction (data collected from https://data.gov.sg/dataset/rainfall-monthly-total)
	2. Temprature prediction (Data collected from http://www.weather.gov.sg/climate-historical-daily/)


### System setup:

The pipelines are written in pyspark and deployed in Azure Cloud.

Useful links for learning:

1. Databricks: https://docs.microsoft.com/en-us/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal

2. Azue Datalake Storage: https://docs.microsoft.com/en-us/learn/modules/introduction-to-azure-data-lake-storage/

3. Key-Vault: https://docs.microsoft.com/en-us/azure/key-vault/secrets/quick-create-portal

4. Azure Key-Vault and Databricks connection: https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#--create-an-azure-key-vault-backed-secret-scope

5. Azure Data Factory: https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal

6. mounting ADLS on Databricks: https://towardsdatascience.com/mounting-accessing-adls-gen2-in-azure-databricks-using-service-principal-and-secret-scopes-96e5c3d6008b


7. Databricks setup: Databricks Cluster can be created using /rainfall/databricks/compute.json


#### Azure Data Factory Setup:

Details of Azure Data Factory setup can be found at rainfall/adf/ and temperature/adf/

#### Execution files:

Both Rainfall prediction and Temperature prediction algorithms have 3 code files each.


###### train_rainfall_data / train_temperature_data: 
This file contains code for training the model and generate the model file

###### generate_rainfall_data / generate_temperature_data: 
This file contains code for generating dummy data for prediction (it can be scheduled to run at certain intervals using (generate_rainfall_data_job.json / generate_temperature_data_job.json)

###### predict_rainfall_data / predict_temperature_data:
This file contains code for predicting the data. This file uses spark streaming.
