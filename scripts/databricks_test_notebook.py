# Databricks notebook source
"""
This is a test script for verifying the Azure CI-CD pipeline is working correctly.
Process: Ingest some data from a source, do some transformation and then push to a azure gen2 blob storage
"""

# COMMAND ----------

import requests
import pandas as pd
import time
API_URI = "https://api.coindesk.com/v1/bpi/currentprice.json"
storage_account_name = "dataprocessings"
blob_container = "databricks-container"
storage_account_access_key = dbutils.secrets.get(scope="dataprocessings-access", key ="dataprocessing-access")


# COMMAND ----------

def check_availability(APU_URI: str) -> bool:
    response = requests.get(API_URI)
    if response.status_code == 200:
        return response
    else:
        return False


def get_data(API_URI: str) -> pd.DataFrame:
    
    data = requests.get(API_URI).json()
    dataset = pd.DataFrame(data=data['bpi']).transpose()
    return dataset


def convert_to_spark(dataset: pd.DataFrame):
    sparkDF = spark.createDataFrame(dataset)
    return sparkDF

def store_data_to_gen2(storage_account_name: str, storage_account_access_key: str, blob_container: str, dataset) -> bool:
    spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
    filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/"+ f"{time.strftime('%Y%m%d_%H%M%S')}.parquet"
    dataset.write.mode("append").parquet(filePath)
    return True






        



# COMMAND ----------

def main():
    if check_availability(API_URI):
        sparkDF = convert_to_spark(get_data(API_URI=API_URI))
        sparkDF.printSchema()
        sparkDF.show()
        store_data_to_gen2(storage_account_name, storage_account_access_key, blob_container, sparkDF)
if __name__ == "__main__":
    main()

# COMMAND ----------




# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


