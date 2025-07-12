import os
import time

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, HiveContext
from src.config import database_name, table_name, temp_path, event_date, sparkConf
from src.logging import setupLogging

logger = setupLogging(__name__)
start_time = time.perf_counter()

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

_make_parent_dirs_and_return_path(temp_path)

# Define Spark Configuration
logger.info("GENERATE SPARK SESSION ============")
appName = "mlcore-read_from_hive"
sc = SparkContext(conf=sparkConf.setAppName(appName)).getOrCreate()
hc = HiveContext(sc)
sq = SQLContext(sc)
spark = SparkSession(sc)

if __name__=="__main__":
    # Log start time
    start_time = time.time() 

    # Define Query Command
    if 'socmed' in table_name:
    	query = f"SELECT * FROM {database_name}.{table_name} WHERE EVENT_DATE = '{event_date}' AND  ENDPOINT = 'data' AND campaign_id_ls != '[16442]'"
    else:
        query = f"SELECT * FROM {database_name}.{table_name} WHERE EVENT_DATE = '{event_date}'" 
    logger.info(query) 

    # Display Query Ouput
    logger.info("RESULT ============")
    df = spark.sql(query)
    logger.info(df.show())
    logger.info(type(df))

    pandasdf = df.toPandas()

    if not pandasdf.shape[0]:
        raise ValueError("No data available!")
    else:
        pandasdf.to_parquet(temp_path, index=False)
        logger.info(f"Save parquet to: {temp_path}")

        logger.info("FINISHED Succesfully ===========")

    end_time = time.perf_counter()
    logger.info(f"Run time: {round((end_time - start_time) / 60, 2)} minute(s)")
