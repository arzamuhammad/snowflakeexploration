import os
import time
import pandas as pd
import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType, TimestampType
from src.config import *
from src.ext_to_hive import include_column_to_store, store_to_hive
from src.logging import setupLogging
from datetime import datetime, date
import pytz

os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/Anaconda3-5.2.0/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/cloudera/parcels/Anaconda3-5.2.0/bin/python'

# Define logging setup
log = setupLogging(__name__)

def get_num_partitions(spark_df):
    target_file_size = 128  # in MB
    total_data_size = spark_df.rdd.map(lambda x: 1).sum()  # Calculate the total size of the data
    num_partitions = int(total_data_size / (target_file_size * 1024 * 1024)) + 1
    return num_partitions

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Make directory
_make_parent_dirs_and_return_path(filePath)

if __name__=="__main__":
    # Log start time
    start_time = time.time() 

    # Define Spark Configuration
    log.info("----------------------------- START SPARK SESSION")
    appName= "writeToHive"
    spark = SparkSession.builder\
                        .appName(appName)\
                        .enableHiveSupport()\
                        .config(conf=sparkConf)\
                        .getOrCreate()

    # Load data 
    log.info("----------------------------- LOAD DATA")
    df = pd.read_parquet(filePath)

    # Audit necessary
    df['load_user'] = 'analytic_ops'
    df['load_ts'] = datetime.now(pytz.timezone('Asia/Jakarta'))

    if dataSource == 'TNPS':
        # TNPS
        # Ensure the data type is same with table
        schema = {  
            'msisdn': str,
            'id_trx': str,
            'channel': str,
            'lac': str,
            'ci': str,
            'area': str,
            'region': str,
            'city_kabupaten': str,
            'tnps_score': float,
            'comment': str,
            'tnps_source': str,
            'tnps_category': str,
            'journey': str,
            'journey_category': str,
            'topic': float,
            'topic_probs': str,
            'sentiment': float,
            'sentiment_probs': str,
            'source': str,
            'event_id': str,
            'load_ts': 'datetime64[ns]',
            'load_user': str,
            'event_date': str
        }
        df = df.astype(schema)
        formatTable = 'hive'
        print(f'Data types after change dtype: {df.dtypes}')

    elif dataSource == 'KIP':
        # KIP 
        # Ensure the data type is same with table
        schema = {
            'kip_id': str,
            'msisdn': str,
            'journey': str,
            'notes': str,
            'brand': str,
            'kip_cust_type': str,
            'kip_cust_sub_type': str,
            'site_id': str,
            'unit_type': str,
            'area': str,
            'region': str,
            'kip_category': str,
            'channel': str,
            'brand_type': str,
            'topic': float,
            'topic_probs': str,
            'sentiment': float,
            'sentiment_probs': str,
            'source': str,
            'event_id': str,
            'load_ts': 'datetime64[ns]',
            'load_user': str,
            'event_date': str
        }
        df = df.astype(schema)
        formatTable = 'hive'
        print(f'Data types after change dtype: {df.dtypes}')

    elif dataSource == 'App-Review':
        # Appbot
        # Make-sure all data type is string
        df = df.astype(str)
        formatTable = 'hive'
        print(f'Data types after change dtype: {df.dtypes}')

    elif dataSource == 'Social-Media':
        # Sonar
        # Make-sure all data type is string
        df = df.astype(str)
        formatTable = 'hive'
        print(f'Data types after change dtype: {df.dtypes}')


    else:
        print('Project is not defined!')

    # Write data to Hive
    log.info("----------------------------- WRITE DATA TO HIVE")

    try:
        spark_df = spark.createDataFrame(df)
        spark_df.show()
        spark_df.select('event_date').distinct().show()
        
        ev_dt = spark_df.select('event_date').drop_duplicates().rdd.map(lambda x: x[0]).collect()
        ev_dt = ', '.join(ev_dt)
        print("event_date : ", ev_dt)
        
        spark.sql(''' ALTER TABLE {}.{} DROP IF EXISTS PARTITION (event_date='{}') '''.format(databaseName,tableName,ev_dt))
        
        spark_df.repartition(5).write.format(formatTable).partitionBy('event_date').mode(writeMode).saveAsTable(databaseName+"."+tableName)
    except Exception as e:
        raise e

    # Log finish execute
    log.info("COMPLETED - Executing  Storing to Hive")
    end_time = time.time()
    processing_time =  end_time - start_time

    log.info("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    log.info("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    log.info("Processing time : {} seconds".format(processing_time))
