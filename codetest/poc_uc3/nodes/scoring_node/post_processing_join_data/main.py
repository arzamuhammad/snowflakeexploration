import os
import time
import pandas as pd
from src.config import *
from src.logging import setupLogging

# Define logging setup
log = setupLogging(__name__)

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Make directory
_make_parent_dirs_and_return_path(tempPath)

if __name__=="__main__":
    # Log start time
    start_time = time.time() 

    # Load data
    log.info("----------------------------- LOAD DATA")
    df = pd.read_parquet(inputPath)
    df2 = pd.read_parquet(inputTopicPath)
    df3 = pd.read_parquet(inputSentimentPath)
    print("Topic",df2.head())
    print("Sentiment",df3.head())

    # Rename column
    try:
        df2.rename(columns={'y_pred':'y_pred_topic'}, inplace=True)
        df2.rename(columns={'y_pred_proba':'y_pred_proba_topic'}, inplace=True)

        df2.rename(columns={'y_pred_sub_class':'y_pred_sub_topic'}, inplace=True)
        df2.rename(columns={'y_pred_proba_sub_class':'y_pred_proba_sub_topic'}, inplace=True)
    except:
        df2.rename(columns={'y_pred':'y_pred_topic'}, inplace=True)
        df2.rename(columns={'y_pred_proba':'y_pred_proba_topic'}, inplace=True)

    df3.rename(columns={'y_pred':'y_pred_sentiment'}, inplace=True)
    df3.rename(columns={'y_pred_proba':'y_pred_proba_sentiment'}, inplace=True)

    # Join predicts result
    log.info("----------------------------- JOIN DATA")
    df_predict_joined = df2.iloc[:,:].merge(df3.iloc[:,:], left_index=True, right_index=True)

    # Join predict result and raw data
    df_joined = df.merge(df_predict_joined, left_index=True, right_index=True, how='outer')
    df_joined.dtypes
    log.info("----------------------------- PRINT SAMPLE DATA")
    print(df_joined.head())
    # Forward the data
    log.info("----------------------------- LOG DATA")
    df_joined.to_parquet(tempPath, index=False)

    # Log finish execute
    log.info("COMPLETED - Executing join data")
    end_time = time.time() 
    processing_time =  end_time - start_time

    log.info("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    log.info("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    log.info("Processing time : {} seconds".format(processing_time))