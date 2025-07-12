import os, json
import time
import pandas as pd
import numpy as np
from src.config import *
from src.logging import setupLogging

# Define logging setup
log = setupLogging(__name__)

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

def get_max_values(f):
    try:
        natural_dict = str(f).replace("'", '"')
        dict_file = json.loads(natural_dict)
        return max(dict_file, key=lambda k: dict_file[k])
    except:
        return "nan"

def get_max_values_sub_topic(f):
    threshold_proba = 0.92
    
    try:
        # Check if the input is None or not a dictionary, return nan
        if f is None or not isinstance(f, dict):
            return "nan"
        
        # Filter out None values from the dictionary
        f_filtered = {key: value for key, value in f.items() if value is not None}
        
        if not f_filtered:  # If the filtered dictionary is empty, return nan
            return "nan"
        
        # Find the maximum value in the filtered dictionary
        max_values = max(f_filtered.values())
        
        if max_values > threshold_proba:
            # Find the key associated with the maximum value
            max_keys = next(key for key, value in f_filtered.items() if value == max_values)
            return max_keys
        else:
            return 'others'
    
    except:
        return "nan"

# Make directory
_make_parent_dirs_and_return_path(tempPath)

if __name__=="__main__":
    # Log start time
    start_time = time.time() 

    # Load data
    log.info("----------------------------- LOAD DATA")
    df = pd.read_parquet(inputPath)

    # Rename column
    try:
        print('try!')
        df.rename(columns={'y_pred_topic': 'topic'}, inplace=True)
        df.rename(columns={'y_pred_proba_topic': 'topic_probs'}, inplace=True)

        df.rename(columns={'y_pred_sub_topic': 'sub_topic'}, inplace=True)
        df.rename(columns={'y_pred_proba_sub_topic': 'sub_topic_probs'}, inplace=True)
    except:
        print('except!')
        df.rename(columns={'y_pred_topic': 'topic'}, inplace=True)
        df.rename(columns={'y_pred_proba_topic': 'topic_probs'}, inplace=True)
    
    df.rename(columns={'y_pred_sentiment': 'sentiment'}, inplace=True)
    df.rename(columns={'y_pred_proba_sentiment': 'sentiment_probs'}, inplace=True)

    # Add new column
    if dataSource == 'TNPS':
        print('TNPS!')
        df['id_trx'] = df['id_trx'].astype(str)
        df['source'] = pd.Series(['tnps' for x in range(len(df.index))])
        df['event_id'] = df['source'] + '-' + df['tnps_source'] + '-' + df['id_trx']
    elif dataSource == 'KIP':
        print('KIP!')
        df['source'] = pd.Series(['kip' for x in range(len(df.index))])
        df['event_id'] = df['source'] + '-' + df['kip_id']
    
    # Switch the values directly for App-review and social-media
    if (dataSource == 'App-Review') or (dataSource == 'Social-Media'):
        print('App-review or Social-media!')
        df['topic'] = df['topic_probs'].apply(get_max_values)
        df['sub_topic'] = df['sub_topic_probs'].apply(get_max_values_sub_topic)
        df['sentiment'] = df['sentiment_probs'].apply(get_max_values)

    # Debugging
    print(df.head())
    print(df.shape)
    print(tempPath)

    # Forward the data
    log.info("----------------------------- LOG DATA")
    df.to_parquet(tempPath, index=False)

    # Log finish execute
    log.info("COMPLETED - Executing join data")
    end_time = time.time() 
    processing_time =  end_time - start_time

    log.info("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    log.info("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    log.info("Processing time : {} seconds".format(processing_time))
