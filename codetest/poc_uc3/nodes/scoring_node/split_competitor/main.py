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
_make_parent_dirs_and_return_path(tselPath)
_make_parent_dirs_and_return_path(competitorPath)

def json_loads(x):
    try:
        return json.loads(x)
    except:
        return []

if __name__=="__main__":
    # Log start time
    start_time = time.time() 

    # Load data
    log.info("----------------------------- LOAD DATA")
    df = pd.read_parquet(inputPath)

    # Filter
    df['campaign_id_ls'] = df['campaign_id_ls'].apply(json_loads)
    
    df_final_tsel = df[df['campaign_id_ls'].apply(lambda x: any(item in list_tsel_business for item in x))]
    df_final_competitor = df[~df['campaign_id_ls'].apply(lambda x: any(item in list_tsel_business for item in x))]

    print('\nTsel data sample:')
    print(df_final_tsel.shape)
    print(df_final_tsel.head())

    print('\nCompetitor data sample:')
    print(df_final_competitor.shape)
    print(df_final_competitor.head())
    print() 
    
    # Forward the data
    log.info("----------------------------- LOG DATA")
    df_final_tsel.reset_index(inplace=True)
    df_final_tsel.to_parquet(tselPath, index=True) 
    df_final_competitor.reset_index(inplace=True)
    df_final_competitor.to_parquet(competitorPath, index=True)

    # Log finish execute
    log.info("COMPLETED - Executing join data")
    end_time = time.time() 
    processing_time =  end_time - start_time

    log.info("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    log.info("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    log.info("Processing time : {} seconds".format(processing_time))