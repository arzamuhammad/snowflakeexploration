import os
import time
import re 
import pandas as pd
from src.config import inputFilePath, textColumnName, regexPattern, tempPath
from src.logging import setupLogging

# Define logging setup
log = setupLogging(__name__)

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Make directory
_make_parent_dirs_and_return_path(tempPath)

def remove_hashtags_mentions(text): 
    try:
        # Remove hashtags 
        text = text.replace('\n', ' ') 
        text = re.sub(r"#\w+", "", text) 
        # Remove mentions 
        text = re.sub(r"@\w+", "", text) 
        text = re.sub(r'https?:\/\/\S+', '', text) 
        # Return cleaned text 
        return text.encode('ascii', 'ignore').decode('ascii').strip() 
    except:
        return text

def preprocess(df):
    print(f'Data before filtered: {df.shape}')

    df[textColumnName] = df[textColumnName].replace(regexPattern, ' ', regex=True)
    df[textColumnName] = df[textColumnName].str.strip()

     # Remove hastag for socmed/app-review
    if (textColumnName == 'omni_search_t') or (textColumnName == 'body'):
        df[textColumnName] = df[textColumnName].apply(remove_hashtags_mentions)

    try:
        shape = df[(df[textColumnName] != '') & (df[textColumnName].notnull())][textColumnName].reset_index().shape[1]
        df_filter = df[(df[textColumnName] != '') & (df[textColumnName].notnull()) & (df[textColumnName] != 'null')][[textColumnName]]
    except:
        df_filter = df[(df[textColumnName] != '') & (df[textColumnName].notnull()) & (df[textColumnName] != 'null')][textColumnName]

    print(f'Data after filtered: {df_filter.shape}')
    print(df_filter)

    return df_filter



if __name__=="__main__":
    # Log start time
    start_time = time.time() 

    # Load data
    log.info("----------------------------- LOAD DATA")
    try:
        df = pd.read_parquet(inputFilePath).set_index('index')
    except:
        df = pd.read_parquet(inputFilePath) 

    # Filter null and one liner text
    log.info("----------------------------- FILTER DATA")
    df_filter = preprocess(df)

    # Write to tempPath as parquet
    log.info("----------------------------- LOG DATA")
    
    df_filter.reset_index(inplace=True)
    df_filter.to_parquet(tempPath, index=True)
    print(df_filter.head())

    # Log finish execute
    log.info("COMPLETED - Executing filter section")
    end_time = time.time() 
    processing_time =  end_time - start_time

    log.info("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    log.info("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    log.info("Processing time : {} seconds".format(processing_time))