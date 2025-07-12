import os
import time
import pandas as pd
from src.config import tempPath, inputPath
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

    df = ['']*len(inputPath)

    for i in range(len(inputPath)):
        df[i] = pd.read_parquet(inputPath[i]).set_index('index')
        print(f'Data shape in split-{i+1}: {df[i].shape}')

        # Join predicts result
        if i == 0:
            df_joined = df[i]
        else:
            df_joined = pd.concat([df_joined, df[i]])

    print(f'Data shape after combined-{i+1}: {df_joined.shape}')

    # Forward the data
    log.info("----------------------------- LOG DATA")
    df_joined.to_parquet(tempPath, index=True)

    # Log finish execute
    log.info("COMPLETED - Executing join data")
    end_time = time.time() 
    processing_time =  end_time - start_time

    log.info("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    log.info("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    log.info("Processing time : {} seconds".format(processing_time))