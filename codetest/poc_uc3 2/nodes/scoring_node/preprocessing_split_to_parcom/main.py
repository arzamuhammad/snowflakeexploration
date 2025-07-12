import os
import time
import pandas as pd
from src.config import inputFilePath, n_fold, tempPath
from src.logging import setupLogging

# Define logging setup
log = setupLogging(__name__)

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

if __name__=="__main__":
    # Log start time
    start_time = time.time() 

    # Load data
    log.info("----------------------------- LOAD DATA")
    df = pd.read_parquet(inputFilePath)

    # Processing
    ceiling_index = int(df.shape[0]/n_fold) + 1
    df_split = ['']*n_fold

    for i in range(n_fold):
        # Split data
        df_split[i] = df.iloc[ceiling_index*i:ceiling_index*(i+1),:]

        print(f'Data shape in split-{i+1}: {df_split[i].shape}')

    # Write to tempPath as parquet
    log.info("----------------------------- LOG DATA")
    for i in range(5):
        # Make directory
        _make_parent_dirs_and_return_path(tempPath[i])

        if i < n_fold:
            df_split[i].to_parquet(tempPath[i], index=False)
        else:
            df_split[n_fold-1].to_parquet(tempPath[i], index=False)

    # Log finish execute
    log.info("COMPLETED - Executing filter section")
    end_time = time.time() 
    processing_time =  end_time - start_time

    log.info("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    log.info("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    log.info("Processing time : {} seconds".format(processing_time))