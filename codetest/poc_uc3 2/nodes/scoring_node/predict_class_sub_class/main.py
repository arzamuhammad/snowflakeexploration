import os
import time
import traceback
import pandas as pd

from src.config import inputFilePath, modelPath, tempPath, referencePath
# from src.logging import log
from src.model_predict import Model_Scoring

def run_pipeline(config_dict):
    # Start execute pipeline
    scoring = Model_Scoring(conf=config_dict)
    scoring.execute()

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Make parent directory
_make_parent_dirs_and_return_path(tempPath)
print(tempPath)
print(f'Is directory: {os.path.isdir(tempPath)}')

if __name__=="__main__":
    # Log start time
    start_time = time.time() 
    print("----------------------------- START - Executing Scoring Process")

    try:
        config_dict = {
            "input_data_path": inputFilePath,
            "model_path": modelPath, 
            "temp_path": tempPath,
            "reference_path": referencePath
        }

        print("------------------all configs set--------------")
        run_pipeline(config_dict)

    except Exception:
        print(traceback.format_exc())
        exit(-1)
        mlflow.end_run()
    
    # Log finish execute
    print("COMPLETED - Executing model scoring")
    end_time = time.time() 
    processing_time =  end_time - start_time

    print("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    print("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    print("Processing time : {} seconds".format(processing_time))