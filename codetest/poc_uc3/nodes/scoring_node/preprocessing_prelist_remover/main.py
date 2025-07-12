import os
import time
import pandas as pd
from src.config import inputFilePath, tempPath, pre_defined_list, textColumnName
from src.logging import setupLogging

# Define logging setup
log = setupLogging(__name__)

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

def take_1(a):
    try:
        return a.split('~')[0]
    except:
        return a

def take_2(a):
    try:
        return a.split('~')[1]
    except:
        return ''

def take_3(a):
    try:
        return a.split('~')[2]
    except:
        return ''

def take_summary(text):
    global predefined_list

    if text.lower() in predefined_list['lists'].tolist():
        return ''
    else:
        return text.replace('NULL', '')

_make_parent_dirs_and_return_path(tempPath)

if __name__=="__main__":
    # Log start time
    start_time = time.time() 

    # Load data
    log.info("----------------------------- LOAD DATA")
    df = pd.read_parquet(inputFilePath)
    predefined_list = pd.read_csv(pre_defined_list)
    
    print(df.info())
    print(df.head())
 
    # PROCESSING
    # Split column
    col1_web = df[df['tnps_source']=='web'][textColumnName].apply(take_1)
    col2_web = df[df['tnps_source']=='web'][textColumnName].apply(take_2)
    col3_web = df[df['tnps_source']=='web'][textColumnName].apply(take_3)

    col1_tsm = df[df['tnps_source']=='tsm'][textColumnName].apply(take_1)
    col2_tsm = df[df['tnps_source']=='tsm'][textColumnName].apply(take_2)

    df['cols1'] = pd.concat([df[df['tnps_source']=='va'][textColumnName], col1_web, col1_tsm]).sort_index(ascending=True)
    df['cols2'] = pd.concat([col2_web, col2_tsm]).sort_index(ascending=True)
    df['cols3'] = pd.concat([col3_web]).sort_index(ascending=True)

    # Remove NULL
    df["cols1"].fillna("NULL", inplace = True)
    df["cols2"].fillna("NULL", inplace = True)
    df["cols3"].fillna("NULL", inplace = True)

    # Apply function
    df['cols1_preprocess'] = df['cols1'].apply(take_summary)
    df['cols2_preprocess'] = df['cols2'].apply(take_summary)
    df['cols3_preprocess'] = df['cols3'].apply(take_summary)

    # Concat data
    df['final_text'] = df['cols1_preprocess'] + df['cols2_preprocess'] + df['cols3_preprocess']

    df_new = pd.DataFrame()
    df_new[textColumnName] = df['final_text']

    # Write to tempPath as parquet
    log.info("----------------------------- LOG DATA")
    df_new.to_parquet(tempPath, index=True)

    # Log finish execute
    log.info("COMPLETED - Executing filter section")
    end_time = time.time() 
    processing_time =  end_time - start_time

    log.info("Start time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))))
    log.info("End time : {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))))
    log.info("Processing time : {} seconds".format(processing_time))