import os
import numpy as np
import pandas as pd
import mlflow
import cloudpickle as pickle

from src.wrapper import BertNLPWrapper

class Model_Scoring:
    def __init__(self, conf):
        self.conf = conf
        self.input_data_path = self.conf['input_data_path']
        self.model_path = self.conf['model_path']
        self.temp_path = self.conf['temp_path']
        self.reference_path = self.conf['reference_path']

    def load_data(self, input_data_path):
        '''
        This function will open data from parquet
        '''
        with open(input_data_path, "rb") as file:
            dataframe = pd.read_parquet(file)
        dataframe = pd.DataFrame({
            'index': dataframe.iloc[:,0],
            'free_text': dataframe.iloc[:,1],
            'CJX_version': [0]*dataframe.shape[0]
        })

        return dataframe

    def load_training_data(self, input_data_path):
        '''
        This function will open data from parquet
        '''
        with open(input_data_path, "rb") as file:
            dataframe = pd.read_csv(file)

        return dataframe

    def load_model(self, model_path):
        '''
        This function will load model from MLFlow
        '''
        with open(model_path, "rb") as model:
            model = pickle.load(model)
            print('model :', model)

        return model

    def model_predict(self, model, data_loader, df):
        '''
        This function will predict data using function through MLFlow wrapper
        '''
        y_pred, y_pred_probs, y_sub_pred, y_sub_pred_probs = model.predict([data_loader, df])
        
        return y_pred, y_pred_probs, y_sub_pred, y_sub_pred_probs

    def pairing_function(self, x, y):
        mydic = {}

        for i in zip(dict(sorted(y.items(), key=lambda x:x[1])), x):
            mydic[i[0]] = i[1]

        return mydic
 
    def execute(self):
        # Load data
        print("----------------------------- START - LOAD DATA")
        dataframe = self.load_data(self.input_data_path)
        training_dataframe = self.load_training_data(self.reference_path)

        print("----------------------------- END - LOAD DATA")

        # Load model and tokenizer
        print("----------------------------- START - LOAD MODEL")
        model = self.load_model(self.model_path)

        print("----------------------------- END - LOAD MODEL")

        # Predict class
        print("----------------------------- START - PREDICT CLASS")
        y_pred, y_pred_probs, y_pred_sub_class, y_pred_probs_sub_class = self.model_predict(
            model, dataframe[['free_text', 'CJX_version']], training_dataframe)

        print("----------------------------- END - PREDICT CLASS")

        # Create new dataframe and map to class param dictionary json
        print("----------------------------- START - LOGGING DATA")
        df_predict = pd.DataFrame({
            'index': dataframe.iloc[:,0],
            'y_pred': y_pred,
            'y_pred_proba': y_pred_probs.tolist(),
            'y_pred_sub_class': y_pred_sub_class,
            'y_pred_proba_sub_class': y_pred_probs_sub_class
        })

        print(df_predict.head())

        # Write to tempPath as parquet
        df_predict.to_parquet(self.temp_path, index=False)

        print("----------------------------- START - LOGGING DATA")