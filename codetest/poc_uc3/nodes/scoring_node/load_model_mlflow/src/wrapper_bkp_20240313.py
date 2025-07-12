import json
import numpy as np
import pandas as pd
import mlflow.pyfunc
import torch
from transformers import BertTokenizer, BertModel
from torch.utils.data import Dataset, DataLoader
from torch import nn
import torch.nn.functional as F

class TselDataset(Dataset):
    def __init__(self, texts, targets, tokenizer, max_len):
        self.texts = texts
        self.targets = targets
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, item):
        review = str(self.texts[item])
        target = self.targets[item]

        encoding = self.tokenizer.encode_plus(
            review,
            add_special_tokens=True,
            max_length=self.max_len,
            return_token_type_ids=False,
            #pad_to_max_length=True,
            padding='max_length',
            #padding=True,
            truncation=False,
            return_attention_mask=True,
            return_tensors='pt',
        )

        return {
            'review_text': review,
            'input_ids': encoding['input_ids'].flatten(),
            'attention_mask': encoding['attention_mask'].flatten(),
            'targets': torch.tensor(target, dtype=torch.long)
        }

class BertNLPWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, no_of_classes, max_len, dropout):
        self.no_of_classes = no_of_classes
        self.max_len = max_len
        self.dropout = dropout

    def load_context(self, context):
        # Define model
        self.model = BertModel.from_pretrained(context.artifacts["pretrained_bert"])
        self.drop = nn.Dropout(p=self.dropout)
        self.out = nn.Linear(self.model.config.hidden_size, self.no_of_classes)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self.model.to(self.device)

        state_dict = torch.load(context.artifacts["finetuned_model"], map_location=self.device)

        try:
            self.model.load_state_dict({k.replace("bert.", ""): v for k, v in state_dict.items() if "bert." in k})
            self.out.load_state_dict({k.replace("out.", ""): v for k, v in state_dict.items() if "out." in k})
        except:
            self.model.load_state_dict(state_dict)

        # Load another
        self.tokenizer = BertTokenizer.from_pretrained(context.artifacts["tokenizer"])
        self.class_params = json.load(open(context.artifacts['class_params']))        

    def load_class_params(self, context):
        return self.class_params

    def pairing_function(self, x, y):
        mydic = {}
        for i in zip(dict(sorted(y.items(), key=lambda x:x[1])), x):
            mydic[i[0]] = i[1]

        return mydic

    def predict(self, context, data):
        predictions = []
        prediction_probs = []

        # Create data loader
        ds = TselDataset(
            texts=data.iloc[:,0].to_numpy(),
            targets=data.iloc[:,1].to_numpy(),
            tokenizer=self.tokenizer,
            max_len=self.max_len
        )

        data_loader = DataLoader(
            ds,
            batch_size=16,
            num_workers=2
        )

        # Predict
        self.model = self.model.eval()

        with torch.no_grad():
            for d in data_loader:
                input_ids = d["input_ids"].to(self.device)
                attention_mask = d["attention_mask"].to(self.device)

                outputs = self.model(
                    input_ids=input_ids, 
                    attention_mask=attention_mask
                )
                outputs = self.drop(outputs[1])
                outputs = self.out(outputs)

                # Process the output 
                _, preds = torch.max(outputs, dim=1)
                probs = F.softmax(outputs, dim=1)

                predictions.extend(preds)
                prediction_probs.extend(probs)

        y_predictions = torch.stack(predictions).cpu()
        y_prediction_probs = torch.stack(prediction_probs).cpu()

        y_prediction_probs_df = pd.DataFrame({
            'y_pred_proba': y_prediction_probs.tolist()
        })

        y_prediction_probs = np.vectorize(self.pairing_function)(
            y_prediction_probs_df['y_pred_proba'], self.class_params)

        return y_predictions, y_prediction_probs