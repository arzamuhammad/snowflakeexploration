import mlflow.pyfunc
import torch
from transformers import BertTokenizer, BertModel
from torch import nn
import torch.nn.functional as F

class BertNLPWrapper(mlflow.pyfunc.PythonModel):

    def __init__(self, no_of_classes, max_len):
        self.no_of_classes = no_of_classes
        self.max_len = max_len

    def load_context(self, context):
        self.tokenizer = BertTokenizer.from_pretrained(context.artifacts["tokenizer"])
        self.model = BertModel.from_pretrained(context.artifacts["pretrained_bert"])
        self.drop = nn.Dropout(p=0.2)
        self.out = nn.Linear(self.model.config.hidden_size, self.no_of_classes)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Load model state_dict
        state_dict = torch.load(context.artifacts["finetuned_model"], map_location=self.device)
        self.model.load_state_dict({k.replace("bert.", ""): v for k, v in state_dict.items() if "bert." in k})
        self.out.load_state_dict({k.replace("out.", ""): v for k, v in state_dict.items() if "out." in k})

        self.model = self.model.to(self.device)
        self.model.eval()

    def predict(self, context, data):
        texts = [data]
        BATCH_SIZE = 16

        # Prepare the data
        encoding = self.tokenizer.batch_encode_plus(
            texts, 
            add_special_tokens=True,
            max_length=self.max_len,
            return_token_type_ids=False,
            padding='max_length',
            truncation=True,
            return_attention_mask=True,
            return_tensors='pt'
        )

        input_ids = encoding["input_ids"].to(self.device)
        attention_mask = encoding["attention_mask"].to(self.device)

        # Make predictions
        with torch.no_grad():
            pooled_output = self.model(input_ids=input_ids, attention_mask=attention_mask)
            output = self.drop(pooled_output[1])
            outputs = self.out(output)

        # Process the output
        _, preds = torch.max(outputs, dim=1)
        probs = F.softmax(outputs, dim=1)
        return {
            'predictions': preds.cpu().tolist(),
            'prediction_probability': probs.cpu().tolist()
        }