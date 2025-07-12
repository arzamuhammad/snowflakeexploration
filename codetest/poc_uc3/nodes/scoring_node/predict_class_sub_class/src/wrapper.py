import json
import time as ttime
import numpy as np
import pandas as pd
import mlflow.pyfunc
import torch
from transformers import BertTokenizer, BertModel
from torch.utils.data import Dataset, DataLoader
from torch import nn
from sklearn.metrics.pairwise import cosine_similarity
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
            truncation=True,
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

        # Freeze all layers
        for param in self.model.parameters():
            param.requires_grad = False

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

    def predict(self, context, datas):
        predictions = []
        prediction_probs = []
        lhs = []
        attention_mask = []

        data = datas[0]
        references_df = datas[1]


        # Create data loader
        ds = TselDataset(
            texts=data.iloc[:,0].to_numpy(),
            targets=data.iloc[:,1].to_numpy(),
            tokenizer=self.tokenizer,
            max_len=self.max_len
        )

        data_loader = DataLoader(
            ds,
            batch_size=2048,
            num_workers=8
        )

        # Predict
        self.model = self.model.eval()

        with torch.no_grad():
            i = 1

            for d in data_loader:
                start_time = ttime.time()
                print(f'\nBatch loader looping-{i}..')
                i = i + 1
                
                input_ids = d["input_ids"].to(self.device)
                attention_masked = d["attention_mask"].to(self.device)

                pooled_output = self.model(
                    input_ids=input_ids, 
                    attention_mask=attention_masked
                )

                last_hidden_states = pooled_output[0]
                outputs = self.drop(pooled_output[1])
                outputs = self.out(outputs)

                # Process the output 
                _, preds = torch.max(outputs, dim=1)
                probs = F.softmax(outputs, dim=1)

                predictions.extend(preds)
                prediction_probs.extend(probs)
                lhs.extend(last_hidden_states)
                attention_mask.extend(attention_masked)

                print(f'Processing time: {ttime.time()-start_time} seconds')

        y_predictions = torch.stack(predictions).cpu()
        y_prediction_probs = torch.stack(prediction_probs).cpu()

        y_prediction_probs_df = pd.DataFrame({
            'y_pred_proba': y_prediction_probs.tolist()
        })

        y_prediction_probs = np.vectorize(self.pairing_function)(
            y_prediction_probs_df['y_pred_proba'], self.class_params)

        # PREDICT SUB-CLASS
        y_sub_class = []
        y_probs_sub_class = []
        mean_pooled_references = []
        mean_pooled_predicted_data = []

        # Collect sub-class to list
        collected_references_df = pd.DataFrame(references_df.groupby('class-label')['text-references'].apply(list)).reset_index().sort_values('class-label')
        print('\ncollected_references_df:')
        print(collected_references_df)

        # Encode the training sentences
        for classes in collected_references_df['class-label']:
            # Take sentences of every class in loop
            sentences = collected_references_df[collected_references_df['class-label']==classes]['text-references'].tolist()[0]

            tokens = {
                'input_ids': [], 
                'attention_mask': []
            }

            for sentence in sentences:
                new_tokens = self.tokenizer.encode_plus(sentence, max_length=100,
                                       truncation=True, padding='max_length',
                                       return_tensors='pt')

                tokens['input_ids'].append(new_tokens['input_ids'][0])
                tokens['attention_mask'].append(new_tokens['attention_mask'][0])

            tokens['input_ids'] = torch.stack(tokens['input_ids'])
            tokens['attention_mask'] = torch.stack(tokens['attention_mask'])

            outputs = self.model(**tokens)
            lhs_ref = outputs[0]

            outputs = self.drop(outputs[1])
            outputs = self.out(outputs)
            
            embeddings_ref = lhs_ref
            attention_mask_ref = tokens['attention_mask']

            mask_ref = attention_mask_ref.unsqueeze(-1).expand(embeddings_ref.size()).float()
            masked_embeddings_ref = embeddings_ref * mask_ref

            summed_ref = torch.sum(masked_embeddings_ref, 1)
            summed_mask_ref = torch.clamp(mask_ref.sum(1), min=1e-9)

            mean_pooled_ref = summed_ref / summed_mask_ref
            mean_pooled_ref = mean_pooled_ref.detach().numpy()

            mean_pooled_references.append(mean_pooled_ref)

        for i, last_hidden_state in enumerate(lhs):
            embeddings = last_hidden_state
            converted_attention_mask = torch.tensor([attention_mask[i].tolist()])

            dim_1 = converted_attention_mask.size()[0]
            dim_2 = converted_attention_mask.size()[1]

            mask = converted_attention_mask.unsqueeze(-1).expand(embeddings.reshape(dim_1,dim_2,-1).size()).float()
            masked_embeddings = embeddings * mask

            summed = torch.sum(masked_embeddings, 1)
            summed_mask = torch.clamp(mask.sum(1), min=1e-9)

            mean_pooled = summed / summed_mask
            mean_pooled = mean_pooled.detach().numpy()

            mean_pooled_predicted_data.append(mean_pooled)

        # Get key
        keys = [references_df[references_df['class-label'] == i]['sub-class-references'].tolist() for i in range(references_df['class-label'].nunique())]
        print('\nkeys:')
        print(keys)
        print()

        # Predict sub_class in loop
        for i, data in enumerate(y_predictions):
            y_predicted_cosine_similarity = cosine_similarity(
                mean_pooled_predicted_data[i],
                mean_pooled_references[data][:]
            )

            _y_probs_sub_class = {keys[int(data)][idx]: y_predicted_cosine_similarity[0][idx] for idx in range(len(keys[int(data)]))}
            _y_predicted_cosine_similarity = np.argmax(y_predicted_cosine_similarity)

            y_sub_class.append(_y_predicted_cosine_similarity)
            y_probs_sub_class.append(_y_probs_sub_class)

        return y_predictions, y_prediction_probs, y_sub_class, y_probs_sub_class