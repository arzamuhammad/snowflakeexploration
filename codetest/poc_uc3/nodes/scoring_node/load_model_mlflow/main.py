import os
import logging
import cloudpickle

from src.config import model_name, model_stage, temp_output_path
from src.model_registry import MLflowLoadModel
from src.wrapper import *

logger = logging.getLogger(__name__)

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
_make_parent_dirs_and_return_path(temp_output_path)

def load_model(model_name: str, model_stage: str, temp_path: str):
    model = MLflowLoadModel(model_name=model_name, stage=model_stage).load_model()

    logger.info("Successfully loaded MLflow model {}".format(model_name))
    logger.info(model)

    cloudpickle.dump(model, open(temp_path, "wb"))

load_model(model_name, model_stage, temp_output_path)