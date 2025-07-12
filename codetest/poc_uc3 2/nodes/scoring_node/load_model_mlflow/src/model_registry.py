import os
from typing import Any
import mlflow.sklearn

import os
from mlflow.tracking import MlflowClient
from typing import Any

class MLflowLoadModel:
    """
    A class for loading the latest version of a model using MLflow.

    Attributes:
    -----------
    model_name : str
        The name of the model to be loaded.
    stage : str
        The name of the stage where the latest version of the model is stored in MLflow.
    uri : str or None
        The URI of the latest version of the model in MLflow.

    Methods:
    --------
    get_uri() -> str:
        Gets the URI of the latest version of the model in MLflow.
    load_model() -> Any:
        Loads the model using the URI of the latest version of the model in MLflow.
    """

    def __init__(self, model_name: str, stage: str):
        """
        Initializes the model_name and stage instance variables.

        Parameters:
        -----------
        model_name : str
            The name of the model to be loaded.
        stage : str
            The name of the stage where the latest version of the model is stored in MLflow.
        """
        self.model_name = model_name
        self.stage = stage
        self.uri = None

    def get_uri(self) -> str:
        """
        Gets the URI of the latest version of the model in MLflow.

        Returns:
        --------
        str
            The URI of the latest version of the model in MLflow.
        """
        if self.uri is None:
            self.uri = self._get_latest_model_version()
        return self.uri

    def _get_latest_model_version(self) -> str:
        client = MlflowClient(os.getenv('MLFLOW_URI'))
        model_filter = f"name='{self.model_name}'"
        models = client.search_registered_models(filter_string=model_filter)

        if not models:
            raise ValueError(f"No registered models found with name '{self.model_name}'")

        latest_version = None
        latest_version_uri = None

        for model in models:
            for version in model.latest_versions:
                if version.current_stage == self.stage:
                    if latest_version is None or int(version.version) > int(latest_version):
                        latest_version = version.version
                        latest_version_uri = version.source

        if latest_version is None:
            raise ValueError(f"No model versions found in stage '{self.stage}' for model '{self.model_name}'")

        return latest_version_uri

    def load_model(self, object_=None) -> Any:
        """
        Loads the model using the URI of the latest version of the model in MLflow.

        Returns:
        --------
        Any
            The loaded model.
        """
        uri = self.get_uri()
        return eval(f'mlflow.{object_}.load_model("{uri}")') if object_ else mlflow.pyfunc.load_model(uri)