from pathlib import Path
import os
import datetime 

from src.config import source_bucket_name, source_folder_name, download_path, source_client
from src.utils import download_folder

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

_make_parent_dirs_and_return_path(download_path)
#_make_parent_dirs_and_return_path(local_path)

download_folder(source_client, source_bucket_name, source_folder_name, download_path)

print("Completed Reading data from Minio")