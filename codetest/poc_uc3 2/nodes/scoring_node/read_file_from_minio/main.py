import os
from src.logging import setupLogging
from src.config import (
    download_file,
    source_bucket_name,
    source_client,
    source_dataset_name,
)

logger = setupLogging(__name__)
# local_path = '/temp/downloaded_data/'

def _make_parent_dirs_and_return_path(file_path: str):
    os.makedirs(os.path.join(os.path.dirname(file_path)), exist_ok=True)

_make_parent_dirs_and_return_path(download_file)

# Read data from MinIO and put it in download_file path
logger.info(f"----------------------------- Download File Path: {download_file}")
logger.info("----------------------------- Read data from MinIO")

# final_path = download_file
source_client.fget_object(source_bucket_name, source_dataset_name, download_file)

download_file = os.path.join(download_file, source_dataset_name)
logger.info(f"----------------------------- Joined Download File Path: {download_file}")
logger.info("----------------------------- Completed Reading data from MinIO")