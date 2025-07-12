import argparse
from minio import Minio
import os
from pathlib import Path
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')


parser.add_argument('--bucket-name',
                    type=str,
                    required=True,
                    help='bucketname')

parser.add_argument('--folder-name',
                    type=str,
                    required=True,
                    help='folder name')

parser.add_argument('--host',
                    type=str,
                    required=True,
                    help='minio host')

parser.add_argument('--access-key',
                    type=str,
                    required=True,
                    help='minio access key')

parser.add_argument('--secret-key',
                    type=str,
                    required=True,
                    help='minio secret key')

parser.add_argument('--temp-output-path',
                    type=str,
                    required=True,
                    help='temporary Path of the results')


args = parser.parse_args()

repo_name = os.getenv('REPO_NAME')


# source user input 
source_bucket_name = args.bucket_name
source_folder_name = args.folder_name
download_path = args.temp_output_path
host= args.host
access_key = args.access_key
secret_key = args.secret_key

source_client = Minio(host,access_key=access_key,secret_key=secret_key,secure=False)
