import argparse
from minio import Minio

parser = argparse.ArgumentParser(description="Read file from public HTTP/S URL")

parser.add_argument("--bucket-name", type=str, required=True, help="bucket name")

parser.add_argument("--dataset-name", type=str, required=True, help="dataset name")

parser.add_argument("--temp-output-path", type=str, required=True, help="temporary Path of the results")

parser.add_argument("--src-url", type=str, required=True, help="source minio url")

parser.add_argument("--src-acc-key", type=str, required=True, help="source access key")

parser.add_argument("--src-sec-key", type=str, required=True, help="source secret key ")

# source user input
args = parser.parse_args()

source_bucket_name = args.bucket_name
source_dataset_name = args.dataset_name
download_file = args.temp_output_path
host = args.src_url
source_key = args.src_acc_key
secret_key = args.src_sec_key

source_client = Minio(host, access_key=source_key, secret_key=secret_key, secure=False)