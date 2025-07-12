import argparse
import os

parser = argparse.ArgumentParser(description='custom minio compoment')
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

parser.add_argument('--input-path',
                    type=str,
                    required=True,
                    help='Absolute path of the raw file/data')

parser.add_argument('--data-source',
                    type=str,
                    required=True,
                    help='Data source')

parser.add_argument('--temp-path',
                    type=str,
                    required=True,
                    help='Temporary output path of the results')

args = parser.parse_args()
inputPath = args.input_path
dataSource = args.data_source
tempPath = args.temp_path