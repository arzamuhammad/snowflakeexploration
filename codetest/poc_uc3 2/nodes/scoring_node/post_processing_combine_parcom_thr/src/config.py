import argparse
import os

parser = argparse.ArgumentParser(description='custom minio compoment')
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

parser.add_argument('--input-data-1',
                    type=str,
                    required=True,
                    help='Absolute path of the data-1')

parser.add_argument('--input-data-2',
                    type=str,
                    required=True,
                    help='Absolute path of the data-2')

parser.add_argument('--input-data-3',
                    type=str,
                    required=True,
                    help='Absolute path of the data-3')

parser.add_argument('--temp-path', 
                    type=str,
                    required=True,
                    help='Temporary output path of the results')

args = parser.parse_args()
tempPath = args.temp_path

inputPath = [args.input_data_1, args.input_data_2, args.input_data_3]