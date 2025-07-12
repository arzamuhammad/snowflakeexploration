import argparse
import os

parser = argparse.ArgumentParser(description='custom minio compoment')
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

parser.add_argument('--input-file-path',
                    type=str,
                    required=True,
                    help='Absolute path of the file/data')

parser.add_argument('--text-column-name',
                    type=str,
                    required=True,
                    help='Name of text column')

parser.add_argument('--pre-defined-list',
                    type=str,
                    required=True,
                    help='Pre-defined list parquet file')

parser.add_argument('--temp-path',
                    type=str,
                    required=True,
                    help='Temporary output path-1 of the results')

args = parser.parse_args()
inputFilePath = args.input_file_path
textColumnName = args.text_column_name
pre_defined_list = args.pre_defined_list
tempPath = args.temp_path