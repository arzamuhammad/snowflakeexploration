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
                    help='Name of text column to be read')
parser.add_argument('--regex-pattern',
                    type=str,
                    required=True,
                    help='Regex pattern of text column will be removed')
parser.add_argument('--temp-path',
                    type=str,
                    required=True,
                    help='Temporary output path of the results')

args = parser.parse_args()
inputFilePath = args.input_file_path
textColumnName = args.text_column_name
regexPattern = args.regex_pattern
tempPath = args.temp_path