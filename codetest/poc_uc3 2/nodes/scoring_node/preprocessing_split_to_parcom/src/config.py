import argparse
import os

parser = argparse.ArgumentParser(description='custom minio compoment')
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

parser.add_argument('--input-file-path',
                    type=str,
                    required=True,
                    help='Absolute path of the file/data')

parser.add_argument('--input-n',
                    type=str,
                    required=True,
                    help='Splitted to n-file')

parser.add_argument('--temp-path-1',
                    type=str,
                    required=True,
                    help='Temporary output path-1 of the results')

parser.add_argument('--temp-path-2',
                    type=str,
                    required=True,
                    help='Temporary output path-2 of the results')

parser.add_argument('--temp-path-3',
                    type=str,
                    required=False,
                    help='Temporary output path-3 of the results')

parser.add_argument('--temp-path-4',
                    type=str,
                    required=False,
                    help='Temporary output path-4 of the results')

parser.add_argument('--temp-path-5',
                    type=str,
                    required=False,
                    help='Temporary output path-5 of the results')

args = parser.parse_args()
inputFilePath = args.input_file_path
n_fold = int(args.input_n)
pathTemporary = [args.temp_path_1, args.temp_path_2, args.temp_path_3, args.temp_path_4, args.temp_path_5]
tempPath = []
for i in range(5):
    tempPath.append(pathTemporary[i])