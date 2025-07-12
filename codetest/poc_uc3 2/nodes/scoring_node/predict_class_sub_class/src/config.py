import argparse
import traceback
import os
import yaml

from urllib.request import Request, urlopen
from yaml.loader import SafeLoader
import torch

parser = argparse.ArgumentParser(description='custom minio compoment')
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

parser.add_argument('--input-file',
                    type=str,
                    required=True,
                    help='Absolute path of the file/data')

parser.add_argument('--model-path',
                    type=str,
                    required=True,
                    help='Model path of the run')

parser.add_argument('--reference-path',
                    type=str,
                    required=True,
                    help='Reference path of the run')

parser.add_argument('--temp-path',
                    type=str,
                    required=True,
                    help='Temporary output path of the results')

args = parser.parse_args()
inputFilePath = args.input_file
modelPath = args.model_path
referencePath = args.reference_path
tempPath = args.temp_path

print(f'torch version: {torch.__version__}')