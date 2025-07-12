import argparse
import os
import json

parser = argparse.ArgumentParser(description='custom minio compoment')
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

def json_with_single_quotes(s):
    print(s)
    # Convert single quotes to double quotes, and ensure booleans are lowercase
    s = s.replace('"', '')
    s = s.replace("'", '"').replace("False", "false").replace("True", "true")
    return json.loads(s)

parser.add_argument('--input-path',
                    type=str,
                    required=True,
                    help='Absolute path of the raw file/data')

parser.add_argument('--input-tsel-business',
                    type=json_with_single_quotes,
                    required=True,
                    help='Absolute path of the file/data')

parser.add_argument('--tsel-path',
                    type=str,
                    required=True,
                    help='Temporary output path of the results')

parser.add_argument('--competitor-path',
                    type=str,
                    required=True,
                    help='Temporary output path of the results')

args = parser.parse_args()
inputPath = args.input_path
init_tsel_list_business = args.input_tsel_business
tselPath = args.tsel_path
competitorPath = args.competitor_path

# Parse params
list_tsel_business = init_tsel_list_business
# list_tsel_business = {}
# for key in init_tsel_list_business:
#     list_tsel_business.append(key)