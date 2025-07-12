import argparse

parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

parser.add_argument('--model-name',
                    type=str,
                    required=True,
                    help='model name')
parser.add_argument('--stage',
                    type=str,
                    required=True,
                    help='model stage')
parser.add_argument('--temp',
                    type=str,
                    required=True,
                    help='temporary Path of the results')

args = parser.parse_args()

# source user input 
model_name = args.model_name
model_stage = args.stage
temp_output_path = args.temp