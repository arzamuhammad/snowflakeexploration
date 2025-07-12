import argparse
import os

parser = argparse.ArgumentParser(description='custom minio compoment')
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

parser.add_argument('--input-path',
                    type=str,
                    required=True,
                    help='Absolute path of the raw file/data')
parser.add_argument('--input-sentiment-path',
                    type=str,
                    required=True,
                    help='Absolute path of the file/data')
parser.add_argument('--input-topic-path',
                    type=str,
                    required=True,
                    help='Absolute path of the file/data')
parser.add_argument('--temp-path',
                    type=str,
                    required=True,
                    help='Temporary output path of the results')

args = parser.parse_args()
inputPath = args.input_path
inputSentimentPath = args.input_sentiment_path
inputTopicPath = args.input_topic_path
tempPath = args.temp_path