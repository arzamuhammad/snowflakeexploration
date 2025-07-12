import os
import yaml
import argparse
import traceback

from pyspark.conf import SparkConf
from urllib.request import Request, urlopen
from yaml.loader import SafeLoader

parser = argparse.ArgumentParser(description='custom minio compoment')
parser = argparse.ArgumentParser(description='Download file from public HTTP/S URL')

parser.add_argument('--data-source',
                    type=str,
                    required=True,
                    help='Data source')
parser.add_argument('--file-path',
                    type=str,
                    required=True,
                    help='Absolute path of the file')
parser.add_argument('--database-name',
                    type=str,
                    required=True,
                    help='Destination Hive database')
parser.add_argument('--table-name',
                    type=str,
                    required=True,
                    help='Destination hive table')
parser.add_argument('--write-mode',
                    type=str,
                    required=True,
                    help='Write mode (append/overwrite)')
parser.add_argument('--repo-name',
                    type=str,
                    required=True,
                    help='existing repo name')

args = parser.parse_args()
dataSource = args.data_source
filePath = args.file_path
databaseName = args.database_name
tableName = args.table_name
writeMode = args.write_mode
repoName = args.repo_name

workingDir = "ext_to_hive"

sparkConfFilePath = repoName+"/nodes/scoring_node/"+workingDir+"/"+"spark.yml"

sparkConf = SparkConf()
with open(sparkConfFilePath) as f:
    confData = yaml.load(f, Loader=SafeLoader)
for confItem in confData:
    sparkConf.set(confItem,confData[confItem])