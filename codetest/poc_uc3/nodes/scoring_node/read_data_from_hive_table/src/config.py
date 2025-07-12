import os
import argparse
import yaml
from yaml.loader import SafeLoader
from pyspark.conf import SparkConf
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

repo_name = os.getenv('REPO_NAME')
parser = argparse.ArgumentParser(description="Read from Hive table compoment")

parser.add_argument(
    "--database-name", 
    type=str, 
    required=True, 
    help="Name of database/scheme"
)

parser.add_argument(
    "--table-name", 
    type=str, 
    required=True, 
    help="Name of table"
)

parser.add_argument(
    "--event-date", 
    type=str, 
    required=False, 
    help="Name of table"
)

parser.add_argument(
    "--temp-output-path", 
    type=str, 
    required=True, 
    help="temporary Path of the results"
)

args = parser.parse_args()
database_name = args.database_name
table_name = args.table_name
event_date = args.event_date
temp_path = args.temp_output_path
    
if not event_date:      # tidak diisi (running date)
    event_date = date.today()
    event_date = event_date.strftime('%Y-%m-%d')
    event_date = datetime.strptime(event_date, '%Y-%m-%d')

    if not 'rating' in table_name:
        event_date = (event_date - relativedelta(days=1)).strftime('%Y-%m-%d')
        print('Current Date = ', event_date)
    else:
        event_date = (event_date - relativedelta(days=3)).strftime('%Y-%m-%d')
        print('Rating D-3 = ', event_date)
else:   
    print('Event Date = ', event_date)

sparkConfFilePath = '{repo_name}/nodes/scoring_node/read_data_from_hive_table/src/spark.yaml'.format(repo_name=repo_name)

# Spark Config
sparkConf = SparkConf()
with open(sparkConfFilePath) as f:
    confData = yaml.load(f, Loader=SafeLoader)
for confItem in confData:
    sparkConf.set(confItem, confData[confItem])