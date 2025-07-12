from typing import Dict

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import bround

def include_column_to_store(
    scoring_table: pyspark.sql.DataFrame, columns_to_include: Dict[str, str]
) -> pyspark.sql.DataFrame:
    """
    Reformat and renamed column to be stored 

    :param scoring_table: Table to score
    :param columns_to_include: Columns to be renamed and included
    :return: Hive table
    """
    scoring_table = scoring_table.select(list(columns_to_include.keys()))
    scoring_table = scoring_table.withColumn(
        "msisdn", f.col("msisdn").cast(StringType())
    )
    scoring_table = scoring_table.withColumn(
        "refresh_date", f.col("refresh_date").cast(StringType())
    )

    for column, column_rename in columns_to_include.items():
        scoring_table = scoring_table.withColumnRenamed(column, column_rename)

    return scoring_table

def store_to_hive(
    scoring_table: pyspark.sql.DataFrame,
    database_name: str,
    table_name: str
    ):
    scoring_table.write.format('hive').mode('append').partitionBy("refresh_date","model_id").option("compression","snappy").saveAsTable(database_name+"."+table_name)
    return

