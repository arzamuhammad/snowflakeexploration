import os
import pyspark.sql.functions as f
from triton.utils.spark.etl_helpers import get_current_user
from datetime import *
import sys
from pyspark import SQLContext, SparkContext, SparkConf, HiveContext
from pyspark.sql import HiveContext,DataFrame as spark_df
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from dateutil.relativedelta import relativedelta
from functools import reduce
import pandas as pd

def get_last_partition(hc,table):
    last_partition = (hc.sql("show partitions "+table)
                      .orderBy(desc("partition")).select("partition").collect()[0][0])
    return last_partition.split('=')[1].strip()

def process_data(spark, env):
    hc = HiveContext(spark)

    #define table
    table_1 = f'{env["table_1"]["database"]}.{env["table_1"]["table"]}'
    table_2 = f'{env["table_2"]["database"]}.{env["table_2"]["table"]}'
    table_3 = f'{env["table_3"]["database"]}.{env["table_3"]["table"]}'
    table_4 = f'{env["table_4"]["database"]}.{env["table_4"]["table"]}'

    #define periode
    event_date = f'{env["table_1"]["filter_d2"]}'
    month_date = f'{env["table_4"]["filter_m1"]}'

    print(f"""run for event_date={event_date}""")

    df = hc.sql(f"""
    SELECT a.trx_date,
           a.transaction_id,
           a.sku,
           a.business_id,
           a.rules_id,
           a.msisdn,
           h.brand,
           h.region_hlr,
           a.non_usage_flag,
           a.allowance_sub_type AS allowance_subtype,
           a.allowances_descriptions,
           a.quota_name,
           a.bonus_apps_id,
           a.profile_name,
           h.cust_type_desc,
           h.cust_subtype_desc,
           h.customer_sub_segment,
           coalesce(b.lacci_id, c.lacci_id) AS lacci_id,
           coalesce(b.area_sales, c.area_sales) AS area_sales,
           coalesce(b.region_sales, c.region_sales) AS region_sales,
           coalesce(b.branch, c.branch) AS branch,
           coalesce(b.subbranch, c.subbranch) AS subbranch,
           coalesce(b.cluster_sales, c.cluster_sales) AS cluster_sales,
           coalesce(b.provinsi, c.provinsi) AS provinsi,
           coalesce(b.kabupaten, c.kabupaten) AS kabupaten,
           coalesce(b.kecamatan, c.kecamatan) AS kecamatan,
           coalesce(b.kelurahan, c.kelurahan) AS kelurahan,
           coalesce(b.node_type, c.node_type) AS node_type,
           a.charge,
           sum(alloc_tp) AS rev,
           a.event_date

    FROM (
    SELECT * FROM {table_1}
    WHERE event_date = {event_date}
      AND non_usage_flag = '1'
      AND status = 'OK00'
    ) a

    LEFT JOIN (
    SELECT * FROM {table_2}
    WHERE event_date = {event_date}
    ) h
    ON a.event_date = h.event_date
      AND a.msisdn = h.msisdn

    LEFT JOIN (
    SELECT a.* FROM (
    SELECT event_date,
           msisdn,
           lacci_id,
           area_sales,
           region_sales,
           branch,
           subbranch,
           cluster_sales,
           provinsi,
           kabupaten,
           kecamatan,
           kelurahan,
           node_type,
           rank() over (partition by event_date, msisdn order by lacci_id desc) AS rnk
    FROM {table_3}
    WHERE event_date = {event_date}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    ) a
    WHERE rnk = '1'
    ) b
    ON a.event_date = b.event_date
      AND a.msisdn = b.msisdn

    LEFT JOIN (
    SELECT a.* FROM (
    SELECT event_date,
           msisdn,
           lacci_id,
           area_sales,
           region_sales,
           branch,
           subbranch,
           cluster_sales,
           provinsi,
           kabupaten,
           kecamatan,
           kelurahan,
           node_type,
           rank() over (partition by event_date, msisdn order by lacci_id desc) AS rnk
    FROM {table_4}
    WHERE event_date = {month_date}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    ) a
    WHERE rnk = '1'
    ) c
    ON a.msisdn = c.msisdn

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,31
    """)

    return df

