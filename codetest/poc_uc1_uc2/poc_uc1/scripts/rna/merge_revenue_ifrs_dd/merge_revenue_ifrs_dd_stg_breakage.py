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
    table_5 = f'{env["table_5"]["database"]}.{env["table_5"]["table"]}'

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
           a.brand,
           a.region_hlr,
           a.non_usage_flag,
           a.allowance,
           a.allowance_sub_type,
           a.allowances_descriptions,
           a.quota_name,
           a.bonus_apps_id,
           a.profile_name,
           a.cust_type_desc,
           a.cust_subtype_desc,
           a.customer_sub_segment,
           a.lacci_id,
           a.area_sales,
           a.region_sales,
           a.branch,
           a.subbranch,
           a.cluster_sales,
           a.provinsi,
           a.kabupaten,
           a.kecamatan,
           a.kelurahan,
           a.node_type,
           cast(sum(rev*seized_rate) AS decimal (38,2)) AS breakage,
           a.event_Date

    FROM (
    SELECT a.trx_date,
           a.transaction_id,
           a.sku,
           a.business_id,
           a.rules_id,
           a.msisdn,
           h.brand,
           h.region_hlr,
           a.non_usage_flag,
           CASE WHEN upper(a.allowance_sub_type) like '%DATA%'
                  OR upper(a.allowance_sub_type) like '%UPCC%' THEN 'DATA'
                WHEN upper(a.allowance_sub_type) like '%SMS%' THEN 'SMS'
                WHEN upper(a.allowance_sub_type) like '%VOICE%' THEN 'VOICE'
           ELSE 'NONUSAGE' END AS allowance,
           a.allowance_sub_type,
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
           sum(alloc_tp) AS rev,
           a.event_Date

    FROM (
    SELECT * FROM {table_1}
    WHERE event_Date = {event_date}
      AND status = 'OK00'
    ) a

    LEFT JOIN (
    SELECT * FROM {table_2}
    WHERE event_Date = {event_date}
    ) h
    ON a.event_Date = h.event_Date
      AND a.msisdn = h.msisdn

    LEFT JOIN (
    SELECT a.* FROM (
    SELECT event_Date,
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
           rank() OVER (PARTITION BY event_Date, msisdn ORDER BY lacci_id DESC) AS rnk
    FROM {table_3}
    WHERE event_Date = {event_date}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    ) a
    WHERE rnk = '1'
    ) b
    ON a.event_Date = b.event_Date
      AND a.msisdn = b.msisdn

    LEFT JOIN (
    SELECT a.* FROM (
    SELECT event_Date,
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
           rank() OVER (PARTITION BY event_Date, msisdn ORDER BY lacci_id DESC) AS rnk
    FROM {table_4}
    WHERE event_Date = {month_date}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    ) a
    WHERE rnk = '1'
    ) c
    ON a.msisdn = c.msisdn
    
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,31
    ) a

    INNER JOIN (
    SELECT * FROM {table_5}
    WHERE event_date IN (SELECT MAX(event_date) FROM {table_5})
    ) b1
    ON a.allowance = b1.allowance_type
      AND a.region_sales = b1.region
    WHERE coalesce(lower(brand), 'prepaid') <> 'kartuhalo'

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,31
    """)

    return df

