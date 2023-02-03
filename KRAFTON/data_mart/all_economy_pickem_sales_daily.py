#-*- coding: utf-8 -*-
# Team: Revenue analaytics
# Author: Seoyoung Park
# goblin : pubg_gi.all_economy_pickem_sales_daily
# Duration: 5 minutes

import sys
from pyspark.sql import SparkSession

app_name = sys.argv[0]
target_date = sys.argv[1]

spark = SparkSession \
    .builder \
    .appName("{} on {}".format(app_name, target_date)) \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from datetime import datetime, timedelta
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pubg_util import mysql
import boto3
from pubg_util.loader.utils import validate_s3_path
client = boto3.client('s3')

from pubg_util import mysql, load_schema, notifier

slack = notifier.SlackNotifier()

def load_data_mart(device, start_date, end_date, table_name="cash_mtx", join_with_meta=True):
    """
        load economy data_mart with its corresponding meta
    """
    bucket = {
        "cash_mtx" : "s3a://pubg-log-labs/data_mart/economy_v2/cash_mtx/{device}/{target_date}",
        "pu_master" : "s3a://pubg-log-labs/data_mart/economy_v2/pu_master/{device}/{target_date}",
        "gcoin_use" : "s3a://pubg-log-labs/data_mart/economy_v3/gcoin_use/{device}/{target_date}",
        "gcoin_topup" : "s3a://pubg-log-labs/data_mart/economy_v3/gcoin_topup/{device}/{target_date}",
        "gcoin_master" : "s3a://pubg-log-labs/data_mart/economy_v3/gcoin_master/{device}/{target_date}",
        "user_master" : "s3a://pubg-log-labs/data_mart/user_master_ver2/{target_date}/{device}"
    }
    meta_name = {
        "cash_mtx" : "meta_cash_mtx",
        "gcoin_use" : "meta_vc_sales_items"
    }
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    date_list = [(start_date + timedelta(d)).strftime("%Y-%m-%d") for d in range((end_date-start_date).days+1)]
    device_str = device.upper() if table_name=='user_master' else device
    path_list = [bucket[table_name].format(device=device_str, target_date=target_date) for target_date in date_list]
    # load_only valid paths
    valid_path_list = list(filter(lambda path: validate_s3_path(client, path), path_list))
    if len(valid_path_list) == 0:
        message = "no {} data in given period {}-{}".format(table_name, start_date, end_date)
        raise Exception(message)
        return None
    if device == 'pc' and table_name == 'cash_mtx':
        df_raw = spark.read.option("mergeSchema", "true").parquet(*valid_path_list)
    elif table_name == 'gcoin_use':
        df_raw = spark.read.option("mergeSchema", "true").parquet(*valid_path_list)
        df_raw_no_salesid = df_raw.where('date >= "2021-07-28" and sales_id is null').toPandas()
        if not df_raw_no_salesid.empty:
            message = 'GCOIN USE data exist: sales_id is null\n{}'.format(df_raw_no_salesid.to_string(index=False))
            raise Exception(message)
    else:
        df_raw = spark.read.parquet(*valid_path_list)
 
    missing_dates = list(set(path_list) - set(valid_path_list))
    if len(missing_dates) > 0:
        message = "no data exists in\n" + "\n".join(missing_dates)
        raise Exception(message)
    if join_with_meta is False or table_name not in ('cash_mtx', 'gcoin_use'):
        # if it's not cash_mtx or gcoin_use, return raw_df as there are no meta to join with
        return df_raw
    else:
        # load meta_cash_mtx
        meta_raw = (
            mysql.read_table(spark, 'metainfo', meta_name[table_name])
            .withColumnRenamed("platform", "platform_")
            .withColumnRenamed("product_id", "product_id_")
            .withColumnRenamed("price", "price_")
          )
        if table_name == 'gcoin_use':
            meta_raw = meta_raw.where("currency = 'gcoin'")
            df_raw = df_raw \
                .withColumn('is_salesid_exist',
                            when(col('sales_id').isNull(), lit(0)).otherwise(lit(1))) \
                .withColumn('sales_id_',
                            when(col('sales_id').startswith('salesitemdesc'),
                            split('sales_id', 'desc.')[1]).otherwise(col('sales_id'))) \
                .drop('sales_id')
        if device == 'console' and table_name == 'cash_mtx':
            # join condition
            condition = [
                df_raw.platform == meta_raw.platform_,
                df_raw.product_id == meta_raw.product_id_
            ]
            # join with meta
            # is_paid is hard coded for products that were provided to Stadia Pro users for free during promo period
            df = (
                df_raw
                .join(meta_raw, condition, 'left')
                .withColumn("transaction_id", lit(None).cast(StringType()))
                .withColumn("status", lit('Succeeded').cast(StringType()))
                .withColumnRenamed("time", "time_")
                .withColumn("time", coalesce(col("time_"), concat(col("date"), lit("T00:00:00"))))
                .drop('platform_', 'product_id_', 'time_')
                .withColumn('is_paid',
                    ~(
                        (col('product_id').isin(['PUBGPIONEREDITION', 'PUBGBDLDSLEISKIN']))
                    )
                ).where('is_paid')
            )
        else:
            meta_duplicate_count = (
                meta_raw
                .groupBy('product_id_', 'platform_')
                .agg(count(lit(1)).alias('duplications'))
            )
            window_spec = (
                Window
                .partitionBy('platform_', 'product_id_')
                .orderBy('start_time')
            )
            meta = (
                meta_raw.alias("meta_raw")
                .join(
                    meta_duplicate_count.alias("meta_duplicate_count"),
                    on=['product_id_', 'platform_'],
                    how='left')
                .select("meta_raw.*", "meta_duplicate_count.duplications")
                .withColumn(
                    "next_start_time",
                    coalesce(lead("start_time").over(window_spec), lit('2038-01-19 00:00:00'))
                )
            )
            if table_name == 'gcoin_use':
                condition = (
                (
                    (df_raw.is_salesid_exist == 1)
                    & (
                        (df_raw.platform == meta.platform_)
                        & (df_raw.sales_id_ == meta.sales_id)
                    )
                )
                | ( (df_raw.is_salesid_exist == 0)
                    & (df_raw.platform == meta.platform_)
                    & (df_raw.product_id == meta.product_id_)
                    & (
                        (
                            (df_raw.time >= meta.start_time)
                            & (df_raw.time < meta.next_start_time)
                            & (meta.duplications >= 2)
                        )
                            | (meta.duplications == 1)
                    )
                ))
                # join with meta
                df = (
                    df_raw
                    .join(meta, condition, 'left')
                    .drop('platform_', 'product_id_', 'next_start_time', "duplications")
                )
            else:
                condition = (
                    (df_raw.platform == meta.platform_)
                    & (df_raw.product_id == meta.product_id_)
                    & (
                        (
                            (df_raw.time >= meta.start_time)
                            & (df_raw.time < meta.next_start_time)
                            & (meta.duplications >= 2)
                        )
                            | (meta.duplications == 1)
                    )
                )
                # join with meta
                df = (
                    df_raw.drop('sales_id')
                    .join(meta, condition, 'left')
                    .drop('platform_', 'product_id_', 'next_start_time', "duplications")
                )
        if table_name == 'cash_mtx':
            df = df.withColumn("ingame_revenue", col("unit_sold") * col("ingame_price"))
 
        return df

def delete_for_rebatch(device, target_date, table_name):
    """
        delete existing entries for rebatch
    """

    delete_from_query = """
    DELETE FROM pubg_gi.{table_name} WHERE device = '{device}' AND date = '{target_date}';
    """.format(table_name=table_name, device=device, target_date=target_date)
    
    with mysql.get_connector(None) as connector:
        mycursor = connector.cursor()
        mycursor.execute(delete_from_query)
        connector.commit()

def get_pickem_info(device, target_date):
    meta_pickem = mysql.read_table(spark, 'metainfo', 'meta_esports_pickem').where("device = '{}'".format(device))
    
    pickem_info = ('None', 'None', 'None')
    
    for i in meta_pickem.collect():
        if (target_date >= i[2] and target_date <= i[3]):
            pickem_name = str(i[0])
            pickem_event_name = str(i[8])
            sales_start_date = i[2]
            pickem_info = (pickem_name, pickem_event_name, sales_start_date)
            break

    return pickem_info

def make_pickem_sales_df(device, target_date):
    (pickem_name, pickem_event_name, sales_start_date) = get_pickem_info(device, target_date)

    cSchema = StructType([StructField("date", StringType())\
                          ,StructField("device", StringType())\
                          ,StructField("pickem_name", StringType())\
                          ,StructField("cum_au", IntegerType())\
                          ,StructField("cum_pu", IntegerType())\
                          ,StructField("cum_paid_pu", IntegerType())\
                          ,StructField("cum_free_gcoin", IntegerType())\
                          ,StructField("cum_paid_gcoin", IntegerType())\
                          ,StructField("daily_npu", IntegerType())])
    
    if pickem_name != 'None':
        df_au = load_data_mart(device, target_date, target_date, "user_master")
        df_cum_gcoin = load_data_mart(device, sales_start_date, target_date, "gcoin_use").where(col("event_name").like("%{}%".format(pickem_event_name))) \
                .withColumn("paid_account_id", when(col("paid_use") > 0, col("account_id")).otherwise(None))
        df_gcoin_master = load_data_mart(device, target_date, target_date, "gcoin_master").where(col("first_use_date") == target_date)
        df_daily_gcoin = load_data_mart(device, target_date, target_date, "gcoin_use").where(col("event_name").like("%{}%".format(pickem_event_name)))

        cum_au = df_au.where((col("lastlogindate") >= sales_start_date) & (col("server_type") == "LIVE")).select(countDistinct("accountid")).collect()[0][0]
        daily_npu = df_gcoin_master.join(df_daily_gcoin, "account_id").select(countDistinct("account_id")).collect()[0][0]
        df_summary = df_cum_gcoin.select(countDistinct("account_id").alias("cum_pu"), countDistinct("paid_account_id").alias("cum_paid_pu"), sum("free_use").alias("cum_free_gcoin"), sum("paid_use").alias("cum_paid_gcoin"))\
            .withColumn("date", lit(target_date)).withColumn("device", lit(device)).withColumn("pickem_name", lit(pickem_name)).withColumn("cum_au", lit(cum_au)).withColumn("daily_npu", lit(daily_npu)) \
            .select("date", "device", "pickem_name", "cum_au", "cum_pu", "cum_paid_pu", "cum_free_gcoin", "cum_paid_gcoin", "daily_npu")
    else:
        df_summary = spark.createDataFrame([], schema=cSchema)
    return df_summary

try:
    pickem_pc = make_pickem_sales_df('pc', target_date)
    pickem_console = make_pickem_sales_df('console', target_date)

    df_final = pickem_pc.unionByName(pickem_console)
    db_name = 'pubg_gi'
    table_name = 'all_economy_pickem_sales_daily'

    delete_for_rebatch("pc", target_date, table_name)
    delete_for_rebatch("console", target_date, table_name)
    mysql.insert_table(df_final, db_name, table_name)

except Exception as e:
    slack.send('#sypark_notice', "{} daily_gb_pdu_seoyoung_pickem_sales_bi.py\n".format(target_date) + str(e), 'danger')

