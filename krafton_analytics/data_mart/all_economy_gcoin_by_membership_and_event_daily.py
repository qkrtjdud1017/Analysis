# -*- coding: utf-8 -*-

# Team: Revenue Analytics Team
# Author: Seoyoung Park
# Maintainer: Seoyoung Park
# Table: all_economy_gcoin_by_membership_and_event_daily
# Duration: 5 minutes

import sys

from pyspark.sql import SparkSession
app_name = sys.argv[0]
target_date = sys.argv[1]

spark = SparkSession.builder.appName("{} on {}".format(app_name, target_date)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql import Window, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from pubg_util.loader.utils import validate_s3_path
client = boto3.client('s3')

from pubg_util import mysql, load_schema, notifier

slack = notifier.SlackNotifier()
sc = spark.sparkContext

CHANNEL = "#sypark_notice"
F2P_START_DATE = "2022-01-12"

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
        print(message)
        return None
    if device == 'pc' and table_name == 'cash_mtx':
        df_raw = spark.read.option("mergeSchema", "true").parquet(*valid_path_list)
    elif table_name == 'gcoin_use':
        df_raw = spark.read.option("mergeSchema", "true").parquet(*valid_path_list)
        df_raw_no_salesid = df_raw.where('date >= "2021-07-28" and sales_id is null').toPandas()
        if not df_raw_no_salesid.empty:
            message = 'GCOIN USE data exist: sales_id is null\n{}'.format(df_raw_no_salesid.to_string(index=False))
            print(message)
    else:
        df_raw = spark.read.parquet(*valid_path_list)
 
    missing_dates = list(set(path_list) - set(valid_path_list))
    if len(missing_dates) > 0:
        message = "no data exists in\n" + "\n".join(missing_dates)
        print(message)
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

def load_membership(game, device, start_date, end_date) :
    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    startdate = datetime.strptime(start_date, '%Y-%m-%d')
    enddate = datetime.strptime(end_date, '%Y-%m-%d')
    date_list = [(startdate + timedelta(d)).strftime("%Y-%m-%d") for d in range((enddate-startdate).days+1)]   
    path_list = ["s3a://pubg-log-labs/data_mart/{game}/membership/{device}/{target_date}" \
            .format(game = game, device = device, target_date = target_date) for target_date in date_list]
    true_path = []
    if device == '*' :
        all_device = ['pc', 'console', 'lpc']
        for devices in all_device :
            for path in path_list :
                true_path.append(path.replace("*",devices))
    else :
        true_path = path_list
    read_path = []
    for real_path in true_path : ###실제 있는 경로리스트 추출
        uri = jvm.java.net.URI(real_path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        if fs.exists(jvm.org.apache.hadoop.fs.Path(real_path)) :
            read_path.append(str(real_path))       
    try : #####
        df = spark.read.parquet(*read_path)
    except : ##### 경로가 없으면 임의로 만들어줌
        df = spark.read.parquet("s3a://pubg-log-labs/data_mart/pubg_test/membership/pc/2022-01-04") \
            .where("date = '2099-01-01'")
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

# device: ["pc", "console"]
def gcoin_used_by_event(device, target_date):
    def classify_country(country_os, country_ip):
        if country_os != "CN":
            return country_ip
        else:
            return country_os
    country_type_udf = udf(classify_country, StringType())
    meta_region = mysql.read_table(spark, "metainfo", "meta_bi_regions")
        
    user = load_data_mart(device, target_date, target_date, table_name="user_master").where((col("server_type") == "LIVE") & (~col("platform").like("%NULL%")))
    user = user.withColumn("country_new", country_type_udf("country_os", "country_ip")).withColumnRenamed("accountid", "account_id")
    user = user.join(meta_region, user.country_new == meta_region.country_code_iso2, "left")
    membership = load_membership("pubg", device, target_date, target_date).select("account_id", "membership").distinct()
    user = user.join(membership, "account_id", "left").withColumn("membership", coalesce(col("membership"), lit("undefined")))
    user.createOrReplaceTempView("user_tmp")
    user = spark.sql("""
            SELECT *
                ,ROW_NUMBER() over(PARTITION BY account_id ORDER BY firstlogindate) as rn
            FROM user_tmp
        """) \
        .where('rn = 1').drop('rn')
            
    gcoin_use = load_data_mart(device, target_date, target_date, table_name="gcoin_use")
    gcoin_use_by_user = gcoin_use.groupBy(["account_id", "sub_category", "event_type", "event_name"]).agg(sum("free_use").alias("free_gcoin_used"), sum("paid_use").alias("paid_gcoin_used"))
    gcoin_use_by_user = gcoin_use_by_user.join(user, "account_id", "left")
    gcoin_df = gcoin_use_by_user.groupBy("device", "platform", "pubg_region", "membership", "sub_category", "event_type", "event_name").agg(countDistinct("account_id").alias("pu"), sum("free_gcoin_used").alias("free_gcoin_used"), sum("paid_gcoin_used").alias("paid_gcoin_used")).withColumn("date", lit(target_date))

    return gcoin_df

try:
    pc_gcoin = gcoin_used_by_event("pc", target_date)
    console_gcoin = gcoin_used_by_event("console", target_date)
    
    all_gcoin = pc_gcoin.unionByName(console_gcoin).withColumn("reg_datetime", lit(datetime.now())).select("date", "device", "platform", "pubg_region", "membership", "sub_category", "event_type", "event_name", "pu", "free_gcoin_used", "paid_gcoin_used", "reg_datetime")
    
    db_name = "pubg_gi"
    table_name = "all_economy_gcoin_by_membership_and_event_daily"
    
    delete_for_rebatch("pc", target_date, table_name)
    delete_for_rebatch("console", target_date, table_name)
    mysql.insert_table(all_gcoin, db_name, table_name)

except Exception as e:
    slack.send(CHANNEL, "Error occurred in daily_gb_pdu_seoyoung_all_economy_gcoin_by_membership_and_event_daily.py, \n target date: {} \n".format(target_date) + str(e), "danger")
