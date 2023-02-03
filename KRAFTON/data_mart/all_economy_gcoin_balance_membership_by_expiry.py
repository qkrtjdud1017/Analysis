#  -*- coding: utf-8 -*-
#  Team: Revenue Analytics
#  Author: Seoyoung Park
#  Maintainer: Seoyoung Park
#  Table: pubg_gi.all_economy_gcoin_balance_membership_by_expiry

import sys

from pyspark.sql import SparkSession
app_name = sys.argv[0]
target_date = sys.argv[1]

spark = SparkSession.builder.appName("{} on {}".format(app_name, target_date)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

import timeit
import traceback
from functools import reduce
from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql import Window, SQLContext, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import Bucketizer
import boto3
from pubg_util.loader.utils import validate_s3_path
client = boto3.client('s3')

from pubg_util import mysql, load_schema, notifier

slack = notifier.SlackNotifier()
sc = spark.sparkContext

except_msg_list = []

ACTIVE_USER_TYPE = {
    'all': None,
    'w-01': 6,
    'w-02': 13,
    'w-04': 27,
    'w-08': 55,
    'w-12': 83
}

F2P_START_DATE = "2022-01-12"
CHANNEL = '#sypark_notice'

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

def extract_user_pubg_region(device, target_date):        
    def classify_country(country_os, country_ip):
        if country_os != "CN":
            return country_ip
        else:
            return country_os
    country_type_udf = udf(classify_country, StringType())
    meta_region = mysql.read_table(spark, "metainfo", "meta_bi_regions")
    
    user = load_data_mart(device, target_date, target_date, table_name="user_master").where("server_type = 'LIVE' and platform not like '%NULL%'")
    user = user.withColumn("country_new", country_type_udf("country_os", "country_ip")).withColumnRenamed("accountid", "account_id")
    user = user.join(meta_region, user.country_new == meta_region.country_code_iso2, "left")
    return user
    
    
def get_table_by_period(device, target_date, active_user_type, period):
    user = extract_user_pubg_region(device, target_date)
    membership = load_membership("pubg", device, target_date, target_date).select("account_id", "membership").distinct()
    df_user_country = user.join(membership, "account_id", "left").drop("date", "platform").withColumn("membership", coalesce(col("membership"), lit("undefined")))
    df_user_country.createOrReplaceTempView("user_tmp")
    df_user_country = spark.sql("""
            SELECT *
                ,ROW_NUMBER() over(PARTITION BY account_id ORDER BY firstlogindate) as rn
            FROM user_tmp
        """) \
        .where('rn = 1').drop('rn')

    df_gcoin_master = load_data_mart(device, target_date, target_date, table_name='gcoin_master')

    if period:
        df_user_country = df_user_country \
            .where(f'days_since_lastlogin <= {period}')
        df_joined = df_gcoin_master \
            .join(df_user_country, 'account_id', how='left') \
            .where('pubg_region is not null')
    else:
        df_joined = df_gcoin_master \
            .join(df_user_country, 'account_id', how='left')

    df_joined = df_joined \
        .withColumn('active_user_type', lit(active_user_type)) \
        .withColumn('device', coalesce(col('device'), upper(lit(device)))) \
        .withColumn('pubg_region', coalesce(col('pubg_region'), lit('Undefined'))) \
        .selectExpr('date', 'account_id', 'device', 'platform', 'pubg_region', 'membership', 'active_user_type', 'inline(gcoin_by_expiry)')

    df_joined_diff = df_joined \
        .withColumn('date_diff', datediff('expiry_date', 'date')-1) \
        .where('date_diff >= 0')

    split_list = [0, 1, 7, 30, 90, 180, float('inf')]
    label_list = ['0', '[1-7)', '[7-30)', '[30-90)', '[90-180)', '[180-inf)']

    bucketizer = Bucketizer(splits=split_list, inputCol='date_diff', outputCol='split')
    df_with_split = bucketizer.transform(df_joined_diff)

    sc = spark.sparkContext
    df_mapping_label = sc.parallelize([(float(i), j) for i, j in enumerate(label_list)]).toDF(['split', 'days_to_expiry'])

    df_with_label = df_with_split.join(broadcast(df_mapping_label), 'split', how='left')

    return df_with_label


def get_balance_table(device, target_date):
    df_list = []
    for active_user_type, period in ACTIVE_USER_TYPE.items():
        df_list.append(get_table_by_period(device, target_date, active_user_type, period))

    df_by_period = reduce(DataFrame.union, df_list)

    df_topup_user = df_by_period \
        .where('daily_free_topup != 0 OR daily_paid_topup != 0') \
        .groupBy('date', 'device', 'platform', 'pubg_region', 'membership', 'active_user_type', 'days_to_expiry') \
        .agg(countDistinct('account_id').alias('daily_gcoin_topup_user'))

    df_use_user = df_by_period \
        .where('daily_free_use != 0 OR daily_paid_use != 0') \
        .groupBy('date', 'device', 'platform', 'pubg_region', 'membership', 'active_user_type', 'days_to_expiry') \
        .agg(countDistinct('account_id').alias('daily_gcoin_use_user'))

    df_all_user = df_by_period \
        .groupBy('date', 'device', 'platform', 'pubg_region', 'membership', 'active_user_type', 'days_to_expiry') \
        .agg(countDistinct('account_id').alias('user_count'),
             sum('total_free_topup').alias('cum_free_gcoin_topup'),
             sum('total_paid_topup').alias('cum_paid_gcoin_topup'),
             sum('total_free_topup_refunds').alias('cum_free_gcoin_topup_refunds'),
             sum('total_paid_topup_refunds').alias('cum_paid_gcoin_topup_refunds'),
             sum('total_free_use').alias('cum_free_gcoin_used'),
             sum('total_paid_use').alias('cum_paid_gcoin_used'),
             sum('total_free_returns').alias('cum_free_gcoin_returns'),
             sum('total_paid_returns').alias('cum_paid_gcoin_returns'),
             sum('free_balance').alias('free_gcoin_balance'),
             sum('paid_balance').alias('paid_gcoin_balance'))

    df_balance_result = df_all_user \
        .join(df_topup_user, on=['date', 'device', 'platform', 'pubg_region', 'membership', 'active_user_type', 'days_to_expiry'], how='left') \
        .join(df_use_user, on=['date', 'device', 'platform', 'pubg_region', 'membership', 'active_user_type', 'days_to_expiry'], how='left') \
        .withColumn('daily_gcoin_topup_user', coalesce(col('daily_gcoin_topup_user'), lit(0))) \
        .withColumn('daily_gcoin_use_user', coalesce(col('daily_gcoin_use_user'), lit(0)))

    return df_balance_result


def upload_table(device, target_date):
    try:
        timer_start = timeit.default_timer()

        table_name = 'all_economy_gcoin_balance_membership_by_expiry'
        df_balance_result = get_balance_table(device, target_date).withColumn("reg_datetime", lit(datetime.now()))
        if target_date > F2P_START_DATE:
            delete_for_rebatch(device, target_date, table_name)
        mysql.insert_table(df_balance_result, 'pubg_gi', table_name)
        result_status = True
    except:
        global except_msg_list
        except_msg = ("[daily_gb_pdu_seoyoung_gcoin_balance_membership_by_expiry_daily.py]\n"
                      "{device}/{target_date} {table_name} upload failed for \n{traceback}")\
            .format(
                device=device,
                target_date=target_date,
                table_name=table_name,
                traceback=traceback.format_exc()
            )
        except_msg_list.append(except_msg)
        result_status = False
    finally:
        elapsed = "{0:.2f}".format(timeit.default_timer() - timer_start)

    return result_status, elapsed


def run_batch(target_date):
    """
        run batch script and send batch result summary to slack notice channel
    """
    global except_msg_list

    try:
        total_time_start = timeit.default_timer()

        device_list = ['pc', 'console']
        result_msg_list = []
        for device in device_list:
            result_status, elapsed = upload_table(device, target_date)
            status_mark = ':heavy_check_mark:' if result_status else ':x:'
            msg_body = f"- {device}: ({elapsed} secs) {status_mark}"
            result_msg_list.append(msg_body)

        if except_msg_list:
            if len(except_msg_list) == len(device_list):
                batch_status = 'danger'
            else:
                batch_status = 'warning'
            raise
        else:
            batch_status = 'good'
    except:
        slack.send(CHANNEL, '\n\n'.join(except_msg_list), 'danger')
    finally:
        total_elapsed = "{0:.2f}".format(timeit.default_timer() - total_time_start)
        body =  "[Batch Result] GCoin Balance by Expiry {} ({} secs)".format(target_date, total_elapsed)
        result_msg_list.append(body)
        slack.send(CHANNEL, '\n'.join(list(reversed(result_msg_list))), batch_status)

run_batch(target_date)
