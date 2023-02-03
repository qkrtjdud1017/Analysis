# -*- coding: utf-8 -*-

# Team: Revenue Analytics Team
# Author: Seoyoung park
# Maintainer: Seoyoung Park
# Table: 
#   pubg_gi.all_outgame_inventory_equips_user_cnt_weekly
#   pubg_gi.all_outgame_inventory_equips_cnt_weekly
# Duration: 10 minutes

import sys
import timeit
import traceback
from pyspark.sql import SparkSession, Window

app_name = sys.argv[0]
target_date = sys.argv[1]

spark = SparkSession.builder.appName("{} on {}".format(app_name, target_date)).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pubg_util import load_schema, mysql, notifier
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import json

import boto3
from pubg_util.loader.utils import validate_s3_path
client = boto3.client('s3')
sc = spark.sparkContext
slack = notifier.SlackNotifier()

CHANNEL = "#sypark_notice"

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

def load_inventory(device, target_date, explode=None):
    inventory_bucket = f"s3a://pubg-log-labs/data_mart/economy/inventory_master/{device.lower()}/{target_date}"
     
    # inventorySnapshot seemed to have duplicate entries;
    df = spark.read.parquet(inventory_bucket)
         
    if explode is None:
        return df
    else:
        df.createOrReplaceTempView("temp")
        result = spark.sql("""
        SELECT date
              ,accountid
              --,netid
              ,inline({})
              ,date_updated
          FROM temp
        """.format(explode))
        return result

def get_secret(secret_name, secret_key, region_name = "us-east-1"):

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except:
        slack.send(CHANNEL, traceback.format_exc(), "danger")
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    try:
        return json.loads(secret)[secret_key]
    except:
        slack.send(CHANNEL, traceback.format_exc(), "danger")


def get_brodesc(desc_name, device):
    """
        get static desc files in game-desin-data direcly from git repository
    """
    
    url = f'https://git.projectbro.com/api/v4/projects/258/repository/files/brodesc-builder%2Foutput%2F{desc_name}.json/raw?ref=master'

    token = get_secret("gitlab-token", "read-game-design-data")

    header = {'Content-Type': 'application/json', 'PRIVATE-TOKEN': token}
    res = requests.get(url, headers=header)

    if res.status_code == 200:
        res_text_replaced = res.text\
            .replace("\\u00a0", " ")\
            .replace("\\u2019", " ")
        
        raw_json = list(json.loads(res_text_replaced)['Descs'])
        
        return spark.read.json(sc.parallelize([json.dumps(raw_json)]), multiLine=True)
    else:
        slack.send(CHANNEL, f"Failed to get brodesc : status_code {res.status_code}", "danger")

def delete_for_rebatch(device, target_date, table_name):
    """
        delete existing entries for rebatch
    """
    
    delete_query = """
    DELETE FROM pubg_gi.{table_name} WHERE device = '{device}' AND end_date = '{target_date}';
    """.format(table_name=table_name, device=device, target_date=target_date)
        
    with mysql.get_connector(None) as connector:
        mycursor = connector.cursor()
        mycursor.execute(delete_query)
        connector.commit()

def get_cash_pu(device, target_date):
    one_month_before = (datetime.strptime(target_date, '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m-%d')
    cash_mtx = load_data_mart(device, one_month_before, target_date, "cash_mtx")
    cash_pu = cash_mtx.groupBy("account_id").agg(sum("ingame_revenue").alias("ingame_revenue")).withColumn("join", lit("tmp"))
    
    percentile_tmp = cash_pu.select(expr("percentile(ingame_revenue, 0.20)").alias("percentile20"), \
                                    expr("percentile(ingame_revenue, 0.50)").alias("percentile50"), \
                                    expr("percentile(ingame_revenue, 0.80)").alias("percentile80"), \
                                    expr("percentile(ingame_revenue, 0.95)").alias("percentile95"), \
                                    lit("tmp").alias("join"))
    
    cash_pu = cash_pu.join(percentile_tmp, "join") \
        .withColumn("pu_group", \
                    when(col("ingame_revenue") <= col("percentile20"), lit("<=20")) \
                    .when(col("ingame_revenue") <= col("percentile50"), lit("<=50")) \
                    .when(col("ingame_revenue") <= col("percentile80"), lit("<=80")) \
                    .when(col("ingame_revenue") <= col("percentile95"), lit("<=95")) \
                    .otherwise(lit(">95")))
    return cash_pu, percentile_tmp
    
def get_user_df(device, start_date, end_date, cash_pu):
    def classify_country(country_os, country_ip):
        if country_os != 'CN':
            return country_ip
        else:
            return country_os
    country_type_udf = udf(classify_country, StringType())
    user_master = load_data_mart(device, end_date, end_date, "user_master").withColumn("country_new", country_type_udf("country_os", "country_ip")) \
        .where((col("server_type") == "LIVE") & (col("lastlogindate") >= start_date))
    meta_region = mysql.read_table(spark, 'metainfo', 'meta_bi_regions')
    user_master = user_master.join(meta_region, user_master.country_new == meta_region.country_code_iso2, "left").select("accountid", "pubg_region")
    
    user = user_master.join(cash_pu.withColumnRenamed("account_id", "accountid"), "accountid", "left") \
        .withColumn("pu_group", coalesce(col("pu_group"), lit("non_pu"))) \
        .select("accountid", "pubg_region", "pu_group").distinct()
    return user

def get_inventory_df(device, start_date, end_date):
    inventory = load_inventory(device, end_date, "equips").where(col("date_updated") >= start_date)
    
    partdesc = get_brodesc("partdesc", device)
    weapondesc = get_brodesc("weapondesc", device)

    inventory_df = inventory.withColumn("partdescid", concat(lit("partdesc."), col("part"))) \
        .join(partdesc.withColumnRenamed("Id", "partdescid"), "partdescid") \
        .join(weapondesc.select("partdescid", "name"), "partdescid", "left")
    return inventory_df

def get_final_df(device, start_date, end_date):    
    cash_pu, percentile_tmp = get_cash_pu(device, end_date)
    user = get_user_df(device, start_date, end_date, cash_pu)
    inventory_df = get_inventory_df(device, start_date, end_date)
    inventory_df = inventory_df.join(user, "accountid").withColumn("pubg_region", coalesce(col("pubg_region"), lit("Undefined")))
    
    user_cnt_by_part = inventory_df.groupBy("pubg_region", "pu_group", "StoreCategory", "part").agg(countDistinct("accountid").alias("user_cnt"))
    user_cnt_by_category = inventory_df.groupBy("pubg_region", "pu_group", "StoreCategory").agg(countDistinct("accountid").alias("user_cnt")).withColumn("part", lit("total"))
    user_cnt_by_group = inventory_df.groupBy("pubg_region", "pu_group").agg(countDistinct("accountid").alias("user_cnt")).withColumn("StoreCategory", lit("total")).withColumn("part", lit("total"))
    item_equip_user_cnt = user_cnt_by_part.unionByName(user_cnt_by_category).unionByName(user_cnt_by_group) \
        .withColumn("join", lit("tmp")).join(percentile_tmp, "join") \
        .withColumn("start_date", lit(start_date)).withColumn("end_date", lit(end_date)).withColumn("device", lit(device)) \
        .select("start_date", "end_date", "device", "pubg_region", "pu_group", col("StoreCategory").alias("category"), "part", "user_cnt", "percentile20", "percentile50", "percentile80", "percentile95")
    
    item_equip_cnt = inventory_df.groupBy("pubg_region", "pu_group", "StoreCategory", "StoreSubcategory", "part", "name", "itemid").agg(count("*").alias("equip_cnt"))
    window_spec = Window.partitionBy("pubg_region", "pu_group", "StoreCategory").orderBy(col("equip_cnt").desc())
    item_equip_cnt_rank = item_equip_cnt.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") <= 50) \
        .withColumn("itemid", concat(lit("itemdesc."), col("itemid"))).withColumn("start_date", lit(start_date)).withColumn("end_date", lit(end_date)).withColumn("device", lit(device)) \
        .select("start_date", "end_date", "device", "pubg_region", "pu_group", col("StoreCategory").alias("category"), col("StoreSubcategory").alias("sub_category"), "part", col("name").alias("weapon_ingame_name"), "itemid", "equip_cnt")
    return item_equip_user_cnt, item_equip_cnt_rank

batch_weekday = {
    "pc": 6,
    "console": 6
}

for device in ["pc", "console"]:
    try:
        if datetime.strptime(target_date, '%Y-%m-%d').isoweekday() == batch_weekday[device]:
            start_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
            end_date = target_date
            item_equip_user_cnt, item_equip_cnt = get_final_df(device, start_date, end_date)
            user_cnt_table_name = "all_outgame_inventory_equips_user_cnt_weekly"
            equip_cnt_table_name = "all_outgame_inventory_equips_cnt_weekly"
            
            delete_for_rebatch(device, end_date, user_cnt_table_name)
            mysql.insert_table(item_equip_user_cnt, "pubg_gi", user_cnt_table_name)
            slack.send(CHANNEL, "{} \n device: {} \n target_date: {} Succeeded".format(user_cnt_table_name, device, target_date), 'good')
            delete_for_rebatch(device, end_date, equip_cnt_table_name)
            mysql.insert_table(item_equip_cnt, "pubg_gi", equip_cnt_table_name)
            slack.send(CHANNEL, "{} \n device: {} \n target_date: {} Succeeded".format(equip_cnt_table_name, device, target_date), 'good')
        else:
            slack.send(CHANNEL, "No update on {}, {} from daily_gb_pud_seoyoung_all_outgame_inventory_equips_weekly.py".format(target_date, device))
    except Exception as e:
        slack.send(CHANNEL, "Error occurred in daily_gb_pdu_seoyoung_all_outgame_inventory_equips_weekly.py, \n device: {} \n target_date: {}".format(device, target_date) + "\n" + str(e), "danger")
