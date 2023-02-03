# -*- coding: utf-8 -*-

# Team: Revenue Analytics Team
# Author: Seoyoung Park
# Maintainer: Seoyoung Park
# Table: 
#   pubg_gi.all_economy_esportspoint_used_by_product_daily

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

def delete_for_rebatch(device, target_date, table_name):
    """
        delete existing entries for rebatch
    """

    delete_from_query = """
    DELETE FROM pubg_gi.{table_name} WHERE device = '{device}' and date = '{target_date}';
    """.format(table_name=table_name, device=device, target_date=target_date)
    
    with mysql.get_connector(None) as connector:
        mycursor = connector.cursor()
        mycursor.execute(delete_from_query)
        connector.commit()

def get_pickem_info(device, target_date):
    meta_pickem = mysql.read_table(spark, 'metainfo', 'meta_esports_pickem').where("device = '{}'".format(device))
    
    pickem_info = ('None', 'None', 'None')
    
    for i in meta_pickem.collect():
        if (target_date >= i[6] and target_date <= i[7]):
            pickem_name = str(i[0])
            esports_tab_open = i[6]
            esports_tab_close = i[7]
            pickem_info = (pickem_name, esports_tab_open, esports_tab_close)
            break
    return pickem_info

def get_ep_sales(device, target_date):
    pickem_name, esports_tab_open, esports_tab_close = get_pickem_info(device, target_date)
    if pickem_name != "None":
        ep_purchase = load_schema.lobby(spark, device, 'live', 'PurchaseResult', target_date, target_date).where(col("Currency") == "esportspoint")
        ep_purchase_by_product = ep_purchase.groupBy("AnalyticEventName", "ProductId").agg(sum("Amount").alias("unit_sold"), sum(col("LocalPrice") * col("Amount")).alias("points_used")) \
            .withColumnRenamed("AnalyticEventName", "event_name").withColumnRenamed("ProductId", "product_id") \
            .withColumn("device", lit(device)).withColumn("date", lit(target_date))
        ep_items = mysql.read_table(spark, "metainfo", "meta_vc_sales_items").where(col("currency") == "esportspoint").select("product_id", "product_name").distinct().withColumn("product_name", when(col("product_name").isNull(), col("product_id")).otherwise(col("product_name")))
        ep_purchase_by_product = ep_purchase_by_product.join(ep_items, "product_id", "left").select("date", "device", "event_name", "product_id", "product_name", "unit_sold", "points_used")
    else:
        cSchema = StructType([StructField("date", StringType())\
                          ,StructField("device", StringType())\
                          ,StructField("event_name", StringType())\
                          ,StructField("product_id", StringType())\
                          ,StructField("product_name", StringType())\
                          ,StructField("unit_sold", IntegerType())\
                          ,StructField("points_used", IntegerType())])
        ep_purchase_by_product = spark.createDataFrame([], schema=cSchema)
    return ep_purchase_by_product

for device in ["pc"]:
    try:
        df = get_ep_sales(device, target_date)
        table_name = "all_economy_esportspoint_used_by_product_daily"
        delete_for_rebatch(device, target_date, table_name)
        mysql.insert_table(df, "pubg_gi", table_name)
        slack.send(CHANNEL, "{} for {} date: {} Succeeded".format(table_name, device, target_date), 'good')
    except Exception as e:
        slack.send(CHANNEL, "Error occurred in daily_gb_pdu_seoyoung_all_economy_esportspoint_used_by_product_daily.py, \n device: {} \n target date: {} \n".format(device, target_date) + str(e), "danger")

