# -*- coding: utf-8 -*-

# Team: Revenue Analytics Team
# Author: Seoyoung Park
# Maintainer: Seoyoung Park
# Table: 
#   pubg_gi.all_economy_workshop_consumable_item_increased_daily
# Duration: 2 minutes

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
PC_WORKSHOP_START_DATE = "2022-07-13"
CONSOLE_WORKSHOP_START_DATE = "2022-07-21"

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

def get_consumableIncreased_increased(device, target_date):
    consumableItem_increased = load_schema.lobby(spark, device, "live", "ConsumableItemIncreased", target_date, target_date)
    consumableItem_df = consumableItem_increased.groupBy("ItemDescId", "Reason").agg(sum(col("Amount")).alias("amount"))
    consumableItem_df = consumableItem_df.withColumn("date", lit(target_date)).withColumn("device", lit(device)).withColumn("reg_datetime", lit(datetime.now())) \
            .withColumnRenamed("ItemDescId", "itemdescid").withColumnRenamed("Reason", "reason") \
            .select("date", "device", "itemdescid", "reason", "amount", "reg_datetime")
    return consumableItem_df

for device in ["pc", "console"]:
    if device == "console" and target_date < CONSOLE_WORKSHOP_START_DATE:
        slack.send(CHANNEL, "target_date earlier than workshop_start_date in daily_gb_pdu_seoyoung_all_economy_workshop_consumable_item_increased_daily.py, \n device: {} \n target date: {} \n".format(device, target_date))
    else:
        try:
            consumableItem_increased = get_consumableIncreased_increased(device, target_date)
            table_name = "all_economy_workshop_consumable_item_increased_daily"
            delete_for_rebatch(device, target_date, table_name)
            mysql.insert_table(consumableItem_increased, "pubg_gi", table_name)
            slack.send(CHANNEL, "{} for {} date: {} Succeeded".format(table_name, device, target_date), 'good')
        except Exception as e:
            slack.send(CHANNEL, "Error occurred in daily_gb_pdu_seoyoung_all_economy_workshop_consumable_item_increased_daily.py, \n device: {} \n target date: {} \n".format(device, target_date) + str(e), "danger")

