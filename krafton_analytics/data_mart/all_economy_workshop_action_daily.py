# -*- coding: utf-8 -*-

# Team: Revenue Analytics Team
# Author: Seoyoung Park
# Maintainer: Seoyoung Park
# Table: 
#   pubg_gi.all_economy_workshop_action_daily
# Duration: 3 minutes

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

PC_WORKSHOP_START_DATE = "2022-07-13"
CONSOLE_WORKSHOP_START_DATE = "2022-07-21"
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

def get_workshop_action(device, target_date):
    open = load_schema.lobby(spark, device, "live", "WorkshopCrateOpened", target_date, target_date)
    craft = load_schema.lobby(spark, device, "live", "WorkshopCrafted", target_date, target_date) \
            .withColumn("item", when(col("ItemDescId").like("%imprint%"), "imprint").when(col("ItemDescId").like("%voucher%"), "voucher").otherwise("else"))
    disassemble = load_schema.lobby(spark, device, "live", "WorkshopDisassembled", target_date, target_date).withColumn("action", lit("disassemble"))
    repurpose = load_schema.lobby(spark, device, "live", "WorkshopRepurposed", target_date, target_date).withColumn("action", lit("repurpose"))
    
    open_by_item = open.groupBy("ItemDescId").agg(sum(col("OpenAmount")).alias("count"), countDistinct("AccountId").alias("user_count")).withColumnRenamed("ItemDescId", "item")
    open_total = open.select(sum(col("OpenAmount")).alias("count"), countDistinct("AccountId").alias("user_count")).withColumn("item", lit("total"))
    open_df = open_by_item.unionByName(open_total).withColumn("action", lit("open"))
    
    craft_by_item = craft.groupBy("item").agg(count("*").alias("count"), countDistinct("AccountId").alias("user_count"))
    craft_total = craft.select(count("*").alias("count"), countDistinct("AccountId").alias("user_count")).withColumn("item", lit("total"))
    craft_df = craft_by_item.unionByName(craft_total).withColumn("action", lit("craft"))
    
    disassemble_df = disassemble.select(count("*").alias("count"), countDistinct("AccountId").alias("user_count")).withColumn("item", lit("total")).withColumn("action", lit("disassemble"))
    repurpose_df = repurpose.select(count("*").alias("count"), countDistinct("AccountId").alias("user_count")).withColumn("item", lit("total")).withColumn("action", lit("repurpose"))
    df = open_df.unionByName(craft_df).unionByName(disassemble_df).unionByName(repurpose_df) \
        .withColumn("device", lit(device)).withColumn("date", lit(target_date)).withColumn("reg_datetime", lit(datetime.now())) \
        .select("date", "device", "action", "item", "count", "user_count", "reg_datetime")
    return df

for device in ["pc", "console"]:
    if device == "console" and target_date < CONSOLE_WORKSHOP_START_DATE:
        slack.send(CHANNEL, "target_date earlier than workshop_start_date in daily_gb_pdu_seoyoung_all_economy_workshop_action_daily.py, \n device: {} \n target date: {} \n".format(device, target_date))
    else:
        try:
            workshop_action = get_workshop_action(device, target_date)
            table_name = "all_economy_workshop_action_daily"
            delete_for_rebatch(device, target_date, table_name)
            mysql.insert_table(workshop_action, "pubg_gi", table_name)
            slack.send(CHANNEL, "{} for {} date: {} Succeeded".format(table_name, device, target_date), 'good')
        except Exception as e:
            slack.send(CHANNEL, "Error occurred in daily_gb_pdu_seoyoung_all_economy_workshop_action_daily.py, \n device: {} \n target date: {} \n".format(device, target_date) + str(e), "danger")

