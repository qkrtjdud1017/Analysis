# -*- coding: utf-8 -*-

# Team: Revenue Analytics Team
# Author: Seoyoung Park
# Maintainer: Seoyoung Park
# Table: 
#   pubg_gi.all_economy_currency_increased_daily
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

def get_currency_increased(device, target_date):
    currency_increased = load_schema.lobby(spark, device, "live", "CurrencyIncreased", target_date, target_date).fillna(value="", subset=["ReasonDetail"])
    currency_df = currency_increased.groupBy("Currency", "Reason", "ReasonDetail").agg(sum("Amount").alias("amount")) \
                .withColumn("date", lit(target_date)).withColumn("device", lit(device)).withColumn("reg_datetime", lit(datetime.now())) \
                .withColumnRenamed("Currency", "currency").withColumnRenamed("Reason", "reason").withColumnRenamed("ReasonDetail", "reason_detail") \
                .select("date", "device", "currency", "reason", "reason_detail", "amount", "reg_datetime")
    return currency_df

for device in ["pc", "console"]:
    try:
        currency_increased = get_currency_increased(device, target_date)
        table_name = "all_economy_currency_increased_daily"
        delete_for_rebatch(device, target_date, table_name)
        mysql.insert_table(currency_increased, "pubg_gi", table_name)
        slack.send(CHANNEL, "{} for {} date: {} Succeeded".format(table_name, device, target_date), 'good')
    except Exception as e:
        slack.send(CHANNEL, "Error occurred in daily_gb_pdu_seoyoung_all_economy_currency_increased_daily.py, \n device: {} \n target date: {} \n".format(device, target_date) + str(e), "danger")

