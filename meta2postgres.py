# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：数据 ---- 医院大全的meta 写入到数据库

"""

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import sum


def write2postgres(df, pgTable):
	print("write postgresql start")
	url = "jdbc:postgresql://ph-db-lambda.cngk1jeurmnv.rds.cn-northwest-1.amazonaws.com.cn/phreports"
	# spark.read.csv(s3Path, header=True) \
	df.write.format("jdbc") \
		.option("url", url) \
		.option("dbtable", pgTable) \
		.option("user", "pharbers") \
		.option("password", "Abcde196125") \
		.option("driver", "org.postgresql.Driver") \
		.mode("overwrite") \
		.save()
	print("write postgresql end")


def prepare():
	sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'
	os.environ["PYSPARK_PYTHON"] = "python3"
	# 读取s3桶中的数据
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("CPA&GYC match refactor") \
		.config("spark.driver.memory", "2g") \
		.config("spark.executor.cores", "4") \
		.config("spark.executor.instances", "4") \
		.config("spark.executor.memory", "2g") \
		.config('spark.sql.codegen.wholeStage', False) \
		.config("spark.sql.execution.arrow.pyspark.enable", "true") \
		.config("spark.driver.extraClassPath", sparkClassPath) \
		.getOrCreate()
		# .config("spark.sql.autoBroadcastJoinThreshold", 1048576000) \
		# .config("spark.sql.files.maxRecordsPerFile", 554432) \

	access_key = os.getenv("AWS_ACCESS_KEY_ID")
	secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
	if access_key is not None:
		spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
		spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
		spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
		# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
		spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

	return spark


if __name__ == '__main__':
	spark = prepare()
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/dims/hosps").withColumn("COUNT", lit(1))
	
	df_city_level = df.groupBy("PROVINCE", "CITY").agg(sum(df.COUNT).alias("COUNT"))
# 	df_city_level.show()
	write2postgres(df_city_level, "hosp_metadata")
	