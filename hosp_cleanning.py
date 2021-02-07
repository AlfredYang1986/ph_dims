# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：数据 ---- 医院大全的meta 写入到数据库

"""

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col, monotonically_increasing_id, isnull
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import desc
from nltk.metrics.distance import jaro_winkler_similarity


def prepare():
	os.environ["PYSPARK_PYTHON"] = "python3"
	# 读取s3桶中的数据
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("CPA&GYC match refactor") \
		.config("spark.driver.memory", "1g") \
		.config("spark.executor.cores", "1") \
		.config("spark.executor.instances", "2") \
		.config("spark.executor.memory", "1g") \
		.config('spark.sql.codegen.wholeStage', False) \
		.config("spark.sql.execution.arrow.pyspark.enable", "true") \
		.getOrCreate()

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
	
	
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pudf_redefine_hosp_name(p, c, d, n):
	frame = {
		"PROVINCE": p,
		"CITY": c,
		"DISTRICT": d,
		"NAME": n,
	}
	df = pd.DataFrame(frame)

	def inner_row_redfine_hosp_name(rn, rp, rc, rd):
		prefix = ""
		if rp:
			rn = rn.replace(rp, "")
			prefix = prefix + rp
		if rc:
			rn = rn.replace(rc, "")
			if rp != rc:
				prefix = prefix + rc
		if rd:
			rn = rn.replace(rd, "")
			prefix = prefix + rd
		return rn
	

	df["RESULT"] = df.apply(lambda x: inner_row_redfine_hosp_name(x["NAME"], x["PROVINCE"], x["CITY"], x["DISTRICT"]), axis=1)
	return df["RESULT"]


@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def pudf_similarity(n, cn):
	frame = {
		"LEFT": n, 
		"RIGHT": cn
	}
	df = pd.DataFrame(frame)
	
	df["RESULT"] = df.apply(lambda x: jaro_winkler_similarity(x["LEFT"], x["RIGHT"]), axis=1)
	return df["RESULT"]

	
if __name__ == '__main__':
	spark = prepare()

	df_standard = spark.read.csv("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/data/Pfizer_standard.csv", header="true")
	df_standard = df_standard.select("医院名称", "省份", "城市", "区县") \
						.withColumnRenamed("医院名称", "NAME") \
						.withColumnRenamed("省份", "PROVINCE") \
						.withColumnRenamed("城市", "CITY") \
						.withColumnRenamed("区县", "DISTRICT") 
	df_standard = df_standard.withColumn("STANDARD_NAME", col("NAME"))
		
	df_cleanning = spark.read.csv("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/cleanning/Pfizer_cleanning.csv", header="true")
	df_cleanning = df_cleanning.select("KF_Code", "account_name", "province", "city", "County") \
						.withColumn("NAME", col("account_name")) \
						.withColumn("PROVINCE", col("province")) \
						.withColumn("CITY", col("city")) \
						.withColumn("DISTRICT", col("County"))
	df_cleanning = df_cleanning.repartition(1).withColumn("ID", monotonically_increasing_id()).repartition(4)
						

	stopwords = r"[市,区,自治,县,省,州]"
	
	df_standard = df_standard.withColumn("NAME", regexp_replace('NAME', stopwords, '')) \
							.withColumn("PROVINCE", regexp_replace('PROVINCE', stopwords, '')) \
							.withColumn("CITY", regexp_replace('CITY', stopwords, '')) \
							.withColumn("DISTRICT", regexp_replace('DISTRICT', stopwords, '')) 
						
	df_standard = df_standard.withColumn("NAME", pudf_redefine_hosp_name(col("PROVINCE"), col("CITY"), col("DISTRICT"), col("NAME")))
	df_standard.persist()

	df_cleanning = df_cleanning.withColumn("NAME", regexp_replace('NAME', stopwords, '')) \
							.withColumn("PROVINCE", regexp_replace('PROVINCE', stopwords, '')) \
							.withColumn("CITY", regexp_replace('CITY', stopwords, '')) \
							.withColumn("DISTRICT", regexp_replace('DISTRICT', stopwords, '')) \
							.withColumnRenamed("NAME", "CLEANNING_NAME")
	df_cleanning = df_cleanning.withColumn("CLEANNING_NAME", pudf_redefine_hosp_name(col("PROVINCE"), col("CITY"), col("DISTRICT"), col("CLEANNING_NAME")))	
	df_cleanning.persist()

	df_result = df_cleanning.join(df_standard, on=["PROVINCE", "CITY", "DISTRICT"], how="leftouter")
	df_result.persist()
	df_result_with_s = df_result.where(~isnull(df_result.NAME))
	df_result_with_s = df_result_with_s.withColumn("SIMILARITY", pudf_similarity(col("NAME"), col("CLEANNING_NAME")))
	print(df_result_with_s.select("ID").distinct().count())
	df_result_without_s = df_result.where(isnull(df_result.NAME))
	print(df_result_without_s.select("ID").distinct().count())
	window = Window.partitionBy("ID").orderBy(desc("SIMILARITY"))
	df_result_with_s = df_result_with_s.withColumn("RANK", rank().over(window))
	df_result_with_s = df_result_with_s.where(df_result_with_s.RANK == 1).select("KF_Code", "account_name", "province", "city", "County", "STANDARD_NAME", "ID", "SIMILARITY")
	df_result_with_s = df_result_with_s.where(df_result_with_s.SIMILARITY > 0.75)
	# df_result_with_s.orderBy("SIMILARITY").show(100)
	print(df_result_with_s.count())
	df_result_with_s.repartition(1).write.option("header", "true").mode("overwrite").csv("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/result")