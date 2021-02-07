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
from pyspark.sql.functions import array, array_distinct, array_union
from pyspark.sql.functions import coalesce
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.ml import PipelineModel


def prepare():
# 	sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'
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
# 		.config("spark.driver.extraClassPath", sparkClassPath) \
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
	
	
@pandas_udf(ArrayType(StringType()), PandasUDFType.GROUPED_AGG)
def pudf_dosage_replace(sch, sen):
	frame_left = {
		"RESULT": sch,
	}
	df_left = pd.DataFrame(frame_left)
	
	frame_right = {
		"RESULT": sen,
	}
	df_right = pd.DataFrame(frame_right)
	df_result = df_left.append(df_right).drop_duplicates()
	
	return df_result["RESULT"].values.tolist()


@pandas_udf(ArrayType(StringType()), PandasUDFType.GROUPED_AGG)
def pudf_dosage_mapping_agg(sch):
	frame = {
		"RESULT": sch,
	}
	df = pd.DataFrame(frame)
	
	df["RESULT"] = df["RESULT"].apply(lambda x: x)
	
	return df["RESULT"]


if __name__ == '__main__':
	spark = prepare()
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-02-06T13_53_23.413024+00_00/cleaning_data_model_predictions/negative_result")
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-02-06T13_53_23.413024+00_00/cleaning_data_model_predictions/positive_result")
	
	df = df \
			.where((df.label == 1) & (df.prediction == 0) & (df.EFFTIVENESS_DOSAGE < 0.8)) \
			.distinct()
			# .where((df.EFFTIVENESS_PACK_QTY == 0)) \
			# .select("PACK_ID_STANDARD", "PACK_QTY", "PACK_QTY_STANDARD", "EFFTIVENESS_PACK_QTY") \
			# .where((df.EFFTIVENESS_DOSAGE > 0) & (df.EFFTIVENESS_DOSAGE < 0.9)) \

	df = df.select("DOSAGE", "DOSAGE_STANDARD")
	df = df.groupBy("DOSAGE").agg(pudf_dosage_mapping_agg(df.DOSAGE_STANDARD).alias("DOSAGE_STANDARD"))
	df = df.withColumn("DOSAGE_STANDARD", array_distinct(df.DOSAGE_STANDARD))
	
	dfm = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DOSAGE_MAPPING/CHC/V0.0.2")
	
	df_result = dfm.join(df, on="DOSAGE", how="fullouter")
	df_result = df_result.withColumn("MASTER_DOSAGE", coalesce(df_result.MASTER_DOSAGE, array()))
	df_result = df_result.withColumn("DOSAGE_STANDARD", coalesce(df_result.DOSAGE_STANDARD, array()))
	df_result = df_result.withColumn("RESULT", array_union(df_result.MASTER_DOSAGE, df_result.DOSAGE_STANDARD))
	df_result = df_result.select("DOSAGE", "RESULT").withColumnRenamed("RESULT", "MASTER_DOSAGE")
	# df_result.show(100)
	
	df_result.repartition(1).write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DOSAGE_MAPPING/CHC/V0.0.3")
	