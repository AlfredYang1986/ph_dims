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
from pyspark.sql.functions import array
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
		.config("spark.driver.memory", "2g") \
		.config("spark.executor.cores", "4") \
		.config("spark.executor.instances", "4") \
		.config("spark.executor.memory", "2g") \
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


if __name__ == '__main__':
	spark = prepare()
	
# 	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/runid_alfred_runner_test/cross_join_cutting/cross_result")
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/runid_alfred_runner_test/cleaning_data_normalization/cleaning_result")
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DOSAGE_MAPPING/CHC/V0.0.1")
	# df.show()
	# df = df.withColumn("DOSAGE", df.CHC_DOSAGE)
	# df = df.groupBy("DOSAGE").agg(pudf_dosage_replace(df.MASTER_DOSAGE, df.EN_DOSAGE).alias("MASTER_DOSAGE"))
	# df.repartition(1).write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DOSAGE_MAPPING/CHC/V0.0.2")
	
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DOSAGE_MAPPING/CPA/V0.0.1")
	# # df.show()

	# df = df.withColumn("DOSAGE", df.CPA_DOSAGE)
	# df = df.drop("CPA_DOSAGE")
	# df.repartition(1).write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/data/DOSAGE_MAPPING/CPA/V0.0.2")
	
	# df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-01-19T05_48_49.058508+00_00/cleaning_data_normalization/cleaning_origin")
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-01-20T07_08_30.726901+00_00/cleaning_data_normalization/cleaning_origin")
	df.show()
	
	
	# model = PipelineModel.load("s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-01-17_16:06:26/Models")
	# print(model[2].toDebugString)
	
	# df = spark.read.parquet("s3a://ph-stream/common/public/prod/0.0.21")
	# df.repartition(1).write.mode("overwrite").option("header", "true").csv("s3a://ph-max-auto/2020-08-11/data_matching/refactor/results/2021-01-17_22-57-34/Standards")