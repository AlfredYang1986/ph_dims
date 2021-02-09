# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：数据 ---- 医院大全的meta 写入到数据库

"""

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, isnull, when
from pyspark.sql.functions import sum, first
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
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/runid_alfred_runner_test/cleaning_data_model_predictions/positive_result")
	# print(df.select("ID").distinct().count())
	# df = df.where((df.label == 1) & (df.prediction == 1))
	# print(df.select("ID").distinct().count())
	df = df.select("ID").distinct().withColumn("TMP", lit(1))
	
	dfn = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/runid_alfred_runner_test/cleaning_data_model_predictions/negative_result")
	# dfn = dfn.join(df, on="ID", how="leftouter")
	# dfn = dfn.where(isnull(dfn.TMP))
	dfn = dfn.where((dfn.label == 1) & (dfn.prediction == 1))
	dfn.select("ID", "label", "prediction", "similarity", "RANK").show(100, truncate=False)
	# print(dfn.select("ID").distinct().count())
	
	dfp = spark.read.parquet("s3a://ph-max-auto/2020-08-11/data_matching/refactor/runs/runid_alfred_runner_test/cleaning_data_model_predictions/prediction_result")
	# dfp = dfp.where(dfp.ID == "ddd02521-2c08-4d85-8fb0-2dbf04f39017") \
	# dfp = dfp.where(dfp.ID == "766138d6-0087-428c-a7d2-976fa4f4a5eb") \
				# .select("ID", "label", "prediction", "similarity", "RANK", "MOLE_NAME", "MOLE_NAME_STANDARD", "EFFTIVENESS_MOLE_NAME") \
				# .select("ID", "label", "prediction", "similarity", "RANK", "EFFTIVENESS_DOSAGE", "EFFTIVENESS_MANUFACTURER", "EFFTIVENESS_MOLE_NAME", "EFFTIVENESS_PACK_QTY", "EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_SPEC") \
				# .select("ID", "label", "prediction", "similarity", "MOLE_NAME", "MOLE_NAME_STANDARD", "EFFTIVENESS_SPEC", "SPEC", "SPEC_STANDARD") \
				# .select("ID", "label", "prediction", "similarity", "MOLE_NAME", "EFFTIVENESS_MANUFACTURER", "MANUFACTURER_NAME", "MANUFACTURER_NAME_STANDARD") \
				# .select("ID", "label", "prediction", "similarity", "MOLE_NAME", "MOLE_NAME_STANDARD", "PRODUCT_NAME", "PRODUCT_NAME_STANDARD", "SPEC", "SPEC_STANDARD", "MANUFACTURER_NAME", "MANUFACTURER_NAME_STANDARD", "DOSAGE", "DOSAGE_STANDARD") \
	# dfp = dfp.where(dfp.ID == "5daed4d5-c650-433d-9d8d-b505f7dcdd01") \
	dfp = dfp.where(dfp.ID == "58e21663-7b03-4de1-9310-a251fe606f14") \
				.show(100, truncate=False)