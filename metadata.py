# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：数据 ---- 医院大全的数据建模

"""

import os
import uuid
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import isnull
from pyspark.sql.functions import lit
from pyspark.sql.functions import when

def prepare():
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


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pudf_id_generator(oid):
	frame = {
		"_ID": oid
	}
	df = pd.DataFrame(frame)
	df["RESULT"] = df["_ID"].apply(lambda x: str(uuid.uuid4()))
	return df["RESULT"]


@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def pudf_max_between_rows(a, b):
	frame = {
		"LEFT": a,
		"RIGHT": b
	}
	df = pd.DataFrame(frame)
	df["RESULT"] = df.apply(lambda x: max(x["LEFT"], x["RIGHT"]), axis=1)
	return df["RESULT"]


def func_pviot_hosp(df, cols, fact_name, cats, tags, version, df_standard, standard_cols):
	select_cols = ["ID", "PHA_ID"]
	select_cols.extend(cols)
	print(cols)
	df_tmp = df.select(select_cols)
	# df_tmp.persist()

	df_tmp_missing = df_tmp.where(col(cols[0]) == "ERROR:  #N/A")
	df_tmp_fill = df_tmp_missing.join(df_standard, df_tmp_missing.PHA_ID == df_standard["PHA ID"], how="left")
	for idx in range(len(cols)):
		df_tmp_fill = df_tmp_fill.withColumn(cols[idx], col(standard_cols[idx]))
	
	df_tmp = df_tmp_fill.select(select_cols).union(df_tmp.where(col(cols[0]) != "ERROR:  #N/A"))
	
	for idx in range(len(tags)):
		df_tmp  = df_tmp.withColumnRenamed(cols[idx], tags[idx])
	
	df_tmp = df_tmp.withColumn("CATEGORY", lit(cats)) \
					.withColumn("FACT_NAME", lit(fact_name)) \
					.withColumn("version", lit(version))
	
	return df_tmp


if __name__ == '__main__':
	spark = prepare()
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/hosps/v20200729_20201102_1").where(col("STANDARD") == "COMMON")
	
	# 1. ID
	df = df.withColumn("ID", pudf_id_generator(df._ID))
	df.persist()
	print(df.count())
	
	# 2. 维度数据表开始
	df_dim_hosp = df.select("ID", "PHA_ID", "PHA_HOSP_NAME", "PROVINCE", "CITY", "CITY_TIER", "DISTRICT", "LOCATION", "REGION")
	df_dim_hosp_missing = df_dim_hosp.where(df.PHA_HOSP_NAME == "ERROR:  #N/A")
	
	df_standard = spark.read.csv("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/2020年Universe更新维护_20200729.csv", header=True).where(~isnull(col("重复"))) \
						.withColumnRenamed("PROVINCE", "省") \
						.withColumnRenamed("CITY", "市") \
						.withColumnRenamed("DISTRICT", "区") \
						.withColumnRenamed("Region", "区域") \
						.withColumnRenamed("location", "坐标")
	df_standard = df_standard.withColumn("医生数1", col("医生数1").cast("int")) \
								.withColumn("医生数2", col("医生数2").cast("int"))
	df_standard = df_standard.na.fill({"医生数1": 0, "医生数2": 0})
	df_standard = df_standard.withColumn("在职员工人数", when(col("在职员工人数") == "#N/A", 0).otherwise(col("在职员工人数")))
	df_standard = df_standard.withColumn("医生数", pudf_max_between_rows(df_standard["医生数1"], df_standard["医生数2"]))
	df_standard.persist()
	# df_standard.where(col("PHA ID") == "PHA0010596").show()

	df_dim_hosp_fill = df_dim_hosp_missing.join(df_standard, df_dim_hosp_missing.PHA_ID == df_standard["PHA ID"], how="left") \
							.withColumn("PHA_HOSP_NAME", df_standard["新版名称"]) \
							.withColumn("PROVINCE", df_standard["省"]) \
							.withColumn("CITY", df_standard["市"]) \
							.withColumn("DISTRICT", df_standard["区"]) 
	df_dim_hosp_fill = df_dim_hosp_fill.select("ID", "PHA_ID", "PHA_HOSP_NAME", "PROVINCE", "CITY", "CITY_TIER", "DISTRICT", "LOCATION", "REGION")

	df_dim_hosp = df_dim_hosp.where(df.PHA_HOSP_NAME != "ERROR:  #N/A").union(df_dim_hosp_fill).withColumn("VERSION", lit(""))
	df_dim_hosp.persist()
	# df.unpersist()
	
	df_dim_hosp.write.mode("overwrite").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/dims/hosps")
	
	# 3. 和医院相关
	df_fact_table_operation = func_pviot_hosp(df, ["DOCTORS_NUM", "EMPLOYEES_NUM"], "OPERATING", "EMPLOYEES", ["TOTAL", "DOCTORS"], "v0.0.1", df_standard, ["医生数", "在职员工人数"])
	# df_fact_table_operation.show()
	print(df_fact_table_operation.count())
	