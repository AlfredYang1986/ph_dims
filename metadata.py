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
from pyspark.sql.functions import regexp_replace


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


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pudf_description_info(t):
	frame = {
		"TAG": t
	}
	df = pd.DataFrame(frame)
	
	lt = [
			"TOTAL_EMPLOYEES", "DOCTORS",
			"IN_RESIDENCE", "ANNUALLY", "PHYSICIAN", "OPD", "SURGERY", "SURGERY_IN_RESIDENCE",
			"GROSS", "GROSS_CH", "COVER",
			"TOTAL_BED_CONPACITY", "COMPILING", "GENERAL", "PHYSICIAN", "OPENNESS", "OPHTHALMIC", "SURGERY",
			"BED_IN_RESIDENCE", "MEDICINE_RMB", "MEDICINE", "MEDICINE_IN_RESIDENCE", "IN_RESIDENCE","SURGERY_IN_RESIDENCE", "TREATMENT_IN_RESIDENCE", 
			"WST_MEDICINE_IN_RESIDENCE", "MEDICINE_OPT", "OPT", "SURGERY_OPT", "TREATMENT_OPT", "WST_MEDICINE_OPT", "TREATMENT", "MEDICINE_DISTRICT", 
			"MILITARY", "EPILEPSY", "DOCTOR_LIB", "REPRODUCT", "BID_SAMPLE", 
			"AREA_LEVEL", "LEVEL", "NATURE", "SPECIALTY_CATE", "SPECIALTY_ADMIN", "RE_SPECIALTY", "SPECIALTY_DETAIL"
		]
		
	lr = [
			"医生数", "在职员工人数",
			"入院人数", "年诊疗人次", "内科诊次", "门诊诊次", "外科诊次", "住院病人手术人次数",
			"占地面积(m2)", "占地面积(亩)", "建筑面积",
			"床位数", "编制床位数", "全科床位数", "内科床位数", "开放床位数", "眼科床位数", "外科床位数",
			"住院床位收入", "Est_DrugIncome_RMB", "药品收入", "住院药品收入", "住院收入", "住院手术收入", "住院治疗收入", "住院西药收入",
			"门诊药品收入", "门诊收入", "门诊手术收入", "门诊治疗收入", "门诊西药收入", "医疗收入", "2017年县医院西药收入",
			"军队", "综合/专科", "医生库医院", "生殖中心", "招标Sample(county HP)共有样本2303，县医院样本932",
			"Type", "Hosp_level", "公立/私立", "Specialty_1_标准化", "Specialty_2_标准化", "Re-Speialty", "Specialty 3"
		]
	
	def inner_index_finder(x):
		return lr[lt.index(x)]
	
	df["RESULT"] = df["TAG"].apply(lambda x: inner_index_finder(x))
	return df["RESULT"]


def func_pviot_hosp(df, cols, fact_name, cats, tags, version, df_standard, standard_cols):
	select_cols = ["ID", "PHA_ID"]
	select_cols.extend(cols)
	df_tmp = df.select(select_cols)
	# df_tmp.persist()

	# df_tmp.show()
	# df_tmp.printSchema()
	df_tmp_missing = df_tmp #.where(col(cols[0]) == "ERROR:  #N/A")
	df_tmp_fill = df_tmp_missing.join(df_standard, df_tmp_missing.PHA_ID == df_standard["PHA ID"], how="left")
	for idx in range(len(cols)):
		df_tmp_fill = df_tmp_fill.withColumn(cols[idx], col(standard_cols[idx]).cast("string"))
	
	df_tmp = df_tmp_fill.select(select_cols) # .union(df_tmp.where(col(cols[0]) != "ERROR:  #N/A"))
	# df_tmp.show()
	
	for idx in range(len(tags)):
		df_tmp  = df_tmp.withColumnRenamed(cols[idx], tags[idx])
	
	df_tmp = df_tmp.withColumn("CATEGORY", lit(cats)) \
					.withColumn("FACT_NAME", lit(fact_name)) \
					.withColumn("VERSION", lit(version))

	# df_tmp.printSchema()
	df_tmp.createOrReplaceTempView("df_pivot")
	
	sql_prefix = """SELECT ID, PHA_ID, CATEGORY, FACT_NAME, VERSION,"""
	sql_suffix = """ as (`TAG`, `VALUE`) FROM df_pivot"""
	sql_steps = []
	for item in tags:
		sql_steps.append("\'" + item + "\'" + ", " + "`" + item + "`")
	sql_query = sql_prefix + "stack(" + str(len(tags)) + "," + ",".join(sql_steps) + ")" + sql_suffix
	# print(sql_query)
	df_pivot = spark.sql(sql_query).na.fill("0").withColumn("VALUE", regexp_replace('VALUE', r'#N/A', '0'))
	df_pivot = df_pivot.withColumn("DESCRIPTION", pudf_description_info(df_pivot.TAG))
	df_pivot = df_pivot.withColumnRenamed("ID", "HOSP_ID")
	df_pivot = df_pivot.withColumn("ID", pudf_id_generator(df_pivot.HOSP_ID))
	
	return df_pivot


if __name__ == '__main__':
	spark = prepare()
	
	df = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/hosps/v20200729_20201102_1").where(col("STANDARD") == "COMMON")
	
	# 1. ID
	df = df.withColumn("ID", pudf_id_generator(df._ID))
	df.persist()
	# print(df.count())
	
	# 2. 维度数据表开始
	df_dim_hosp = df.select("ID", "PHA_ID", "PHA_HOSP_NAME", "PROVINCE", "CITY", "CITY_TIER", "DISTRICT", "LOCATION", "REGION")
	df_dim_hosp_missing = df_dim_hosp.where(df.PHA_HOSP_NAME == "ERROR:  #N/A")
	
	df_standard = spark.read.csv("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/2020年Universe更新维护_20200729.csv", header=True).where(~isnull(col("重复"))) \
						.withColumnRenamed("PROVINCE", "省") \
						.withColumnRenamed("CITY", "市") \
						.withColumnRenamed("DISTRICT", "区") \
						.withColumnRenamed("Region", "区域") \
						.withColumnRenamed("location", "坐标") \
						.withColumnRenamed("Hosp_level", "医院等级")
	df_standard = df_standard.withColumn("医生数1", col("医生数1").cast("int")) \
							.withColumn("医生数2", col("医生数2").cast("int")) \
							.withColumn("门诊诊次1", col("门诊诊次1").cast("int")) \
							.withColumn("门诊诊次2", col("门诊诊次2").cast("int")) \
							.withColumn("年诊疗人次1", col("年诊疗人次1").cast("int")) \
							.withColumn("年诊疗人次2", col("年诊疗人次2").cast("int"))
	df_standard = df_standard.na.fill({
									"医生数1": 0, "医生数2": 0,
									"年诊疗人次1": 0, "年诊疗人次2": 0,
									"门诊诊次1": 0, "门诊诊次2": 0,
								})
	df_standard = df_standard.withColumn("在职员工人数", when(col("在职员工人数") == "#N/A", 0).otherwise(col("在职员工人数")))
	df_standard = df_standard.withColumn("医生数", pudf_max_between_rows(df_standard["医生数1"], df_standard["医生数2"]))
	df_standard = df_standard.withColumn("年诊疗人次", pudf_max_between_rows(df_standard["年诊疗人次1"], df_standard["年诊疗人次2"]))
	df_standard = df_standard.withColumn("门诊诊次", pudf_max_between_rows(df_standard["门诊诊次1"], df_standard["门诊诊次2"]))
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
	df_fact_table_operation = func_pviot_hosp(df, ["DOCTORS_NUM", "EMPLOYEES_NUM"], "OPERATING", "EMPLOYEES", ["TOTAL_EMPLOYEES", "DOCTORS"], "v0.0.1", df_standard, ["医生数", "在职员工人数"])
	df_fact_table_operation.write.partitionBy("VERSION", "FACT_NAME", "CATEGORY").mode("append").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/facts/hosp_operations")
	
	df_fact_table_operation = func_pviot_hosp(df, ["ADMIS_TIME", "ANNU_DIAG_TIME", "INTERNAL_DIAG_TIME", "OUTP_DIAG_TIME", "SURG_DIAG_TIME", "SURG_TIME"], 
												"OPERATING", "PATIENT", ["IN_RESIDENCE", "ANNUALLY", "PHYSICIAN", "OPD", "SURGERY", "SURGERY_IN_RESIDENCE"], 
												"v0.0.1", df_standard, ["入院人数", "年诊疗人次", "内科诊次", "门诊诊次", "外科诊次", "住院病人手术人次数"])
	df_fact_table_operation.write.partitionBy("VERSION", "FACT_NAME", "CATEGORY").mode("append").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/facts/hosp_operations")
	
	
	df_fact_table_operation = func_pviot_hosp(df, ["AREA_SQ_M", "AREA_MU", "AREA_STRUCT"], "OPERATING", "AREASIZE", ["GROSS", "GROSS_CH", "COVER"], "v0.0.1", df_standard, ["占地面积(m2)", "占地面积(亩)", "建筑面积"])
	df_fact_table_operation.write.partitionBy("VERSION", "FACT_NAME", "CATEGORY").mode("append").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/facts/hosp_operations")
	
	df_fact_table_operation = func_pviot_hosp(df, ["BED_NUM", "AUTH_BED_NUM", "GENERAL_BED_NUM", "INTERNAL_BED_NUM", "OPEN_BED_NUM", "OPHTH_BED_NUM", "SURG_BED_NUM"], 
												"OPERATING", "BEDCAPACITY", ["TOTAL_BED_CONPACITY", "COMPILING", "GENERAL", "PHYSICIAN", "OPENNESS", "OPHTHALMIC", "SURGERY"], 
												"v0.0.1", df_standard, ["床位数", "编制床位数", "全科床位数", "内科床位数", "开放床位数", "眼科床位数", "外科床位数"])
	df_fact_table_operation.write.partitionBy("VERSION", "FACT_NAME", "CATEGORY").mode("append").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/facts/hosp_operations")
	
	df_fact_table_operation = func_pviot_hosp(df, ["BED_INCOME", "DRUGINCOME_RMB", "DRUG_INCOME", "IN_HOSP_DRUG_INCOME", "IN_HOSP_INCOME", "IN_HOSP_SURG_INCOME", "IN_HOSP_TREAT_INCOME", 
													"IN_HOSP_WST_DRUG_INCOME", "OUTP_DRUG_INCOME", "OUTP_INCOME", "OUTP_SURG_INCOME", "OUTP_TREAT_INCOME", "OUTP_WST_DRUG_INCOME", "MED_INCOME", 
													"COUNTY_HOSP_WST_DRUG_INCOME"], "OPERATING", "REVENUE", 
													["BED_IN_RESIDENCE", "MEDICINE_RMB", "MEDICINE", "MEDICINE_IN_RESIDENCE", "IN_RESIDENCE","SURGERY_IN_RESIDENCE", "TREATMENT_IN_RESIDENCE", 
													"WST_MEDICINE_IN_RESIDENCE", "MEDICINE_OPT", "OPT", "SURGERY_OPT", "TREATMENT_OPT", "WST_MEDICINE_OPT", "TREATMENT", "MEDICINE_DISTRICT"], 
												"v0.0.1", df_standard, ["住院床位收入", "Est_DrugIncome_RMB", "药品收入", "住院药品收入", "住院收入", "住院手术收入", "住院治疗收入", "住院西药收入",
													"门诊药品收入", "门诊收入", "门诊手术收入", "门诊治疗收入", "门诊西药收入", "医疗收入", "2017年县医院西药收入"])
	df_fact_table_operation.write.partitionBy("VERSION", "FACT_NAME", "CATEGORY").mode("append").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/facts/hosp_operations")
	
	df_fact_table_operation = func_pviot_hosp(df, ["MILITARY", "EPILEPSY", "DOCT_BANK", "REPRODUCT", "BID_SAMPLE"], "CLASS", "IS", 
													["MILITARY", "EPILEPSY", "DOCTOR_LIB", "REPRODUCT", "BID_SAMPLE"], 
												"v0.0.1", df_standard, ["军队", "综合/专科", "医生库医院", "生殖中心", "招标Sample(county HP)共有样本2303，县医院样本932"])
	df_fact_table_operation.write.partitionBy("VERSION", "FACT_NAME", "CATEGORY").mode("append").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/facts/hosp_operations")
	
	df_fact_table_operation = func_pviot_hosp(df, ["HOSP_TYPE", "HOSP_LEVEL", "HOSP_QUALITY", "SPECIALTY_CATE", "SPECIALTY_ADMIN", "RE_SPECIALTY", "SPECIALTY_DETAIL"], "CLASS", "TYPE", 
													["AREA_LEVEL", "LEVEL", "NATURE", "SPECIALTY_CATE", "SPECIALTY_ADMIN", "RE_SPECIALTY", "SPECIALTY_DETAIL"], 
												"v0.0.1", df_standard, ["Type", "医院等级", "公立/私立", "Specialty_1_标准化", "Specialty_2_标准化", "Re-Speialty", "Specialty 3"])
	df_fact_table_operation.write.partitionBy("VERSION", "FACT_NAME", "CATEGORY").mode("append").parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/alfred/tmp/dimensions/refactor/alfred/facts/hosp_operations")
	