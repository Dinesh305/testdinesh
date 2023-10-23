# Databricks notebook source
dbutils.widgets.text("AwsIAMRole", "", "AwsIAMRole")
AwsIAMRole = dbutils.widgets.get("AwsIAMRole")

dbutils.widgets.text("Environment", "", "Environment")
Environment = dbutils.widgets.get("Environment")

dbutils.widgets.text("RedshiftDBName", "", "RedshiftDBName")
RedshiftDBName = dbutils.widgets.get("RedshiftDBName") 

dbutils.widgets.text("RedshiftInstance", "", "RedshiftInstance")
RedshiftInstance = dbutils.widgets.get("RedshiftInstance")

dbutils.widgets.text("RedshiftPort", "", "RedshiftPort")
RedshiftPort = dbutils.widgets.get("RedshiftPort")

dbutils.widgets.text("RedshiftUser", "", "RedshiftUser")
RedshiftUser = dbutils.widgets.get("RedshiftUser")

dbutils.widgets.text("RedshiftUserPass", "", "RedshiftUserPass")
RedshiftUserPass = dbutils.widgets.get("RedshiftUserPass")

dbutils.widgets.text("rs_final_read", "", "rs_final_read")
rs_final_read = dbutils.widgets.get("rs_final_read")

dbutils.widgets.text("TempS3Bucket", "", "TempS3Bucket")
tempS3Dir = dbutils.widgets.get("TempS3Bucket")

dbutils.widgets.text("ParquetFile", "", "ParquetFile")
parquetFile = dbutils.widgets.get("ParquetFile")

# COMMAND ----------

import traceback
import time
no_of_retry  = 3
jdbcConnection = ''

for retry in range(no_of_retry):
  exception = ''
  try:
    jdbcConnection = "jdbc:redshift://{}:{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(RedshiftInstance, RedshiftPort, RedshiftDBName, RedshiftUser, RedshiftUserPass)
    enrichDF = sqlContext.read.format("com.databricks.spark.redshift").option("url",jdbcConnection).option("tempdir", tempS3Dir).option("dbtable", rs_final_read).option("aws_iam_role",AwsIAMRole).load()
    enrichDF.createOrReplaceTempView("enrich_Final")
    dfFromRedshift = sqlContext.sql("select * from enrich_Final limit 1")
  except Exception as e:
    tb = traceback.format_exc()
    exception = "Exception on Redshift connection check. Stack Trace is: " + '\n' + str(tb)
    dfFromRedshift = ''
    time.sleep(5)# 5 seconds delay
    print (f"Failed due to {exception}")
  else:
    break

# COMMAND ----------

print (jdbcConnection)

# COMMAND ----------

TestStatus =""
if dfFromRedshift:
  TestStatus = "PASSED"
else:
  TestStatus = "FAILED \n" + exception
  print (exception)

# COMMAND ----------

# --- Secondary checks to make sure that the parquet file matches schema of the redshift table --- #
try:
  redshiftDataframe = dfFromRedshift
  parquetDataframe = spark.read.parquet(parquetFile)

  columnsMatch = True
  for column in redshiftDataframe.columns:
    if not column in parquetDataframe.columns:
      columnsMatch = False
except:
  columnsMatch = False

print (columnsMatch)

# COMMAND ----------

#TODO: commenting this out as we haven't fully integrated 'columnsMatch' test yet
#FinalStatus = "PASSED" if columnsMatch and TestStatus == "PASSED" else "FAILED"

FinalStatus = "PASSED" if TestStatus == "PASSED" else "FAILED"
columnsMatch = "PASSED" if columnsMatch else "FAILED"

TestOutputNotebook  = "Enrichment Redshift Connection Test Output"
EnvironmentVal      = "Environment is           : %s" %Environment  
RedshiftInstanceVal = "Redshift Instance        : %s" %RedshiftInstance
RedshiftDBNameVal   = "Redshift DBName          : %s" %RedshiftDBName
RedshiftTableName   = "Redshift Table Name      : %s" %rs_final_read
RedshiftUserVal     = "Redshift User            : %s" %RedshiftUser 
RetriedNum          = "Connection Attempts Done : %s" %str(retry+1)
ConnectionStatus    = "Connection Status        : %s" %TestStatus
SchemaStatus        = "Column Name Test Status  : %s" %columnsMatch
FinalTestStatus     = "Test Status is           : %s" %FinalStatus
Underline           = "---------------------------------------------------------------------------------------"

fullTestResult = "\n".join([TestOutputNotebook,Underline,EnvironmentVal,RedshiftInstanceVal,RedshiftDBNameVal,RedshiftTableName,RedshiftUserVal,RetriedNum,ConnectionStatus,SchemaStatus,FinalTestStatus])

# COMMAND ----------

dbutils.notebook.exit(fullTestResult)