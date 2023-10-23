# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("stg_dir", "", "stg_dir")
stg_dir = dbutils.widgets.get("stg_dir")

dbutils.widgets.text("parquet_flags_write","", "parquet_flags_write")
parquet_flags_write = dbutils.widgets.get("parquet_flags_write")

dbutils.widgets.text("parquet_final_write","", "parquet_final_write")
parquet_final_write = dbutils.widgets.get("parquet_final_write")

dbutils.widgets.text("environment","","environment")
environment = str(dbutils.widgets.get("environment"))

dbutils.widgets.text("email","","email")
email = str(dbutils.widgets.get("email"))

dbutils.widgets.text("instance","","instance")
instance = str(dbutils.widgets.get("instance"))

dbutils.widgets.text("required_dq_checks","","required_dq_checks")
required_dq_checks = str(dbutils.widgets.get("required_dq_checks"))

dbutils.widgets.text("stg_rec_guid","","stg_rec_guid")
stg_rec_guid_col_name = str(dbutils.widgets.get("stg_rec_guid"))


dbutils.widgets.text("final","","final")
final = str(dbutils.widgets.get("final"))

# COMMAND ----------

# Convert file_list from string to LIST
if final == '':
  final = []
else:
  final = list(final.split(','))

# COMMAND ----------

print (email)
print (environment)
print (parquet_final_write) 
print (parquet_flags_write)
print (stg_dir)
print (instance)
print (final)
print (required_dq_checks)

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

# MAGIC %run ../workflow/source_key_constants

# COMMAND ----------

# MAGIC %run ../workflow/bat_data

# COMMAND ----------

from collections import OrderedDict
###########################################################################################################
###       Create the list of Dictionary Variables to be send on notebook exit                           ###
###########################################################################################################
metric_dict = OrderedDict()
metric_dict["STATUS"] = 'FAILURE'
metric_dict["STEP"] = 'START'
metric_dict["ERROR"] = 'ERROR'

# COMMAND ----------

LOCATION = 'STAGE'
df_stg_list = []
count_stg = []
stg_dirs = []

for i in final:
  vars()["stg_" + i] = stg_dir + i +'/'
  stg_dirs.append(vars()["stg_" + i])

for i in range(len(stg_dirs)):
  df_stg,count =  get_dataframe_info(LOCATION, stg_dirs[i])
  df_stg_list.append(df_stg)
  count_stg.append(count)

# COMMAND ----------

print(stg_dirs)

# COMMAND ----------

LOCATION = 'FLAGS'
df_flags_list = []
count_flags = []
flags_dirs = []

for i in final:
  vars()["flags_" + i] = parquet_flags_write + i +'_flags/'
  flags_dirs.append(vars()["flags_" + i])

for i in range(len(flags_dirs)):
  df_flags,count =  get_dataframe_info(LOCATION, flags_dirs[i])
  df_flags_list.append(df_flags)
  count_flags.append(count)

# COMMAND ----------

# import boto3
 
# #s3 = boto3.resource('s3')
# s3 = get_boto3_conn ()
# #bucket = s3.Bucket(parquet_flags_write.split("/")[2])
# bucket_name = parquet_flags_write.split("/")[2]
# key = "/".join(parquet_flags_write.split("/")[3:]) + instance + '/error_code=0'
# objs = s3.list_objects(Bucket=bucket_name,Prefix=key+"/")
# #objs = list(bucket.objects.filter(Prefix=key))

# COMMAND ----------

LOCATION = 'FINAL'
df_final_list = []
count_final = []
final_dirs = []

#if len(objs) > 0:
# for i in final:
#   vars()["final_" + i] = parquet_final_write + i +'/'
#   #vars()["final_" + i] = parquet_final_write + i +'/error_code=0/'
#   final_dirs.append(vars()["final_" + i])
# else:
for i in final:
  vars()["final_" + i] = parquet_final_write + i +'/'
  final_dirs.append(vars()["final_" + i])

for i in range(len(final_dirs)):
  df_final,count =  get_dataframe_info(LOCATION, final_dirs[i])
  df_final_list.append(df_final)
  count_final.append(count)

# COMMAND ----------

print(final_dirs)

# COMMAND ----------

passedTests = []
failedTests = []
tuple_type = ''
tupleTypeResult = ''
lotIdValList = []
lotIdList = ''
Underline = "----------------------------------------------------------------------------------------------------------------"

# COMMAND ----------

source = instance
source_name = source

# COMMAND ----------

try:
  for i in range(len(final)):
      validateLotIdResult = validate_lot_id(df_flags_list[i])
      status = str(validateLotIdResult)
      status_msg = "Expected the distinct lot_ids for {} - {} parquet flags converted to timestamp returns True, actual is {} .".format(source,final[i],status)
      if validateLotIdResult == True:
        passedTests.append(status_msg)
      else:
        failedTests.append(status_msg)
except Exception as e:
  status_msg = "Failed to validate lot_id as {} Flag Parquet is empty/not existing.".format(source)
  failedTests.append(status_msg)
  print (e)

# COMMAND ----------

if stg_rec_guid_col_name != "":
  try:
    file_kind = source + 'Final'
    print(stg_rec_guid_col_name)
    source_name = source
    
    for i in range(len(final)):
      if stg_rec_guid_col_name in df_final_list[i].columns:
        validate_stg_rec_guid(df_final_list[i],source_name,stg_rec_guid_col_name,count_final[i],file_kind+final[i])
        
  except Exception as e:
    print(e)
    status_msg = "Failed to validate {0} as {1} File is empty/not existing.".format(stg_rec_guid_col_name,file_kind)
    failedTests.append(status_msg)
else:
  print('STG REC GUID DOES NOT EXIST FOR {}'.format(source_name))

# COMMAND ----------

if stg_rec_guid_col_name != "":  
  try:
    file_kind = source +' Flags'
    print(stg_rec_guid_col_name)


    for i in range(len(final)):
      if stg_rec_guid_col_name in df_flags_list[i].columns:
        validate_stg_rec_guid(df_flags_list[i],source_name,stg_rec_guid_col_name,count_flags[i],file_kind+final[i])

  except Exception as e:
    print(e)
    status_msg = "Failed to validate {0} as {1} File is empty/not existing.".format(stg_rec_guid_col_name,file_kind)
    failedTests.append(status_msg)
else:
  print('STG REC GUID DOES NOT EXIST FOR {}'.format(source_name))

# COMMAND ----------

# mod list validation 
try:
  
  column_name = 'mod_list'
  specific_key = 'DM'
  
  for i in range(len(final)):
      passed, expected, applied  = validate_column_list_flags(df_flags_list[i],column_name,specific_key)
      status_msg = "Expected distinct values of column '{}' in  parquet flag is {}, actual is {} for {}.".format(column_name,expected, applied,final[i])
      if passed:
        passedTests.append(status_msg)
      else:
        failedTests.append(status_msg)
except Exception as e:
  print(e)
  status_msg = "Failed to validate {0} content as Enrich Flag table is empty/not existing.".format(column_name)
  failedTests.append(status_msg)

# COMMAND ----------

# error list validation 
try:
  
  column_name = 'error_list'
  specific_key = 'DQ'
  
  for i in range(len(final)):
      passed, expected, applied  = validate_column_list_flags(df_flags_list[i],column_name,specific_key)
      status_msg = "Expected distinct values of column '{}' in  parquet flag is {}, actual is {} for {}.".format(column_name,expected, applied,final[i])
      if passed:
        passedTests.append(status_msg)
      else:
        failedTests.append(status_msg)
except Exception as e:
  print(e)
  status_msg = "Failed to validate {0} content as Enrich Flag table is empty/not existing.".format(column_name)
  failedTests.append(status_msg)

# COMMAND ----------

try:
  file_kind = 'Parquet Flags'

  for i in range(len(final)):
      actual_schema = df_flags_list[i].schema
      expected_schema = expected_schema_parquet_flags[final[i]]
      if (expected_schema != 'N/A' and actual_schema != 'N/A'):
        validate_schema(actual_schema,expected_schema,file_kind + ' for ' + final[i])
      else:
        failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate schema {0} is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)

# COMMAND ----------

try:
  file_kind = 'Parquet Flags'

  for i in range(len(final)):
      actual_dtypes = df_flags_list[i].dtypes
      expected_dtypes = expected_dtypes_parquet_flags[final[i]]
      if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
        validate_datatypes(actual_dtypes,expected_dtypes,file_kind + ' for ' + final[i])
      else:
        failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate datatypes {0} is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)

# COMMAND ----------

try:
  file_kind = 'Parquet Flags'

  for i in range(len(final)):
      actual_columns = get_columns(df_flags_list[i].dtypes)
      expected_columns = get_columns(expected_dtypes_parquet_flags[final[i]])
      if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
        validate_columns(actual_columns,expected_columns,file_kind + ' for ' + final[i])
      else:
        failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate columns {0}  is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)

# COMMAND ----------

try:
  file_kind = 'Parquet Final'

  for i in range(len(final)):
      actual_schema = df_final_list[i].schema
      expected_schema = expected_schema_parquet_final[final[i]]
      if (expected_schema != 'N/A' and actual_schema != 'N/A'):
        validate_schema(actual_schema,expected_schema,file_kind + ' for ' + final[i])
      else:
        failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate schema {0} is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)

# COMMAND ----------

try:
  file_kind = 'Parquet Final'

  for i in range(len(final)):
      actual_dtypes = df_final_list[i].dtypes
      expected_dtypes = expected_dtypes_parquet_final[final[i]]
      if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
        validate_datatypes(actual_dtypes,expected_dtypes,file_kind + ' for ' + final[i])
      else:
        failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate datatypes {0} is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)

# COMMAND ----------

try:
  file_kind = 'Parquet Final'

  for i in range(len(final)):
      actual_columns = get_columns(df_final_list[i].dtypes)
      expected_columns = get_columns(expected_dtypes_parquet_final[final[i]])
      if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
        validate_columns(actual_columns,expected_columns,file_kind + ' for ' + final[i])
      else:
        failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate columns {0} is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)

# COMMAND ----------

try:
  file_kind = 'Parquet Stage'

  for i in range(len(final)):
      actual_schema = df_stg_list[i].schema
      expected_schema = expected_schema_parquet_stg[final[i]]
      if (expected_schema != 'N/A' and actual_schema != 'N/A'):
        validate_schema(actual_schema,expected_schema,file_kind + ' for ' + final[i])
      else:
        failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate schema {0} is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)

# COMMAND ----------

try:
  file_kind = 'Parquet Stage'

  for i in range(len(final)):
      actual_dtypes = df_stg_list[i].dtypes
      expected_dtypes = expected_dtypes_parquet_stg[final[i]]
      if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
        validate_datatypes(actual_dtypes,expected_dtypes,file_kind + ' for ' + final[i])
      else:
        failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate datatypes {0} is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)
  

# COMMAND ----------

try:
  file_kind = 'Parquet Stage'

  for i in range(len(final)):
      actual_columns = get_columns(df_stg_list[i].dtypes)
      expected_columns = get_columns(expected_dtypes_parquet_stg[final[i]])
      if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
        validate_columns(actual_columns,expected_columns,file_kind + ' for ' + final[i])
      else:
        failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))
except Exception as e:
  print(e)
  status_msg = "Failed to validate columns {0} is empty/not existing.".format(file_kind)
  failedTests.append(status_msg)

# COMMAND ----------

TotalTestsCount = len(passedTests) + len(failedTests)
passedCount = len(passedTests)
failedCount = len(failedTests)
passPercent = (passedCount/TotalTestsCount)*100
failPercent = (failedCount/TotalTestsCount)*100
passedValidations = ''
failedvalidations = ''

for testpassed in passedTests:
  passedValidations += "\n" + testpassed
for testpassed in failedTests:
  failedvalidations += "\n" + testpassed

# COMMAND ----------

TestOutputNotebook = "Enrichment Validation Test Output for "+instance
Environment        = "Environment is    : " + environment 
Instance           = "Instance is    : " + instance 
Counts             = "Counts:"
TotalTestCount     = "Total Test Count  : " + str(TotalTestsCount)
passedTestsCount   = "Passed Test Count : " + str(len(passedTests))
Percentage         = "PASS/FAIL Percentage:"
failedTestsCount   = "Failed Test Count : " + str(len(failedTests))
passedPercentage   = "Passed Test Percentage : " + str(passPercent)+"%" 
failedPercentage   = "Failed Test Percentage : " + str(failPercent)+"%"
PassedTestsText    = "\nPassed Tests:"
passedValidations  = passedValidations
FailedTestsText    = "\nFailed Tests:" 
failedvalidations  = failedvalidations

fullTestOutput = "\n".join([TestOutputNotebook,Underline,Environment,Underline,Counts,TotalTestCount,passedTestsCount,failedTestsCount,Underline,Percentage,passedPercentage,failedPercentage,Underline,PassedTestsText,passedValidations,Underline,FailedTestsText,failedvalidations])

print(fullTestOutput)

# COMMAND ----------

dfResult = spark.createDataFrame([("Passed",passedCount),("Failed",failedCount)],["State","Result"])
dfResult.createOrReplaceTempView("resultTable")
display(spark.sql("SELECT * FROM resultTable"))

# COMMAND ----------

dbutils.notebook.exit(fullTestOutput)