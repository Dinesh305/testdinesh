# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("delta_flags_write","", "delta_flags_write")
delta_flags_write = dbutils.widgets.get("delta_flags_write")

dbutils.widgets.text("email","","email")
email = str(dbutils.widgets.get("email")) 

dbutils.widgets.text("environment","","environment")
environment = str(dbutils.widgets.get("environment")) 

dbutils.widgets.text("dw_source_name","","dw_source_name")
dw_source_name = str(dbutils.widgets.get("dw_source_name"))

# COMMAND ----------

# MAGIC %run ./ent_dq_validation_library

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

###########################################################################################################
###                Create the list of Dictionary Variables to be send on notebook exit                  ###
###########################################################################################################
metric_dict = OrderedDict()
metric_dict["FINALRESULT"] = ''
metric_dict["STATUS"] = 'FAILURE'
metric_dict["STEP_ERROR"] = 'DQ_VALIDATION_START#ERROR'

# COMMAND ----------

passed_test = []
failed_test = []

def compliance_check_status(dq_violation_count,dq_check_type,expected_count = 0):
  
  global passed_test
  global failed_test
  
  result_status = "Expected failed count for {0} is {1} and actual is {2}".format(dq_check_type,expected_count,dq_violation_count)
  
  if dq_violation_count == 0:
    passed_test.append(result_status)
  else:
    failed_test.append(result_status)

# COMMAND ----------

df_enrich_flag = spark.read.format('delta').load(delta_flags_write)
delta_source_name = 'country_postal_ref' #dw_source_name

# COMMAND ----------

################ HIGH LEVEL OVERVIEW OF DQ & DM Checks inplace #########################

# DQ70 : states the flag records having NULL value in particular column
# DQ70 check for columns : country_code, postal_code and place_name
# DQ70 check for candidate key : admin_name1, admin_code1, admin_name2, admin_code2, admin_name3, admin_code3 cadidate key should not be null

# DQ73 : states that if a record wrt a column value does not have expected dateformat then flag it 
# DQ73 check for columns : business_update_date

# DQ9 : states that flag record having duplicate records wrt to a particular candidate key
# DQ9 check for columns : (country_code & postal_code & place_name)
# DQ9 check for columns : (country_code & postal_code & place_name & admin_name1)

# COMMAND ----------

# unique composite key check, i.e. combination of (country_code, postal_code, place_name) is unique - DQ9

def validate_DQ9_rule_composite_key(df_enrich, parquet_file_name):
  
  global failedTest

  try:
    col_name = "country_code, postal_code, place_name"
      
    composite_key_col_name = 'temporary_composite_key'
    # adding composite key column 
    df_enrich = df_enrich.withColumn(composite_key_col_name, F.concat(F.col('country_code'),F.col('postal_code'),F.col("place_name")))
    # doing distinct based on composite key column and counting these distinct records
    df_unique_composite_key_records_count = df_enrich.where(F.col('error_code') == 0).select(composite_key_col_name).distinct().count()
    # counting the number of good records
    df_record_count = df_enrich.where(F.col('error_code') == 0).count()
    
    # checking any violation
    if df_unique_composite_key_records_count == df_record_count :
      dq9_violation_count = 0
    else : 
      dq9_violation_count =  df_record_count - df_unique_composite_key_records_count
    
   
    # status msg which will be appened to either PassedTests or FailedTests based on dm80_voliation_count,
    status_msg_for_dq9 = 'DQ9 validate for col {0} from {1} file'.format(col_name,parquet_file_name)
    # function, to validate pass or fail.
    # ideally violation_count should be equal to zero '0' for it to be appened to passed_tests 
    compliance_check_status(dq9_violation_count,status_msg_for_dq9)
    
  except Exception as e:
    # send status msg for that particular name and dataframe
    failure_msg = "Failed to validate DQ9 rule for col {0} from {1} file.".format(col_name,parquet_file_name)
    # incase of any actual failure, print error {for debugging later}
    print (failure_msg)
    print (e)
    # appending the status msg
    failed_test.append(failure_msg)      
  

try:
  validate_DQ9_rule_composite_key(df_enrich_flag,delta_source_name)
except Exception as e:
  print(e)
  failed_msg ="{0} Failed to validate DQ9 rule as Enrich Flag table is empty/not existing.".format(delta_source_name)
  failed_test.append(failed_msg)

# COMMAND ----------

###########################################################################################################
###                         DQ70 validation for country_code, postal_code and place_name                ###
###########################################################################################################

cols_names = ['country_code', 'postal_code', 'place_name']

def validate_DQ70_rule(df_enrich,parquet_file_name,col_name):
  
  global failedTest
  
  try:
    df_enrich_check_dq70 = df_enrich.withColumn('verify_dq70',DQ70_null_check(col(col_name),col_name,col('error_list')))
    # gets the col-record count which has verify_dq1 col value False {more abstractly anything other than True} 
    # those are the record which are not modified & not flagged --> faulty records
    dq70_violation_count = df_enrich_check_dq70.filter(df_enrich_check_dq70.verify_dq70 != True).count()
    # status msg which will be appened to either PassedTests or FailedTests based on dq1_voliation_count,
    status_msg_for_dq70 = 'DQ70 validate for col {0} from {1} file'.format(col_name,parquet_file_name)
    # function, to validate pass or fail.
    # ideally violation_count should be equal to zero '0' for it to be appened to PassesTests 
    compliance_check_status(dq70_violation_count,status_msg_for_dq70)
  except Exception as e:
    # send status msg for that particular name and dataframe
    failure_msg = "Failed to validate DQ70 rule for col {0} from {1} file.".format(col_name,parquet_file_name)
    # incase of any actual failure, print error {for debugging later}
    print (failure_msg)
    print (e)
    # appending the status msg
    failed_test.append(failure_msg)  
    
for col_name in cols_names:     
  try:
    STEP = 'DQ70 Validation for {0}'.format(col_name)
    validate_DQ70_rule(df_enrich_flag,delta_source_name,col_name)
  except Exception as e:
    print(e)
    failed_msg ="{0}-Failed to validate DQ70 rule as Enrich Flag table is empty/not existing.".format(delta_source_name)
    failed_test.append(failed_msg)

# COMMAND ----------

###############################################################################################################################################
###                         DQ70 validation for admin_name1, admin_code1, admin_name2, admin_code2, admin_name3, admin_code3                ###
###############################################################################################################################################

def validate_DQ70_rule_candidate_key(df_enrich,parquet_file_name):
  
  global failedTest
  
  try:
    col_name = 'admin_name1, admin_code1, admin_name2, admin_code2, admin_name3, admin_code3'
    composite_key_column = 'temporary_composite_key'
    df_enrich = df_enrich.withColumn(composite_key_column,F.concat(F.coalesce(F.col('admin_name1'),F.lit("")),F.coalesce(F.col('admin_code1'),F.lit("")),F.coalesce(F.col('admin_name2'),F.lit("")),F.coalesce(F.col('admin_code2'),F.lit("")),F.coalesce(F.col('admin_name3'),F.lit("")),F.coalesce(F.col('admin_code3'),F.lit(""))))
    df_enrich_check_dq70 = df_enrich.withColumn('verify_dq70',DQ70_null_check(F.col(composite_key_column),composite_key_column,F.col('error_list')))
    # gets the col-record count which has verify_dq1 col value False {more abstractly anything other than True} 
    # those are the record which are not modified & not flagged --> faulty records
    dq70_violation_count = df_enrich_check_dq70.filter(df_enrich_check_dq70.verify_dq70 != True).count()
    # status msg which will be appened to either PassedTests or FailedTests based on dq1_voliation_count,
    status_msg_for_dq70 = 'DQ70 validate for col {0} from {1} file'.format(col_name,parquet_file_name)
    # function, to validate pass or fail.
    # ideally violation_count should be equal to zero '0' for it to be appened to PassesTests 
    compliance_check_status(dq70_violation_count,status_msg_for_dq70)
  except Exception as e:
    # send status msg for that particular name and dataframe
    failure_msg = "Failed to validate DQ70 rule for col {0} from {1} file.".format(col_name,parquet_file_name)
    # incase of any actual failure, print error {for debugging later}
    print (failure_msg)
    print (e)
    # appending the status msg
    failed_test.append(failure_msg)  

try:
  STEP = 'DQ70 Validation for serial number.'
  validate_DQ70_rule_candidate_key(df_enrich_flag,delta_source_name)
except Exception as e:
  print(e)
  failed_msg ="{0}-Failed to validate DQ70 rule as Enrich Flag table is empty/not existing.".format(delta_source_name)
  failed_test.append(failed_msg)

# COMMAND ----------

Underline = "---------------------------------------------------------------------------------------"
passedValidations = ''
failedvalidations = ''

TestOutputNotebook = 'Reference DQ Validation Results'

passedCount = len(passed_test)
failedCount = len(failed_test)
Counts = "Counts:"
TotalTestCount   = "Total Test Count  : " + str(passedCount + failedCount)
passedTestsCount = "Passed Test Count : " + str(passedCount)
failedTestsCount = "Failed Test Count : " + str(failedCount)

PassedTests = "Passed Tests:"
for testpassed in passed_test:
  passedValidations += "\n" + testpassed
  
FailedTests = "Failed Tests:"
for testfailed in failed_test:
  failedvalidations += "\n" + testfailed

fullTestOutput = "\n".join([TestOutputNotebook,Underline, Counts, TotalTestCount, passedTestsCount, failedTestsCount, Underline, PassedTests, passedValidations, Underline, FailedTests, failedvalidations])

print (fullTestOutput)


# COMMAND ----------


dfResult = spark.createDataFrame([("Passed",passedCount),("Failed",failedCount)],["State","Result"])
dfResult.createOrReplaceTempView("resultTable")
display(spark.sql("SELECT * FROM resultTable"))

# COMMAND ----------

###########################################################################################################
###  Metrics for Logging the Stats and creating the Dictionary to be send as output of run              ###
###########################################################################################################
try:
  DQ_STEP='Create final metrics.'
  metric_dict["FINALRESULT"] = fullTestOutput
  metric_dict["STATUS"] = 'SUCCESS'
  metric_dict["STEP"] = 'END'
  metric_dict["ERROR"] = 'NO_ERROR'
  metric_json = json.dumps(metric_dict)
###########################################################################################################
except Exception as e:
  metric_dict["STEP"] = DQ_STEP
  metric_dict["ERROR"] = str(e)[:200]
  metric_json = json.dumps(metric_dict)
  print(metric_json)
  dbutils.notebook.exit(metric_json)

# COMMAND ----------

dbutils.notebook.exit(metric_json)

# COMMAND ----------

