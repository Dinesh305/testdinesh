# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# Set the default values 
###########################################################################################################
###               Get the Run time parameters from the Databricks widgets                               ###
###########################################################################################################
dbutils.widgets.text("deltaFinalPath","", "deltaFinalPath")
ParquetFinalPath = dbutils.widgets.get("deltaFinalPath")

dbutils.widgets.text("environment","", "environment")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("RedshiftFinalTable","", "RedshiftFinalTable")
RedshiftFinalTable = dbutils.widgets.get("RedshiftFinalTable")

dbutils.widgets.text("run_type","", "run_type")
run_type = dbutils.widgets.get("run_type")

dbutils.widgets.text("lot_id", "", "lot_id")
lot_id = dbutils.widgets.get("lot_id")
run_type = run_type + ": lot_id=" + str(lot_id)

dbutils.widgets.text("envt_dbuser", "")
envt_dbuser = dbutils.widgets.get("envt_dbuser")

# COMMAND ----------

import ast

# COMMAND ----------

RedshiftFinalTable = dict(x.split('=') for x in RedshiftFinalTable.split(','))
print (RedshiftFinalTable)
print ('-------------------------------------------------------------------------------')
# the 1 argu value is for maxsplit in split(demiliter,maxsplit) func, this ensures that first occurrenct of delimiter is the one, that causes the split in the string x
ParquetFinalPath = dict(x.split('=',1) for x in ParquetFinalPath.split(','))
print (ParquetFinalPath)

# COMMAND ----------

# MAGIC %run ../libraries/ent_dq_module

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

###########################################################################################################
###                Create the list of Dictionary Variables to be send on notebook exit                  ###
###########################################################################################################
metric_dict = OrderedDict()
metric_dict["STATUS"] = 'FAILURE'
metric_dict["STEP_ERROR"] = 'DW_VALIDATION_START#NO_ERROR'

# COMMAND ----------

###########################################################################################################
###              Initalize variables                                                                    ###
###########################################################################################################

import traceback
import time
# load results strings default - assign to passed if successful
passed = []
failed = []
no_of_retry  = 3
validate_count = len(RedshiftFinalTable)
RedshiftInstanceInfo = ''
RedshiftRegionNameInfo = ''

# COMMAND ----------

###########################################################################################################
###            Read delta files and get counts                                                        ###
###########################################################################################################
try:
  deltaFinalPath='s3://ref-data-itg/country_postal_ref/'
  deltaFinalPath= deltaFinalPath+ + 'country_postal_ref/'
  STEP = 'Read delta files and get counts'
  final_dfs = {}
  count_dfs = {}
  for k,v in deltaFinalPath.items():
    final_dfs[str(k)] = spark.read.delta(v)
    count_dfs[str(k)] = final_dfs[str(k)].count()
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

try:
  envt_dbuser_list = envt_dbuser.split(',')
  for evnt_db in envt_dbuser_list:
    creds_str = dbutils.notebook.run("../libraries/credentials_for_enterprise", 600, {"envt_dbuser":evnt_db})
    creds = ast.literal_eval(creds_str)

    rs_env             = creds['rs_env']
    rs_dbname          = creds['rs_dbname']
    rs_user            = creds['rs_user']
    rs_password        = creds['rs_password']
    rs_host            = creds['rs_host']
    rs_port            = creds['rs_port']
    rs_sslmode         = creds['rs_sslmode']
    user_name          = creds['rs_user']
    jdbc_url           = creds['rs_jdbc_url']
    iam                = creds['aws_iam']
    RunTrackingTable   = creds['run_tracking_table']
    tempS3Dir          = creds['tmp_dir']

    ###########################################################################################################
    ###                   Displaying all the parameters                                                     ###
    ########################################################################################################### 
    print("============================================================================")
    print("RS Instance : {} \nRS User : {} \nRS Port : {} \nRS DBName : {}".format(rs_host,rs_user,rs_port,rs_dbname+' in '+ rs_env))
    print("============================================================================")
    ###########################################################################################################
    RedshiftInstanceInfo      += "Redshift Instance is                    : " + rs_host
    RedshiftRegionNameInfo    += "Redshift Region Name is                 : " + rs_env
    stl_load_err_dict = {}
    dict_str = ''
    ###########################################################################################################
    ### Loop through individual table per region for validation                                             ###
    ###########################################################################################################
    for k,v in RedshiftFinalTable.items(): 
      ###########################################################################################################
      ###  Gathers stl_load_error count to check for any error while loading to DW                            ### 
      ###########################################################################################################
      STEP = 'Gathers stl_load_error count to check for any error while loading to DW'
      dict_str = "{'" + k + "':0}"
      print (dict_str)
      new_dict = eval(dict_str)
      stl_load_err_dict.update(new_dict)
      bucket_lot_id = tempS3Dir + lot_id + '/' + v + '/'
      get_id, rs_load_errors = get_rs_errors(rs_dbname, rs_user, rs_password\
                               , rs_host, rs_port, bucket_lot_id)

      print ("rs_load_errors: ", rs_load_errors)
      print ("get_id :",get_id)
      if get_id:
        num_failures = len(rs_load_errors)
        stl_load_err_dict[k] = num_failures
      ###########################################################################################################
      ### Read DW table and check if counts are matching with parquet.                                        ### 
      ###########################################################################################################
      STEP = 'Read DW table and check if counts are matching with parquet'
      dw_dfs = {}
      count_dw_dfs = {}
      validate_count_dfs = {}
      dw_dfs[str(k)] = sqlContext.read.format("com.databricks.spark.redshift").option("url",jdbc_url).option("tempdir",\
                       tempS3Dir).option("dbtable", v).option("aws_iam_role",iam).load()
      count_dw_dfs[str(k)] = dw_dfs[str(k)].count()
      validate_count_dfs[str(k)] = count_dfs[k] - count_dw_dfs[k]
      print (validate_count_dfs)
      if validate_count_dfs[k] == 0:
        validate_result = "Validation for " + k + " (" + v + ") in " + rs_env + " " + rs_dbname + ": PASSED "\
                         + "file rec count "+ str(count_dfs[k]) + " vs table rec count " + str(count_dw_dfs[k]) 
        passed.append(validate_result)
      elif validate_count_dfs[k] == stl_load_err_dict[k]:
        validate_result = "Validation for " + k + " (" + v + ") in " + rs_env + " " + rs_dbname + \
                          ":PASSED (Difference is "+ stl_load_err_dict[k] + ") "\
                          + "file rec count "+ str(count_dfs[k]) + " vs table rec count " + str(count_dw_dfs[k])
        passed.append(validate_result)
      else:
        validate_result = "Validation for " + k + " (" + v + ") in " + rs_env + " " + rs_dbname + ": FAILED "\
                          + "file rec count "+ str(count_dfs[k]) + " vs table rec count " + str(count_dw_dfs[k])
        failed.append(validate_result)

      ###########################################################################################################
      ### Validate for run tracking table                                                                     ### 
      ###########################################################################################################
      for retry_tracking_table in range(no_of_retry): 
        try:
           RunTrackingTableDF = sqlContext.read.format("com.databricks.spark.redshift").option("url",jdbc_url).option("tempdir",\
           tempS3Dir).option("dbtable", RunTrackingTable).option("aws_iam_role",iam).load()
        except:
          tb = traceback.format_exc()
          exception = "Exception on RunTrackingTable Redshift connection. Stack Trace is: " + '\n' + str(tb)
          time.sleep(5) # 5 seconds delay
        else:
          break

      run_tracking_table_data = ''

      RunTrackingTableDF.createOrReplaceTempView("temp_run_tracking_table")
      query =  "select count (*) as lot_id_count from temp_run_tracking_table where run_type like '%"\
                +run_type+"%' and run_type like '%" + lot_id + "'" 
      print (query)
      run_tracking_table_data = (spark.sql(query))
      count_lot_id = list(map(lambda x:x.lot_id_count,run_tracking_table_data.collect()))[0]
      if count_lot_id == validate_count:
        validate_result = "Validation in run tracking table (" + RunTrackingTable + ") in " + rs_env + " " \
                           + rs_dbname + " for " + k + ": PASSED. Run Type is " + run_type 
        passed.append(validate_result)
      else:
        validate_result = "Validation in run tracking table (" + RunTrackingTable + ") in " + rs_env + " " \
                           + rs_dbname + " for " + k +  ": FAILED. Run Type is " + run_type
        failed.append(validate_result)
########################################################################################################### 
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###  Create final validation report                                                                     ###
###########################################################################################################

try:
  STEP = 'Create final validation report'
  passedValidations = ''
  failedvalidations = ''

  TestOutputNotebook        = "Enrichment Redshift Validation Test Output"
  passedCount = len(passed)
  failedCount = len(failed)
  Counts = "Counts:"
  TotalTestCount   = "Total Test Count  : " + str(passedCount + failedCount)
  passedTestsCount = "Passed Test Count : " + str(passedCount)
  failedTestsCount = "Failed Test Count : " + str(failedCount)
  PassedTests = "Passed Tests:"
  for i in passed:
    passedValidations += "\n\n" + i
    
  FailedTests = "Failed Tests:"
  for i in failed:
    failedvalidations += "\n\n" + i

  EnvironmentVal            = "Environment is                          : %s" %environment
  AwsIAMRoleInfo            = "AWS IAM Role is                         : %s" %iam
  RedshiftUserInfo          = "Redshift User Info is                   : %s" %rs_user 
  RedshiftDBNameInfo        = "Redshift DB Name is                     : %s" %rs_dbname
  RunTrackingTableInfo      = "RunTrackingTable Name is                : %s" %RunTrackingTable
  RunTrackConnectionRetry   = "Connct Attempt to RunTrackingTable Done : %s" %str(no_of_retry)
  FinalConnectionRetry      = "Connection Attempts to Final Table Done : %s" %str(no_of_retry)
  ParquetFinalPathInfo      = "Parquet Final info                      : %s" %ParquetFinalPath

  Underline                 = "---------------------------------------------------------------------------------------"

  fullTestResult            = "\n".join([TestOutputNotebook,EnvironmentVal,AwsIAMRoleInfo,RedshiftRegionNameInfo,RedshiftInstanceInfo,\
                              RedshiftUserInfo,RedshiftDBNameInfo,RunTrackingTableInfo,RunTrackConnectionRetry,FinalConnectionRetry,\
                              ParquetFinalPathInfo,Underline,Counts, TotalTestCount, passedTestsCount, failedTestsCount, Underline,\
                              PassedTests, passedValidations, Underline, FailedTests, failedvalidations,Underline])
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

print (fullTestResult)

# COMMAND ----------

###########################################################################################################
###  Metrics for Logging the Stats and creating the Dictionary to be send as output of run              ###
###########################################################################################################
try:
  STEP='Create final metrics.'
  if 'Failed Test Count : 0' in fullTestResult:
    metric_dict["STATUS"] = 'SUCCESS'
    
  metric_dict["STEP_ERROR"] = 'DW_VALIDATION_END#NO_ERROR'
  metric_dict["MESSAGE"] = fullTestResult
  
  metric_json = json.dumps(metric_dict)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

dbutils.notebook.exit(metric_json)