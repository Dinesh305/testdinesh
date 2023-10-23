# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# Set the default values 
###########################################################################################################
###               Get the Run time parameters from the Databricks widgets                               ###
###########################################################################################################
dbutils.widgets.text("ParquetFinalPath","", "ParquetFinalPath")
ParquetFinalPath = dbutils.widgets.get("ParquetFinalPath")

dbutils.widgets.text("environment","", "environment")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("RedshiftFinalTable","", "RedshiftFinalTable")
RedshiftFinalTable = dbutils.widgets.get("RedshiftFinalTable")

dbutils.widgets.text("run_type","", "run_type")
run_type = dbutils.widgets.get("run_type")

dbutils.widgets.text("lot_id", "", "lot_id")
lot_id = dbutils.widgets.get("lot_id")
run_type = run_type + ": lot_id=" + str(lot_id)

dbutils.widgets.text("RunTrackingTable", "", "RunTrackingTable")
RunTrackingTable = dbutils.widgets.get("RunTrackingTable")

dbutils.widgets.text("RedshiftDBName", "", "RedshiftDBName")
RedshiftDBName = dbutils.widgets.get("RedshiftDBName")

dbutils.widgets.text("RedshiftPort", "", "RedshiftPort")
RedshiftPort = dbutils.widgets.get("RedshiftPort")

dbutils.widgets.text("RedshiftUser", "", "RedshiftUser")
RedshiftUser = dbutils.widgets.get("RedshiftUser")

dbutils.widgets.text("RedshiftUserPass", "", "RedshiftUserPass")
RedshiftUserPass = dbutils.widgets.get("RedshiftUserPass")

dbutils.widgets.text("prod_switch", "", "prod_switch")
prod_switch = dbutils.widgets.get("prod_switch")

dbutils.widgets.text("jdbc_url","", "jdbc_url")
jdbc_url = dbutils.widgets.get("jdbc_url")

dbutils.widgets.text("RedshiftInstance","", "RedshiftInstance")
RedshiftInstance = dbutils.widgets.get("RedshiftInstance")

dbutils.widgets.text("TempS3Bucket","", "TempS3Bucket")
TempS3Bucket = dbutils.widgets.get("TempS3Bucket")

# COMMAND ----------

RedshiftFinalTable = dict(x.split('=') for x in RedshiftFinalTable.split(','))
print (RedshiftFinalTable)
print ('-------------------------------------------------------------------------------')
# the 1 argu value is for maxsplit in split(demiliter,maxsplit) func, this ensures that first occurrenct of delimiter is the one, that causes the split in the string x
ParquetFinalPath = dict(x.split('=',1) for x in ParquetFinalPath.split(','))
print (ParquetFinalPath)

# COMMAND ----------

# MAGIC %run ../libraries/credentials_for_enrichment

# COMMAND ----------

# MAGIC %run ../libraries/enrich_dq_module

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
###            Read parquet files and get counts                                                        ###
###########################################################################################################
try:
  STEP = 'Read parquet files and get counts'
  final_dfs = {}
  count_dfs = {}
  for k,v in ParquetFinalPath.items():
    final_dfs[str(k)] = spark.read.parquet(v)
    count_dfs[str(k)] = final_dfs[str(k)].count()
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                   Setting the Run Environment for the Redshift table load                           ###
###########################################################################################################
try:
  STEP = "Setting the Run Environment for the Redshift table load"
  check_env = environment.lower()
  load_db_list = []
  if check_env == 'dev' or check_env == 'dev_dsr':
    load_db_list = ['DEV']   
  elif check_env == 'itg' or check_env == 'itg_dsr':
      load_db_list = ['ITG-02']
  elif check_env == 'prod' or check_env == 'prod_dsr':
      if "inklaser" in prod_switch.lower():
        load_db_list = ['PROD-02', 'PROD-04']
      elif "laser" in prod_switch.lower():
        load_db_list = ['PROD-04']
      elif "ink" in prod_switch.lower():
        load_db_list = ['PROD-02']

  # As in Prod we have 2 Database, we need to iterate over them for ITG iteration will be once.
  ###########################################################################################################
  ###                   Setting Parameters for data load into Redshift table in ITG/PROD02/PROD04         ###
  ###########################################################################################################
  for db in load_db_list:
    print (db)
    if db == "DEV":
        rs_env = "dev"
        ## Connection Parameters
        rs_dbname=RedshiftDBName
        rs_user=RedshiftUser
        rs_password= get_rs_env_password(rs_env,rs_user)
        rs_host=RedshiftInstance_dev
        rs_port=port
        rs_sslmode ='require'
        user_name =RedshiftUser
        jdbc_url =get_rs_env_jdbc_url(rs_env,rs_user)
        iam =dev_iam
    #For ITG Ink and Laser ITG-02
    if db == "ITG-02":
      rs_env = "itg-02"
      ## Connection Parameters
      rs_dbname=RedshiftDBName
      rs_user=RedshiftUser
      rs_password= get_rs_env_password(rs_env,rs_user)
      rs_host=RedshiftInstance_itg
      rs_port=port
      rs_sslmode ='require'
      user_name =RedshiftUser
      jdbc_url =get_rs_env_jdbc_url(rs_env,rs_user)
      iam =itg_iam

    #For Prod Ink PROD-02
    if db == "PROD-02":
      rs_env = "prod-02"
      ## Connection Parameters
      rs_dbname=RedshiftDBName
      rs_user=RedshiftUser
      rs_password= get_rs_env_password(rs_env,rs_user)
      rs_host=RedshiftInstance_prod02
      rs_port=port
      rs_sslmode ='require'
      user_name =RedshiftUser
      jdbc_url =get_rs_env_jdbc_url(rs_env,rs_user)
      iam = prod_iam

    #For Prod Laser PROD-04
    if db == "PROD-04":
      rs_env = "prod-04"
      ## Connection Parameters
      rs_dbname=RedshiftDBName
      rs_user=RedshiftUser
      rs_password= get_rs_env_password(rs_env,rs_user)
      rs_host=RedshiftInstance_prod03
      rs_port=port
      rs_sslmode ='require'
      user_name =RedshiftUser
      jdbc_url =get_rs_env_jdbc_url(rs_env,rs_user)
      iam = prod_iam  
    ###########################################################################################################
    ###                   Displaying all the parameters                                                     ###
    ########################################################################################################### 
    print("============================================================================")
    print("RS Instance : {} \nRS User : {} \nRS Port : {} \nRS DBName : {}".format(rs_host,rs_user,rs_port,rs_dbname+' in '+ db))
    print("============================================================================")
    ###########################################################################################################
    RedshiftInstanceInfo      += "Redshift Instance is                    : " + rs_host
    RedshiftRegionNameInfo    += "Redshift Region Name is                 : " + db
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
      bucket_lot_id = TempS3Bucket + lot_id + '/' + v + '/'
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
                       TempS3Bucket).option("dbtable", v).option("aws_iam_role",iam).load()
      count_dw_dfs[str(k)] = dw_dfs[str(k)].count()
      validate_count_dfs[str(k)] = count_dfs[k] - count_dw_dfs[k]
      print (validate_count_dfs)
      if validate_count_dfs[k] == 0:
        validate_result = "Validation for {0} ({1}) in {2} {3}: PASSED (Difference is {4}) file rec count {5} vs table rec count {6}"\
        .format(k,v,db,rs_dbname,stl_load_err_dict[k],count_dfs[k],count_dw_dfs[k])
        passed.append(validate_result)
      elif validate_count_dfs[k] == stl_load_err_dict[k]:
        validate_result = "Validation for {0} ({1}) in {2} {3}: PASSED (Difference is {4}) file rec count {5} vs table rec count {6}"\
        .format(k,v,db,rs_dbname,stl_load_err_dict[k],count_dfs[k],count_dw_dfs[k])
        passed.append(validate_result)
      else:
        validate_result = "Validation for {0} ({1}) in {2} {3} : FAILED file rec count {4} vs table rec count {5}"\
        .format(k,v,db,rs_dbname,count_dfs[k],count_dw_dfs[k])
        failed.append(validate_result)
        
     #########################################################################################################
      ### Validate for run tracking table                                                                     ### 
      ###########################################################################################################
      for retry_tracking_table in range(no_of_retry): 
        try:
           RunTrackingTableDF = sqlContext.read.format("com.databricks.spark.redshift").option("url",jdbc_url).option("tempdir",\
           TempS3Bucket).option("dbtable", RunTrackingTable).option("aws_iam_role",iam).load()
        except Exception as e:
          print(e)
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
        validate_result ="Validation in run tracking table {0} in  {1} {2} for {3} : PASSED. Run Type is {4}".format(RunTrackingTable,db,rs_dbname,k,run_type) 
        passed.append(validate_result)
      else:
        validate_result = "Validation in run tracking table (" + RunTrackingTable + ") in " + db + " " \
                           + rs_dbname + " for " + k +  ": FAILED. Run Type is " + run_type
        failed.append(validate_result)
        
########################################################################################################## 
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