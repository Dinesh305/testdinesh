# Databricks notebook source
import boto3
from datetime import datetime, timedelta
import json

conn = boto3.client('s3')

# COMMAND ----------

dbutils.widgets.text("metrics_file_location","", "metrics_file_location")
metrics_file_location = dbutils.widgets.get("metrics_file_location")

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

###########################################################################################################
###                Create the list of Dictionary Variables to be send on notebook exit                  ###
###########################################################################################################
from collections import OrderedDict
import datetime
import json
import re
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
metric_dict = OrderedDict()
metric_dict["STATUS"] = 'FAILURE'
metric_dict["STEP_ERROR"] = 'Metrics_Validation_START#NO_ERROR'

# COMMAND ----------

###########################################################################################################
###                Error handling routine                                                               ###
###########################################################################################################
def exit_notebook(step,error):
  """
  exiting the notebook, incase of any exception or error during execution of the script
  """
  global metric_dict
  
  metric_dict["STEP_ERROR"] = step + "#" + str(error)[:200]
  metric_json = json.dumps(metric_dict)
  print(metric_json)
  dbutils.notebook.exit(metric_json)

# COMMAND ----------

###########################################################################################################
###                Parse Metrics file location                                                          ###
###########################################################################################################
try:
  STEP = 'Parse Metrics file location.' 
  tokens = metrics_file_location.split('/')
  bucket_name = tokens.pop(2)
  prefix_path = '/'.join(tokens[2:len(tokens)-1])
  prefix_path = prefix_path + '/'
  upload_file_name = tokens[-1]
  
  validationMsg = []
  failedTestCount = 0

  print (bucket_name)
  print (prefix_path)
  print (upload_file_name)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                List the keys in Metrics for validation                                              ###
###########################################################################################################
try:
  STEP = 'List the keys in Metrics for validation.' 
  expected_keys = ['enrich_job_name', 'enrich_source_type', 'enrich_job_id', 'enrich_run_id', 'enrich_processing_date', 'enrich_start_time', 'enrich_end_time', 'enrich_workflow_runtime', 'enrich_app_runtime', 'enrich_workflow_step', 'enrich_bat_runtime', 'enrich_dq_test_runtime', 'enrich_dq_summary_runtime', 'enrich_pre_check_rs_runtime', 'enrich_dw_load_runtime', 'enrich_dw_test_runtime', 'enrich_input_size_bytes', 'enrich_output_size_bytes', 'enrich_job_status', 'enrich_environment', 'enrich_number_of_instances', 'enrich_number_of_spot_instances', 'enrich_instance_type', 'enrich_databricks_runtime_version', 'enrich_python_version', 'enrich_package_version', 'enrich_step_error', 'enrich_final_output', 'enrich_lot_id', 'enrich_total_error_count', 'enrich_total_error_count_percent', 'enrich_total_inc_count', 'enrich_total_mod_count', 'enrich_total_mod_count_percent', 'enrich_velocity_in_records_per_second', 'enrich_total_inc_hist_count', 'enrich_good_percentage', 'enrich_total_good_count', 'enrich_total_final_count','enrich_total_intermediate_count', 'enrich_dm14_error_count', 'enrich_dm14_error_percentage', 'enrich_dm15_error_count', 'enrich_dm15_error_percentage', 'enrich_dm16_error_count', 'enrich_dm16_error_percentage', 'enrich_dm17_error_count', 'enrich_dm17_error_percentage', 'enrich_dm19_error_count', 'enrich_dm19_error_percentage', 'enrich_dm21_error_count', 'enrich_dm21_error_percentage', 'enrich_dm22_error_count', 'enrich_dm22_error_percentage', 'enrich_dm24_error_count', 'enrich_dm24_error_percentage', 'enrich_dm27_error_count', 'enrich_dm27_error_percentage', 'enrich_dm28_error_count', 'enrich_dm28_error_percentage', 'enrich_dm29_error_count', 'enrich_dm29_error_percentage', 'enrich_dm30_error_count', 'enrich_dm30_error_percentage', 'enrich_dm32_error_count', 'enrich_dm32_error_percentage', 'enrich_dm37_error_count', 'enrich_dm37_error_percentage', 'enrich_dm39_error_count', 'enrich_dm39_error_percentage', 'enrich_dm40_error_count', 'enrich_dm40_error_percentage', 'enrich_dm43_error_count', 'enrich_dm43_error_percentage', 'enrich_dm44_error_count', 'enrich_dm44_error_percentage', 'enrich_dm46_error_count', 'enrich_dm46_error_percentage', 'enrich_dm4_modlist_count', 'enrich_dm4_modlist_percentage', 'enrich_dm50_error_count', 'enrich_dm50_error_percentage', 'enrich_dm53_error_count', 'enrich_dm53_error_percentage', 'enrich_dm54_error_count', 'enrich_dm54_error_percentage', 'enrich_dm55_error_count', 'enrich_dm55_error_percentage', 'enrich_dm56_error_count', 'enrich_dm56_error_percentage', 'enrich_dm57_error_count', 'enrich_dm57_error_percentage', 'enrich_dm58_error_count', 'enrich_dm58_error_percentage', 'enrich_dm59_error_count', 'enrich_dm59_error_percentage', 'enrich_dm60_error_count', 'enrich_dm60_error_percentage', 'enrich_dm61_error_count', 'enrich_dm61_error_percentage', 'enrich_dm62_error_count', 'enrich_dm62_error_percentage', 'enrich_dm63_error_count', 'enrich_dm63_error_percentage', 'enrich_dm64_error_count', 'enrich_dm64_error_percentage', 'enrich_dm65_error_count', 'enrich_dm65_error_percentage', 'enrich_dq10_error_count', 'enrich_dq10_error_percentage', 'enrich_dq11_error_count', 'enrich_dq11_error_percentage', 'enrich_dq13_error_count', 'enrich_dq13_error_percentage', 'enrich_dq18_error_count', 'enrich_dq18_error_percentage', 'enrich_dq1_error_count_val', 'enrich_dq1_error_percentage', 'enrich_dq34_error_count', 'enrich_dq34_error_percentage', 'enrich_dq35_error_count', 'enrich_dq35_error_percentage', 'enrich_dq3_error_count', 'enrich_dq3_error_percentage', 'enrich_dq5_error_count', 'enrich_dq5_error_percentage', 'enrich_dq9_error_count', 'enrich_dq9_error_percentage', 'enrich_total_final_size','enrich_total_intermediate_size', 'enrich_total_inc_size', 'enrich_total_flags_size','DB_job_name', 'Job_ID', 'Run_ID', 'Run_URL', 'cadence', 'job_health_status', 'trigger_type', 'app_type']

  keys1 = ["enrich_app_runtime", "enrich_bat_runtime", "enrich_dq_summary_runtime", "enrich_dq_test_runtime", "enrich_dw_load_runtime", "enrich_dw_test_runtime", "enrich_pre_check_rs_runtime", "enrich_workflow_runtime", "enrich_job_status", "enrich_total_flags_size", "enrich_total_inc_hist_count"]

  vital_keys = ['enrich_app_runtime', 'enrich_bat_runtime', 'enrich_dq_summary_runtime', 'enrich_dq_test_runtime',
 'enrich_dw_load_runtime', 'enrich_dw_test_runtime', 'enrich_end_time', 'enrich_environment',
 'enrich_final_output', 'enrich_good_percentage', 'DB_job_name', 'Job_ID', 'Run_ID', 'Run_URL', 'cadence', 'enrich_job_name',
 'enrich_job_status', 'enrich_lot_id', 'job_health_status', 'trigger_type',
 'enrich_pre_check_rs_runtime', 'enrich_processing_date', 'enrich_source_type', 'enrich_start_time',
 'enrich_step_error', 'enrich_total_error_count', 'enrich_total_error_count_percent',  'enrich_total_final_count','enrich_total_final_size','enrich_total_intermediate_count',
 'enrich_total_intermediate_size', 'enrich_total_flags_size', 'enrich_total_good_count', 'enrich_total_inc_count',
 'enrich_total_inc_hist_count', 'enrich_total_inc_size', 'enrich_total_mod_count', 'enrich_total_mod_count_percent',
 'enrich_velocity_in_records_per_second', 'enrich_workflow_runtime', 'enrich_workflow_step', 'app_type']

  # vital_keys2 = ['enrich_input_size_bytes', 'enrich_instance_type', 'enrich_job_id', 'enrich_python_version', 'enrich_run_id']
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
#     fetchs file or object as string
###########################################################################################################
# conn = get_boto3_conn ()

def get_file_content(bucket_name, prefix_path, file_name):
    global conn
    key = prefix_path + file_name 
    response = conn.get_object(Bucket = bucket_name, Key = key)
    body = response.get('Body')
    file_in_bytes = body.read()
    file = file_in_bytes.decode('utf-8')
    return file
  
###########################################################################################################
### Validation starts
###########################################################################################################
try:
  STEP = 'Validation starts.' 
  file = get_file_content(bucket_name, prefix_path,upload_file_name)
  f_dict = json.loads(file)
  final_dict = dict([(str(k), v) for k, v in f_dict.items()])
  
  validationMsg.append("-------------List of keys not following kibana dashboard guidelines------------")
  validationMsg.append(" ")
  
  for k, v in final_dict.items():
    if k not in expected_keys:
      if k[len(k)-11:] != 'error_count' and k[len(k)-16:] != 'error_percentage':
        validationMsg.append(k)
        failedTestCount = failedTestCount + 1
        
  validationMsg.append("-------------List of missing vital keys----------------------------------------")
  validationMsg.append(" ")
  list_keys = []
  for k, v in final_dict.items():
    list_keys.append(k)
    
  for k in vital_keys:
    if k not in list_keys:
      validationMsg.append(k)
      failedTestCount = failedTestCount + 1
        
  validationMsg.append("-------------Run Time Metrics--------------------------------------------------")
  test_run_time = 0
  for k, v in final_dict.items():
    if k == 'enrich_job_status':
      if v not in ['SUCCESS','FAILED']:
        validationMsg.append('check for proper job status as it is not SUCCESS')
        failedTestCount = failedTestCount + 1
    if k in keys1:
      if k in ['enrich_pre_check_rs_runtime','enrich_bat_runtime','enrich_dq_test_runtime','enrich_dq_summary_runtime','enrich_dw_test_runtime'] :
        test_run_time = test_run_time + v
      if k in ['enrich_app_runtime','enrich_workflow_runtime','enrich_dw_load_runtime','enrich_total_flags_size','enrich_total_inc_hist_count'] :
        tmp_msg = k + (100-len(k))*" " +": " + str(v)
        validationMsg.append(tmp_msg)
      else:
        tmp_msg = k + (100-len(k))*" " +": " + str(v)
        validationMsg.append(tmp_msg)
        
  tmp_msg = 'test_run_time ' + (100-len(k))*' ' + str(test_run_time)     
  validationMsg.append(tmp_msg)
  validationMsg.append("-------------Metrics other than error and Mod---------------------------------")
  for k, v in final_dict.items():
    if k[len(k)-11:] != 'error_count' and k[len(k)-16:] != 'error_percentage' and k !='enrich_final_output' and k not in keys1:
      tmp_msg = k + (100-len(k))*" " +": " + str(v)
      validationMsg.append(tmp_msg)
  validationMsg.append("-------------Error Percentages------------------------------------------------")
  for k, v in final_dict.items(): 
    if k[len(k)-16:] == 'error_percentage' and k not in keys1:
      tmp_msg = k + (100-len(k))*" " +": " + str(v)
      validationMsg.append(tmp_msg)
  validationMsg.append("-------------Error counts-----------------------------------------------------")
  for k, v in final_dict.items(): 
    if k[len(k)-11:] == 'error_count' and k not in keys1:
      tmp_msg = k + (100-len(k))*" " +": "+ str(v)
      validationMsg.append(tmp_msg)
  validationMsg.append("-------------Summary dump--------------------------------------------------")    
  for k, v in final_dict.items(): 
    if k == 'enrich_final_output':
      tmp_msg = k + (100-len(k))*" " +": " + str(v)
      validationMsg.append(tmp_msg)
        
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

message = ''
Underline = "---------------------------------------------------------------------------------------"
TestOutputNotebook = "Metrics Validation Results"
Counts = "Counts:"

failedTestsCount = "Failed Test Count : " + str(failedTestCount)
for check in validationMsg:
  message += "\n" + check

fullTestOutput = "\n".join([TestOutputNotebook,Underline, Counts, failedTestsCount, Underline, message])
print (fullTestOutput)

# COMMAND ----------

###########################################################################################################
###  Metrics for Logging the Stats and creating the Dictionary to be send as output of run              ###
###########################################################################################################
try:
  STEP='Create final metrics.'
  metric_dict["FINALRESULT"] = fullTestOutput
  metric_dict["STATUS"] = 'PASSED' if failedTestCount == 0 else 'FAILED'
  metric_dict["STEP_ERROR"] = 'Metrics_Validation_END#NO_ERROR'
  metric_json = json.dumps(metric_dict)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

dbutils.notebook.exit(metric_json)

# COMMAND ----------

