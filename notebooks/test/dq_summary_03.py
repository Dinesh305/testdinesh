# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

dbutils.widgets.text("delta_flags_write","", "delta_flags_write")
delta_flags_write = dbutils.widgets.get("delta_flags_write")

dbutils.widgets.text("environment","","environment")
environment = str(dbutils.widgets.get("environment"))

dbutils.widgets.text("email","","email")
email = str(dbutils.widgets.get("email"))

dbutils.widgets.text("required_dq_checks","","required_dq_checks")
required_dq_checks = str(dbutils.widgets.get("required_dq_checks"))

dbutils.widgets.text("file_list","","file_list")
file_list = str(dbutils.widgets.get("file_list"))

dbutils.widgets.text("app","","app")
app = str(dbutils.widgets.get("app"))

# COMMAND ----------

# Convert file_list from string to LIST
if file_list == '':
  file_list = []
else:
  file_list = list(file_list.split(','))

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
metric_dict["STEP_ERROR"] = 'DQ_SUMMARY_START#NO_ERROR'

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

df1=spark.read.format('delta').load(delta_flags_write)

# COMMAND ----------

###########################################################################################################
###                Read the Mod List and DQ list for Sales and Inventory Dataframe                     ###
###########################################################################################################
try:
  STEP = 'Getting the Data Files.' 

  schema = StructType([
           StructField("error_list", StringType(), True), 
           StructField("mod_list", StringType(), True)
           ])

  df_enrich = spark.createDataFrame([], schema)
  if file_list == []:
    df_enrich = df_enrich.union(spark.read.format('delta').load(delta_flags_write).select("error_list", "mod_list"))

  else:
    for file in file_list:
      df_enrich = df_enrich.union(spark.read.format('delta').load(delta_flags_write + file).select("error_list", "mod_list"))
    
  df_enrich.createOrReplaceTempView("temp_guid")
  df_enrich_count = df_enrich.count()
  FinalColumnsFromParquet = df_enrich.columns
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                Getting the Modlist Count and Errorlist                                              ###
###########################################################################################################
try:
  STEP='Check DQ and DM Counts.'
  
  dq_ErrorList = spark.sql("select error_list from temp_guid where error_list LIKE '%DQ%'")
  dq_ErrorListCount = dq_ErrorList.count()
  dq_ErrorListPercentage = float((dq_ErrorListCount*100.00))/(df_enrich_count)
  dq_ErrorListPercentageVal = round(dq_ErrorListPercentage, 2)

  if "mod_list" in df_enrich.columns:
    dq_ModList = spark.sql("select mod_list from temp_guid where mod_list LIKE '%DM%'")
  else:
    dq_ModList = spark.sql("select field_modification_list from temp_guid where field_modification_list RLIKE '%DM%'")
  dq_ModListCount = dq_ModList.count()
  dq_ModListPercentage = float((dq_ModListCount*100.00))/(df_enrich_count)
  dq_ModListPercentageVal = round(dq_ModListPercentage, 2)

  dqlCleanRecordsCount = df_enrich_count-dq_ErrorListCount
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                Running the Dictionary Map Path                                                      ###
###########################################################################################################
try:
  STEP= 'Running the Dictionary Map.'
  DQDictionaryMapPath  = "DQDictionaryMap"
  DQDictionary = dbutils.notebook.run(
        DQDictionaryMapPath,
        36000,
        arguments = {})
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                Defining the Variables required for DQ Summary                                       ###
###########################################################################################################
try:
  STEP='Defining the Variables.'
  DQValues = eval(DQDictionary)

  finalSummary = []
  finalSummaryDM = []
  errorCount = "Record Count"
  comma = ", "
  colon = " : "
  dq_ErrorList = []
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                Defining the Required functions for DQ Summary                                       ###
###########################################################################################################
def CalculateDQPercentage(sqlQuery,dqTypeVal,cnt):
  dqErrorCount = spark.sql(sqlQuery).count()
  if cnt == 0:
    dqErrorPercentage = 0
  else:
    dqErrorPercentage = (dqErrorCount*100.00)/(cnt)
  dqPercentageVal = str(round(dqErrorPercentage, 2)) + "%"
  percentage = "Percentage of total DQ Failure Count" + "(" + str(cnt) + ")"
  
  dq_count = (dqTypeVal + comma + errorCount + colon + str(dqErrorCount) + comma + percentage + colon + str(dqPercentageVal))
#   print (dq_count)
  return (dq_count, dqErrorCount, dqErrorPercentage)

# COMMAND ----------

###########################################################################################################
###                Check if the DQ and DM is Required for a Deliverable from Workflow Params            ###
###########################################################################################################
try:
  STEP='Checking the DM and DQ which require Validations.'
  dq_check_list = []
  for i in required_dq_checks.split(","):
    dq_check_list.append(i.strip().upper())
    
  for check in dq_check_list:
    if check[0:2] == 'DQ':
      dqCheck = "'" + check + "(?![0-9])'"
      sqlQuery =  "select error_list from temp_guid where error_list RLIKE %s" %dqCheck
      dq_stats, Error_Count, Error_Percentage = CalculateDQPercentage(sqlQuery,check,dq_ErrorListCount)
      finalSummary.append(dq_stats)
      metric_dict[check +"_error_count"]  = Error_Count
      metric_dict[check +"_error_percentage"] = Error_Percentage
    elif check[0:2] == 'DM':  
      dqCheck = "'" + check + "(?![0-9])'"
      sqlQuery =  "select mod_list from temp_guid where mod_list RLIKE %s" %dqCheck
      dq_stats, Error_Count, Error_Percentage = CalculateDQPercentage(sqlQuery,check,dq_ModListCount)
      finalSummaryDM.append(dq_stats)
      metric_dict[check +"_error_count"]  = Error_Count
      metric_dict[check +"_error_percentage"] = Error_Percentage
    else:
      print ("Not a proper DM or DQ in required_dq_checks: ",check)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                The below script will give us the Counts and Percentage of DQ and DM                 ###
###########################################################################################################
try:
  STEP = 'Creating the metrics.'

  sourceDirectoryPath  = str(delta_flags_write).rsplit('/',0)[0]
#   prefix = "xref_enrichment_errormetrics_"
  summaryStartTime = '{:%Y-%m-%d %H:%M:%S.%f}'.format(datetime.datetime.now())[:-4]
  
  STEP = 'Get the total Counts of Records.'
  #errorSummaryData = {}
  metric_dict["total_final_count"] = df_enrich_count
  metric_dict["total_error_count"] = dq_ErrorListCount
  metric_dict["total_error_count_percent"] = dq_ErrorListPercentageVal
  metric_dict["total_mod_count"] = dq_ModListCount
  metric_dict["total_mod_count_percent"] = dq_ModListPercentageVal
  metric_dict["total_good_count"] = dqlCleanRecordsCount
  
  STEP = 'Run commands to get the count and Percentage for DM and DQ.'
  metric_dict["job_name"] = app
  metric_dict["processing_date"] = summaryStartTime
  metric_dict["environment"] = environment

  errorSummaryMetrics = str(json.dumps(metric_dict, sort_keys=True) + "\n")
#   print (errorSummaryMetrics)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)    

# COMMAND ----------

###########################################################################################################
###                Printing the final Summary Statistics and Creating the final JSON                    ###
###########################################################################################################
try:
  errorSummary = ''  
  for dqErrors in finalSummary:
    errorSummary +=  dqErrors + "\n"

  dmSummary = ''
  for dm in finalSummaryDM:
    dmSummary += dm + "\n"

  sourceDirectoryPath           = str(delta_flags_write).rsplit('/',1)[0]
  Underline                     = "---------------------------------------------------------------------------------------"

  noteBook_exited               = "EnrichmentLoadResultsByDQ"
  Environment                   = "Environment is                                : " + environment 
  SourceDirectory               = "Source Directory                              : " + sourceDirectoryPath
  TotalFinalCount               = "Total Data Count(TDC)                         : " + str(df_enrich_count)
  TotalErrorCount               = "Total DQ Failure Count(TFC)                   : " + str(dq_ErrorListCount)
  ErrorPercentage               = "Total DQ Failure Count Percentage             : " + str(dq_ErrorListPercentageVal)+"%(TFC*100/TDC)"
  TotalModCount                 = "Total ModList Count(TMC)                      : " + str(dq_ModListCount)
  ModPercentage                 = "Total ModList Count Percentage                : " + str(dq_ModListPercentageVal)+"%(TMC*100/TDC)"
  TotalGoodCount                = "Total good count                              : " + str(dqlCleanRecordsCount)
  errorSummaryText              = "DQ failure count summary:\n"
  dmSummaryText                 = "DM count summary:\n"

  finalOutput = "\n".join([noteBook_exited,Underline,Environment,SourceDirectory,Underline,TotalFinalCount,TotalErrorCount,ErrorPercentage,TotalModCount,ModPercentage,TotalGoodCount,Underline,errorSummaryText,errorSummary,dmSummaryText,dmSummary])

  print (finalOutput)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

metric_dict["STEP_ERROR"] = 'DQ_SUMMARY_END#NO_ERROR'
metric_dict["STATUS"] = 'PASSED'
metric_dict["FINAL_OUTPUT"] = finalOutput
metric_json = json.dumps(metric_dict)
dbutils.notebook.exit(metric_json)

# COMMAND ----------

