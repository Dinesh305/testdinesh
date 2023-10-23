# Databricks notebook source
# DBTITLE 1,Utilities for Various Scripts
######################################### Generic Utils is to make development more streamlined and easier ###################################################

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install cron-descriptor

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from functools import reduce

from datetime import date
from datetime import timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

import psycopg2
import pandas as pd
import smtplib
import sys
import boto3
from botocore.exceptions import ClientError

import json
import datetime
from pathlib2 import Path
import time
import traceback
from time import localtime, strftime
import requests
import boto3
import json
import urllib.request
from urllib.error import HTTPError
from botocore.exceptions import ClientError
import dataos_splunk
from delta import DeltaTable
import re
from pyspark.sql import DataFrame
from cron_descriptor import get_description, ExpressionDescriptor
import pytz

# COMMAND ----------

class Trace(object):
  
  def __init__(self):
    self.bypass = False
    
  def __call__(self, func):
    def wrapper(*args, **kwargs):
      if self.bypass:
        tb = traceback.format_exc()
        exception = "Exception on running, Stack Trace is: ".format('\n' + str(tb))
        print(tb)
      else:
        result = func(*args, **kwargs)
        return result
    return wrapper

# COMMAND ----------

# DBTITLE 1,Ephemeral Notebook Functions
trace_exit_notebook = Trace()
@trace_exit_notebook
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

# DBTITLE 1,BAT Functions
def read_dataframe(file_dir, file_format = 'parquet'):
  
  """
  Arguments
    file_dir : directory/file_path as string.
    file_format : various file format values, currently only two are supported
                  'parquet' and 'delta' are the values it can take.

  Return Value
    returns a dataframe given a valid file_dir {file directory}
    returns a 'N/A' if invalid file_dir string or unable to read
  
  """
  df = 'N/A'
  
  try:
    
    # setting default value to be 'N/A'
    if file_format == 'parquet':
      df = spark.read.parquet(file_dir)
    elif file_format == 'delta':
      df = spark.read.format('delta').load(file_dir)
    else:
      raise Exception('UnknownFile Format!')
      
  except Exception as e:
    print(e)
  return df

# COMMAND ----------

def get_dataframe_info(kind_of_file, directory,file_format='parquet'):
  
  """
  arguments 
  
    kind_of_files : takes three values, 'STAGE', 'FLAGS', 'FINAL', 'INTERMEDIATE'. 
                  general indication of where is the data being read from
  
    directory : takes path/directory for data as string

    file_format: file format of the data, can take, parquet or delta as value

  Return Value
    returns a dataframe and count of records in that dataframe
  """
  
  df = ''
  df_count = ''
  
  try:
    
    STEP = 'Reading Data in {0} Directory'.format(kind_of_file)
    df = read_dataframe(directory, file_format)
    
    if df !='':
      STEP = 'Count Records in {0} Directory'.format(kind_of_file)
      df_count = df.count()
    
  except Exception as e:
    exit_notebook(STEP,e)
    
  else:
    return df,df_count


# COMMAND ----------

# check lot_id is valid timestamp in parquet flags
def validate_lot_id(df, lot_id_column_name = 'lot_id', compliance_format = "%Y-%m-%dT%H:%M:%S.%f"):
  lotIdList = df.select(lot_id_column_name).distinct().collect()
  try:
    print (lotIdList)
    for lot_ids in lotIdList:
      lot_id_val = time.mktime(datetime.datetime.strptime(lot_ids.lot_id, compliance_format).timetuple())
      return True
  except:
    return False

# COMMAND ----------

def validate_stg_rec_guid(df,src_name,col_name,records_count,file_kind):
  global passedTests
  global failedTests
  stg_rec_guid_count = df.select(F.col(col_name)).distinct().count()
  print (stg_rec_guid_count)
  status_msg = "Expected count for total values of column '{0}' in {1} {2} {3} actual is  {4}  .".format(col_name,src_name,file_kind, records_count,stg_rec_guid_count)
  if str(records_count) == str(stg_rec_guid_count):
    passedTests.append(status_msg)
  else:
    failedTests.append(status_msg)

# COMMAND ----------

def list_column_content(df_parquet_flags,col_name = 'mod_list'):
  """
  arguments: df_parquet_flags, col_name 
  df_parquet_flags : dataframe 
  col_name : column name which has be reduced to list values 
  
  returns a single value :  reduced_mod_list
  reduced_mod_list : reduced list containing the DM & DQ check names like ['DM7','DM62','DQ90',...]
  
  """
  try:
    distinct_list = df_parquet_flags.select(col_name).distinct()

    # split the row based on '[,]'
    # then aggregate the values 
    collect_list_content = distinct_list.select(F.split(F.col(col_name), "[,]").alias('S')).agg(F.collect_set('S').alias('S')).collect()

    # from collected set 
    split_list = [list(set([j.split('_')[0] for j in i])) for i in collect_list_content[0].S]

    # reduce the collected values which are in list
    reduced_list = set(reduce(lambda x,y:x+y, split_list))

    # return the value ['DM89','DM8','DM998','DQ23',...] 
    print (reduced_list)
    return list(reduced_list)
  except Exception as e:
    print(e)
    print('ERROR [list_column_content function level] {0}'.format(e))

def validate_column_list_flags(dataframe,column_name,specific_key):
  if column_name: pass
  else:
    raise Exception('Please Mention Column Name')
  
  if specific_key: pass
  else:
    raise Exception('Please Mention DM or DQ as argument for specific key')
    
  try:
    # collecting required dm & dqs checks in a list 
    expected_checks = [each for each in required_dq_checks.split(',') if specific_key.lower() in each.lower()]
    
    expected_checks_set = set()
    for each in expected_checks:
      if each == '':pass
      else: expected_checks_set.add(each)

    # collecting applied or flagged dm flags in a list 
    applied_checks = list_column_content(dataframe,column_name)
    
    applied_checks_set = set()
    for each in applied_checks:
      if each =='': pass 
      else: applied_checks_set.add(each)

    # boolean value if any of them were to match 
    # applied_checks_set is a subset of expected_checks_set
    flag = applied_checks_set.issubset(expected_checks_set) #any(each_element in applied_checks for each_element in expected_checks)
  except Exception as e:
    print('ERROR [validate_column_list_flags function level] {0}'.format(e))
  else :
    return flag, expected_checks, applied_checks

# COMMAND ----------

"""
Note The following Functions : 
  1. validate_datatypes
  2. validate_schema
  3. validate_columns
  
each function has different purpose, they just happen to share same implementation.
At some point, they might change so for that purpose, they are not implemented as a single function which is a re-used.

if this was objected-oriented programming paradigm then this implementation can be interepreted as extending the following Abstract Class BAT 

####

abstract class BAT(__workflow__):

  def __init__():
  raise Exception('NotImplemented!')
  
  
  def validate_columns():
  raise Exception('Not Implemented, Needs to be implemented!')
  
  def validate_datatypes():
  raise Exception('Not Implemented, Needs to be implemented!')
  
  def validate_columns():
  raise Exception('Not Implemented, Needs to Implemented!')

####

now, till this point the implementation is coded in same fashion but going forward, there might be change in the respective functions so don't merge these functions.

"""  

# COMMAND ----------

def validate_datatypes(actual_dtypes,expected_dtypes,file_kind):
  
  global passedTests
  global failedTests
  global Underline
  
 
  diff_in_dtypes = set(actual_dtypes) - set(expected_dtypes) 
  review = "{} \n Expected difference between actual and expected datatypes of {} {} is 0 & actually is {} .".format(Underline,source_name,file_kind,len(diff_in_dtypes))
  print(diff_in_dtypes)
  if len(diff_in_dtypes) == 0:
    passedTests.append(review)
  else:
    failedTests.append(review)

# COMMAND ----------

def validate_columns(actual_columns,expected_columns,file_kind):
  
  global passedTests
  global failedTests
  global Underline
  
  actual_columns  = set(actual_columns)
  expected_columns = set(expected_columns)
  diff_in_cols = actual_columns - expected_columns 
  review = "{} \n Expected difference between actual and expected Columns of {} {} is 0 & actually is {} .".format(Underline,source_name,file_kind,len(diff_in_cols))
  print(diff_in_cols)
  if len(diff_in_cols) == 0:
    passedTests.append(review)
  else:
    failedTests.append(review)
    

# COMMAND ----------

def validate_schema(actual_schema,expected_schema,file_kind):
  
  global passedTests
  global failedTests
  global Underline
  
  actual_schema  = set(actual_schema)
  expected_schema = set(expected_schema)
  diff_in_schemas = actual_schema - expected_schema
  review = "{} \n Expected difference between actual and expected schema of {} {} is 0 & actually is {} .".format(Underline,source_name,file_kind,len(diff_in_schemas))
  print(diff_in_schemas)
  if len(diff_in_schemas) == 0:
    passedTests.append(review)
  else:
    failedTests.append(review)
    

# COMMAND ----------

# this is used by basic validations for expected types/columns

# Purpose - returns a list of column names
# Inputs - list of types. List of tuple pairs, [0] is the column name, [1] is the type of the column
# Outputs - list of column names
def get_columns(types):
  cols = []
  for i in types:
    cols.append(i[0])
  return cols

# COMMAND ----------

# DBTITLE 1,Workflow Invoker Functions
# more workflow constants
## note: this needs to contain every variable across the workflow invokers to be common
def set_constants():
  global sender
  global RedshiftConnectionTestPath
  global LoadPath
  global DSRLoadPath
  global DQTestPath
  global BATPath
  global DQSummaryPath
  global ProdLoadPath
  global DSRProdLoadPath
  global pre_check_RS_nb
  global load_nb
  global BAT_nb
  global dq_test_nb
  global RS_test_nb
  global dq_summary_nb
  global prod_load_nb
  global total_time
  global validation_total_time
  global pre_check_RS_runtime
  global load_runtime
  global BAT_runtime
  global DQ_test_runtime
  global redshift_validation_time
  global DQ_summary_runtime
  global prod_load_runtime
  global pre_check_RS_status
  global load_status
  global lot_id
  global BAT_status
  global DQ_test_status
  global RS_test_runtime
  global enrichment_summary_status
  global prod_load_status
  global redshift_validation_status
  global receivers
  global stg_dir
  global dirs_included

  RedshiftConnectionTestPath               = '../test/pre_check_RS'
  LoadPath                                 = "../app/{}".format(source_key.get(instance).get("LoadPath"))
  DSRLoadPath                              = "../app/{}".format(source_key.get(instance).get("DSRLoadPath"))
  DQTestPath                               = source_key.get(instance).get("DQTestPath")
  BATPath                                  = source_key.get(instance).get("BATPath")
  DQSummaryPath                            = source_key.get(instance).get("DQSummaryPath")
  ProdLoadPath                             = source_key.get(instance).get("ProdLoadPath")
  DSRProdLoadPath                          = source_key.get(instance).get("DSRProdLoadPath")

  pre_check_RS_nb                          = ''
  load_nb                                  = ''
  BAT_nb                                   = ''
  dq_test_nb                               = ''
  RS_test_nb                               = ''
  dq_summary_nb                            = ''
  prod_load_nb = ''

  total_time                               = 0
  validation_total_time                    = 0
  pre_check_RS_runtime                     = 0
  load_runtime                             = 0
  BAT_runtime                              = 0
  DQ_test_runtime                          = 0
  RS_test_runtime                          = 0
  DQ_summary_runtime                       = 0
  prod_load_runtime                        = 0

  pre_check_RS_status                      = "NOT EXECUTED"
  load_status                              = "NOT EXECUTED"
  lot_id                                   = ""
  BAT_status                               = "NOT EXECUTED"
  DQ_test_status                           = "NOT EXECUTED"
  redshift_validation_status               = "NOT EXECUTED"
  enrichment_summary_status                = "NOT EXECUTED"
  prod_load_status                         = "NOT EXECUTED"
  DQ_summary_status                        = "NOT EXECUTED"
    
  receivers                                = email

# COMMAND ----------

def handle_multi_file(lst1,lst2):
  """
  Arguments :
    lst1 : logical names in a list like alias_list = ['split_dataos_ref','split_dashboard_ref','split_analyst_ref','ignore_tbl']
    lst2 : path names in a list like parquet_list = [parquet_final_write+'/dataos',parquet_final_write+'/dashboard',parquet_final_write+'/analyst',parquet_final_write +'/ignore']
  
  Return Value :
    returns a string which has ParquetFinalPath as 'alias1=<s3://path1>,alias2=<s3://path2>'  
  """

  final_str = ''
  for i in range(len(lst2)):
    final_str = final_str + lst1[i]+ '=' + lst2[i] + ','
  final_str = final_str[0:len(final_str)-1]
  return (final_str)

# COMMAND ----------

def exception_step(step,error,instance,environment,now):
  """
  Calling this method incase of any exception or error during execution of the notebook
  """
  print(str(error) + '\n\n')
  tb = traceback.format_exc()
  exception = "Exception on running {}. Stack Trace is: ".format(step) + '\n' + str(tb)
  subject = 'FAILED: {} - {} Exception - {} {}'.format(instance,step,environment,now)
  status = SendEmail(exception, sender, receivers, subject)
  print("Job Status: FAILED")
  print("Email Status: " + status)
  print(exception)
  global workflow_step
  workflow_step = '{} failed'.format(step)

# COMMAND ----------

def stopCurrentProcess_with_metrics(msg):
  import datetime
  m_dict = {}
  m_dict["enrich_job_name"] = source_key.get(instance).get('app').replace("_","-")
  m_dict["enrich_environment"] = environment
  m_dict["enrich_workflow_step"] = workflow_step
  
  if lot_id != '':
    m_dict["enrich_lot_id"] = lot_id
  else:
    m_dict["enrich_lot_id"] = "{:%Y-%m-%dT%H:%M:%S.%-S}".format(datetime.datetime.now())
  
  source_type = prod_switch
  if source_type == 'inklaser':
    m_dict["enrich_source_type"] = 'MIXED'
  else :
    m_dict["enrich_source_type"] = source_type.upper()
  
  m_dict['enrich_processing_date'] = '{:%Y-%m-%d %H:%M:%S.%f}'.format(datetime.datetime.now())[:-4]
  m_dict["enrich_end_time"] = m_dict['enrich_processing_date'] 
  m_dict["enrich_start_time"] = start_time
    
  m_dict["enrich_job_status"] = 'SUCCESS'
  m_dict["enrich_step_error"] = msg

  metrics_upload_status = False

  timestamp = '{:%Y%m%d-%H%M%S}'.format(datetime.datetime.now()) # go from "yyyy-MM-dd HH:mm:ss" to "yyyyMMdd-HHmmss"
  upload_file_name = source_key.get(instance).get("run_type") + '_' + timestamp + ".json"
  workflowMetricsPath = s3_for_json_upload_elk + upload_file_name

  #write metrics json file
  metrics_upload_status = dbutils.fs.put(workflowMetricsPath, json.dumps(m_dict), overwrite = True)
  elk_metrics_file_location = "Metrics file location : %s" %workflowMetricsPath

  print(elk_metrics_file_location)
  print(metrics_upload_status)

  msg = '{"STATUS" : "SUCCESS"}'
  
  dbutils.notebook.exit(json.dumps(json.loads(msg)))

# COMMAND ----------

def killCurrentProcess_with_metrics(error_msg):
  import datetime
  m_dict = {}
  m_dict["enrich_job_name"] = source_key.get(instance).get('app').replace("_","-")
  m_dict["enrich_environment"] = environment
  m_dict["enrich_workflow_step"] = workflow_step
  
  if lot_id != '':
    m_dict["enrich_lot_id"] = lot_id
  else:
    m_dict["enrich_lot_id"] = "{:%Y-%m-%dT%H:%M:%S.%-S}".format(datetime.datetime.now())
  
  source_type = prod_switch
  if source_type == 'inklaser':
    m_dict["enrich_source_type"] = 'MIXED'
  else :
    m_dict["enrich_source_type"] = source_type.upper()
  
  m_dict['enrich_processing_date'] = '{:%Y-%m-%d %H:%M:%S.%f}'.format(datetime.datetime.now())[:-4]
  m_dict["enrich_end_time"] = m_dict['enrich_processing_date'] 
  m_dict["enrich_start_time"] = start_time
    
  m_dict["enrich_job_status"] = 'FAILED'
  m_dict["enrich_step_error"] = error_msg
  
  dbutils.notebook.exit(json.dumps(m_dict))

# COMMAND ----------

# function to create connection to database
def create_db_connection(host, port, user, password, dbname,sslmode):
  connection_string = "host = '%s' port = '%s' user = '%s' password = '%s' dbname = '%s'sslmode = '%s' connect_timeout = '60'" % (host, port, user, password, dbname, sslmode)
  conn = psycopg2.connect(connection_string)
  conn.autocommit = False
  return conn

# COMMAND ----------

# function to fetch aws password
def get_aws_secret(secrets_manager_url, region, secret_name, secret_string_key):
  
  aws_secrets_manager_service = boto3.client(service_name = 'secretsmanager', region_name = region, endpoint_url = secrets_manager_url)
  
  try:
    key = eval(aws_secrets_manager_service.get_secret_value(SecretId = secret_name)['SecretString'])[secret_string_key]
  
  except Exception as e:
      key = ""
      if e.response['Error']['Code'] == 'ResourceNotFoundException':
          print("The requested secret " + secret_name + " was not found")
      elif e.response['Error']['Code'] == 'InvalidRequestException':
          print("The request was invalid due to:", e)
      elif e.response['Error']['Code'] == 'InvalidParameterException':
          print("The request had invalid params:", e)
      else:
          print(str(e.response))

  return key

# COMMAND ----------

def Send_Email(body_text, fromaddr, recipient, subject):
    ses = boto3.client(service_name='ses', region_name="us-west-2")  
    charset = "utf-8"

    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = recipient

    msg_body = MIMEText('\n'.join([body_text]), 'html')
    msg.attach(msg_body)

    try:
        # Provide the contents of the email.
        response = ses.send_raw_email(
            Source=fromaddr,
            Destinations=recipient.split(','),
            RawMessage={
                'Data': msg.as_string(),
            }
        )
        print(response)
    # Display an error if something goes wrong.
    except Exception as e:
        print(e.response['Error']['Message'])
        return 'FAILED'
    else:
        print("Email sent! Message ID:"),
        print(response['ResponseMetadata']['RequestId'])    
        return 'SUCCESS'

# COMMAND ----------

def SendEmail(body_text, fromaddr, recipient, subject):
    ses = boto3.client(service_name='ses', region_name="us-west-2")  
    charset = "utf-8"

    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = recipient

    msg_body = MIMEMultipart('alternative')
    textpart = MIMEText(body_text.encode(charset), 'plain', charset)
    msg_body.attach(textpart)
    msg.attach(msg_body)

    try:
        # Provide the contents of the email.
        response = ses.send_raw_email(
            Source=fromaddr,
            Destinations=recipient.split(','),
            RawMessage={
                'Data': msg.as_string(),
            }
        )
        print(response)
    # Display an error if something goes wrong.
    except Exception as e:
        print(e.response['Error']['Message'])
        return 'FAILED'
    else:
        print("Email sent! Message ID:"),
        print(response['ResponseMetadata']['RequestId'])    
        return 'SUCCESS'

# COMMAND ----------

def SendEmailWithAttachment(attachment, filename, body_text, fromaddr, recipient, subject):
    ses = boto3.client(service_name='ses', region_name="us-west-2")  
    charset = "utf-8"

    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = recipient
    
    msg_body = MIMEMultipart('alternative')
    textpart = MIMEText(body_text.encode(charset), 'plain', charset)
    msg_body.attach(textpart)
    msg.attach(msg_body)
    
    

    try:
      if attachment != '' and filename != '':
        part = MIMEApplication(
                  attachment,
                  Name=filename
              )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % filename
        msg.attach(part)
        # Provide the contents of the email.
        response = ses.send_raw_email(
            Source=fromaddr,
            Destinations=recipient.split(','),
            RawMessage={
                'Data': msg.as_string(),
            }
        )
        print(response)
    # Display an error if something goes wrong.
    except Exception as e:
        print(e.response['Error']['Message'])
        return 'FAILED'
    else:
        print("Email sent! Message ID:"),
        print(response['ResponseMetadata']['RequestId'])    
        return 'SUCCESS'

# COMMAND ----------

def SendEmailWithMultiAttachment(attachment_dict, body_text, fromaddr, recipient, subject):
    ses = boto3.client(service_name='ses', region_name="us-west-2")  
    charset = "utf-8"

    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = recipient
    
    msg_body = MIMEText('\n'.join([body_text]), 'html')
    msg.attach(msg_body)
    
    try:
      if attachment_dict != '':
        for k,v in attachment_dict.items():
          part = MIMEApplication(
                    k,
                    Name=v
                )
          # After the file is closed
          part['Content-Disposition'] = 'attachment; filename="%s"' % v
          msg.attach(part)
        # Provide the contents of the email.
        response = ses.send_raw_email(
            Source=fromaddr,
            Destinations=recipient.split(','),
            RawMessage={
                'Data': msg.as_string(),
            }
        )
        print(response)
    # Display an error if something goes wrong.
    except Exception as e:
        print(e.response['Error']['Message'])
        return 'FAILED'
    else:
        print("Email sent! Message ID:"),
        print(response['ResponseMetadata']['RequestId'])    
        return 'SUCCESS'

# COMMAND ----------

###########################################################################################################  
# Function for standardising header: Standardize Header
########################################################################################################### 
# function to replace everything except "alphabets", "digits" and "$" from column header
def standardize_headers(df, var = ''):
  for header in df.schema.names:
    standard_header = re.sub('[^a-zA-Z0-9$]', var, header)
    standard_header = standard_header.lower()
    df = df.withColumnRenamed(header, standard_header)
  return df

# COMMAND ----------

###########################################################################################################  
# Create error_list or mod_list columns
# how to call this function to create error_list
#         df = create_error_mod_list(df,'check_dq','error_list')
# how to call this function to create mod_list
#         df = create_error_mod_list(df,'check_dm','mod_list')
########################################################################################################### 
def create_error_mod_list(df,check,chk_col):
  cols = [F.col(col_name) for col_name in df.columns if check in col_name]
  return(df.withColumn(chk_col, udf_concat_list(*cols)))

# COMMAND ----------

###########################################################################################################  
# Change datatypew of columns of a dataframe
# how to call this function to create error_list
#         df = change_dtype(df,expected_types_df) # here expected_types_df is defined in schema.py
########################################################################################################### 
def change_dtype(df,expected_dtype):
  for col_dtype in expected_dtype:
    if 'numeric' in col_dtype[1]:
      df = df.withColumn(col_dtype[0], F.col(col_dtype[0]).cast(DoubleType()))
    elif "timestamp" in col_dtype[1]:
      df = df.withColumn(col_dtype[0], F.col(col_dtype[0]).cast(TimestampType()))
    elif "date" in col_dtype[1]:
      df = df.withColumn(col_dtype[0], F.col(col_dtype[0]).cast(DateType()))
  return (df)

# COMMAND ----------

################################################################################################################
####  Copy or move from one S3 directory to another
################################################################################################################
def moveOrcopy_oneS3_to_another_s3(from_s3_dir,to_s3_dir,dirs_to_process,operation):
  try:
    print ('dirs_to_process', dirs_to_process)
    if dirs_to_process:
      for folder in dirs_to_process:
        from_folder = from_s3_dir  + folder
        to_folder   = to_s3_dir    + folder
        files_to_process = [i.name for i in dbutils.fs.ls(from_folder)]
        if files_to_process:
          for a_file in files_to_process:
            from_file = from_folder + a_file
            to_file   = to_folder   + a_file
            print (from_file)
            print (to_file)
            print ('  processing', a_file)
            if operation.lower() == 'cp':
              dbutils.fs.cp(from_file, to_file,recurse=True)
            elif operation.lower() == 'mv':
              dbutils.fs.mv(from_file, to_file,recurse=True)
            else:
              print ('Neither Copy nor Move is specified')
  except Exception as e: 
    print (str(e))


# COMMAND ----------

#parses out input into individual widgets
def parse_input_widgets(inputJson):
  # creates widgets with default values
  for key, value in json.loads(str(inputJson)).items():  
    dbutils.widgets.text(key,value)
    print (key.ljust(25) + ": " + value)

  # get values from widget   
  return [(x,y) for x,y in json.loads(str(inputJson)).items()]


# COMMAND ----------

def write_data_to_stage_location(source_data_name, df, location, file_format = 'delta',data_write_mode ='overwrite', merge_schema = False, file_schema_overwrite = False):
  ###########################################################################################################
  ###                Append the current load data to the existing data in stage                           ###
  ###########################################################################################################
  try:
  ###########################################################################################################
    # get the individual directory of the stage for each source
    STEP = 'Writing to {0} (Stage Directory) for {1}'.format(location,source_data_name)
    
    print('excution file_format is {0} and mode is {1}'.format(file_format,data_write_mode))
 
    # append df to stg directory in s3
    if file_format == 'delta':
      
      if merge_schema :
        df.write.mode(data_write_mode).option("compression", "snappy").option("mergeSchema",merge_schema).format("delta").save(location)

      elif file_schema_overwrite:
        df.write.mode(data_write_mode).option("compression", "snappy").option("overwriteSchema",file_schema_overwrite).format("delta").save(location)
        
      else:
        df.write.mode(data_write_mode).option("compression", "snappy").format("delta").save(location)
        
    elif file_format=='parquet':
      df.write.mode(data_write_mode).option("compression", "snappy").parquet(location)
  ###########################################################################################################
  except Exception as e:
    
    status = 'write to stage exited for {0}'.format(source_data_name)
    print(status)
    exit_notebook(STEP,e)
    
  else:
    status = 'write to stage complete for {0}'.format(source_data_name)
    print(status)

# COMMAND ----------

def remove_invalid_record(source_data_name,df,col_name,invalid_record_value = ''):
  ###########################################################################################################
  ###                drop records having invalid value 
  #                eg:  some common invalid value is ''----'' in country in wpp                                     ###
  ###########################################################################################################
  try:
  ###########################################################################################################
    STEP = "Drop records where {0} is '{1}'".format(col_name,invalid_record_value)
    df = df.filter(F.col(col_name) != invalid_record_value)
  ###########################################################################################################
  except Exception as e:
    exit_notebook(STEP,e)
  else:
    return df

# COMMAND ----------

###########################################################################################################
###                Convert the datatype of the dataframe to the type Expected                           ###
###########################################################################################################
 
def type_cast_columns(source_data_name,df,expected_types):
  
  type_to_cast = {
    'double':DoubleType,
    'float':FloatType,
    'timestamp':TimestampType,
    'date':DateType,
    'smallint':ShortType,
    'byte':ByteType,
    'int':IntegerType,
    'integer':IntegerType,
    'numeric':IntegerType,
    'string':StringType,
    'varchar':StringType,
    'boolean' : BooleanType

  }
  
  STEP = 'Converting the String datatype to Appropriate DataType {0}'.format(source_data_name)
  try:
    for col_dtype in expected_types:
      type_cast = type_to_cast.get(col_dtype[1])
      if type_cast is None:
        raise Exception('specified dtype {} for {} column is not suppored'.format(col_dtype[1],col_dtype[0]))
      else:
        df = df.withColumn(col_dtype[0], F.col(col_dtype[0]).cast(type_cast()))
  ###########################################################################################################
  except Exception as e:
    status = 'type casting complete for {0}'.format(source_data_name)
    exit_notebook(STEP,e)
  else:
    status = 'type casting complete for {0}'.format(source_data_name)
    print(status)
    return df
    

# COMMAND ----------

###########################################################################################################
###                Get Boto3 S3 connection                                                              ###
###########################################################################################################
def get_boto3_conn():
  sts = boto3.client('sts', region_name='us-west-2')
  if environment.lower() == 'dev':
      response = sts.assume_role(RoleArn='arn:aws:iam::841913685921:role/bdbt-s3-access-role',RoleSessionName='get')
  elif environment.lower() == 'itg':
      response = sts.assume_role(RoleArn='arn:aws:iam::651785582400:role/databricks-itg-s3-access-role', RoleSessionName='get')
  elif environment.lower() == 'prod':
      response = sts.assume_role(RoleArn='arn:aws:iam::651785582400:role/databricks-prod-s3-access-role', RoleSessionName='get')

  conn = boto3.client('s3'
          ,aws_access_key_id=response['Credentials']['AccessKeyId']
          ,aws_secret_access_key=response['Credentials']['SecretAccessKey']
          ,aws_session_token=response['Credentials']['SessionToken'])

  return conn

# COMMAND ----------

def fetch_token(client, secretId):
  try:
    response = client.get_secret_value(SecretId=secretId)
    token = json.loads(response.get('SecretString')).get('token')
  except ClientError as e:
    raise Exception("Client Error: {}".format(e))
  except Exception as e:
    raise Exception(e)
  return(token)

# COMMAND ----------

def trigger_databricks_job(token, api_endpoint, username, job_id, notebook_params = {}):
  try:
    res = requests.post(api_endpoint, auth=(username, token), 
                        json = {
                        "job_id": job_id,
                        "notebook_params": notebook_params    
                        })
  except HTTPError as err:
      if err.code in [401, 403]:
          raise Exception(f'Error in request. Possibly authentication failed [{err.code}]: {err.reason}')
      elif err.code == 404:
          raise Exception('Requested item could not be found')
      else:
          raise err
  except Exception as e:
    raise e
  return res

# COMMAND ----------

######################################### Defining the function for calling token and trigger functions #####################################
def databricks_job_call(client, SecretId, api_endpoint, username, job_id, notebook_params = {}):
  try:
    token = fetch_token(client, SecretId)
    result = trigger_databricks_job(token, api_endpoint, username, job_id, notebook_params)
    if result.status_code == requests.codes.ok:
      print("Job has been triggered successfully with status code: ", result.status_code)
    else :
      print("Job trigerred failed")
  except ClientError as e:
    raise Exception("Credentials are not valid: {}".format(e))
  except HTTPError as err:
    raise Exception("Error in request: {}".format(e))
  except Exception as e:
    raise e

# COMMAND ----------

######################################## for getting the link of latest runs of jobs ############################################
def fetch_run_url(endpoint_url, username, token, job_id, run_index = 0):
  try:
    response = requests.get(endpoint_url.format(job_id),auth=(username, token))    
    temp = response.json()
    runs_info = temp['runs']
    run_link = runs_info[run_index].get('run_page_url')
    trigger_type = runs_info[run_index].get('trigger')
    return run_link,trigger_type
  except Exception as e:
    print(response.json())
    raise e

# COMMAND ----------

def send_email_to_users_with_attachment_SES(local_dir, fromaddr, recipient, subject, body_text, body_type="plain"):

    try:
      ses = boto3.client(service_name='ses', region_name="us-west-2")  
    except ClientError as e:
      print("Error in  SES service: [%s]" % str(e))
    
    # The character encoding for the email.
    charset = "utf-8"

    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    # Add subject, from and to lines.
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = recipient

    # Create a multipart/alternative child container.
    msg_body = MIMEMultipart('alternative')

    # necessary if you're sending a message with characters outside the ASCII range.
    textpart = MIMEText(body_text.encode(charset), body_type, charset)

    # Add the text and HTML parts to the child container.
    msg_body.attach(textpart)
  
    #att.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
    exists = os.path.isdir(local_dir)
    if exists:
      for f in os.listdir(local_dir):
        if not f.startswith("."):
          part = MIMEBase('application', "octet-stream")
          part.set_payload( open(local_dir+'/'+f,"rb").read() )
          encoders.encode_base64(part)
          part.add_header('Content-Disposition', 'attachment; filename="{0}"'.format(os.path.basename(f)))
          msg.attach(part)

    # parent container.
    msg.attach(msg_body)
    try:
        # Provide the contents of the email.
        response = ses.send_raw_email(
            Source=fromaddr,
            Destinations=recipient.split(','),
            RawMessage={
                'Data': msg.as_string(),
            }
        )
        print(response)
    # Display an error if something goes wrong.
    except ClientError as e:
        print("Error in  SES service: [%s]" % str(e))
    else:
        print("Email sent! Message ID:"),
        print(response['ResponseMetadata']['RequestId'])

# COMMAND ----------

def delete_s3_files_by_age(s3_path:str, no_of_days:int):
  """
  Deletes files in S3 in the specified path that are equal to or older than no of days speciifed.
  
  Parameters
  ----------
  
  s3_path: str
    The S3 path in format s3://<bucket-name>/<some-path>.
  no_of_days: int
    The number of days determines which files, older or equal to the specified number of days will be deleted.
  
  Returns
  -------
  None
  
  """
  try:
    STEP = "Creating S3 client object"
    s3 = boto3.client('s3')

    STEP = "Get bucket and prefix from s3 path specified"
    bucket = s3_path.split('/')[2]
    prefix = '/'.join(s3_path.split('/')[3:])

    STEP = "Calculate cutoff time from current time in UTC"
    current_time = datetime.datetime.now(pytz.timezone('UTC'))
    cutoff_time = current_time - timedelta(days=int(no_of_days))
    
    STEP = "Get all objects in the specified S3 bucket and prefix"
    response = s3.list_objects_v2(Bucket=bucket, Prefix = prefix)
    
    STEP = "Extract object details from the retrieved list"
    objects = response['Contents']

    STEP = "Iterate over all objects and delete those that are older or equal to no of days specified"
    for object in objects:
      last_modified = object['LastModified']
      
      # Convert datetime object to UTC if not already in UTC.
      if last_modified.strftime('%Z') == 'UTC':
        last_modified = last_modified
      else:
        last_modified = last_modified.astimezone(pytz.timezone('UTC'))
      
      # Check if last modified date if equal or older than cutoff time.
      if last_modified <= cutoff_time:
        key = object['Key']
        s3.delete_object(Bucket = bucket, Key = key)
        print(f" Deleted {key}")

  except Exception as e:
    raise Exception(f"Exception in {STEP}: {e}")

# COMMAND ----------

def write_as_delta(
  df: DataFrame, 
  path: str, mode: str = 'append', 
  compression_type: str = 'snappy', 
  partition_columns: list = None, 
  mergeSchema: bool = False,
  overwriteSchema: bool = False,
  optimize: bool = False,
  z_order_columns: list = None,
  vacuum: bool = False,
  table_name:str = None,
  num_partitions=None,
  coalesce=None):
  
  """
  Writes a dataframe to a delta table at the specified path, with specified mode and options. If partition columns are specified, it will write with partitions. 
  It will also optimize with Z-ordering if set to True to improve performance and eliminate large number of small files. 
  It will also remove unused files by vacuuming if vacuum option is set to True.

  Parameters
  ----------
  df: DataFrame
    The dataframe to write to the delta table.
  path: str
    The file path for the delta table.
  mode: str
    The write mode, either 'overwrite' for events like full load or 'append' for events like incremental load.
  compression_type: str
    Compression mechanism to use while writing delta table. Example: snappy, gzip, etc. Defaults to snappy.
  partition_columns: List
    A Comma Separated list of column names used to partition a delta table. By partitioning the data, queries can skip reading data that doesn't match filter condition in 
    where clause resulting in faster execution times. Use columns with low cardinality like gender, year, etc. Defaults to None.
  mergeschema: bool
    If true, Columns that are present in the DataFrame but missing from the target table are automatically added as part of a write transaction. Defaults to None. 
  overwriteSchema: bool
    If true, change's column type or name or drop a column by rewriting the target table. Defaults to None.
  optimize: bool
    Combines small files into larger ones to eliminate small file problem resulting in better processing times. Defaults to None
  z_order_column: List
    A list of column names to use for Z-ordering. Defaults to None. We can specify multiple columns for ZORDER BY as a comma-separated list. However, the effectiveness 
    of the locality drops with each extra column.
  vacuum: bool
    If set to true, removes unused data files. Defaults to False.
  table_name: str
    External delta table name to be created in databricks hive_metastore. Need to specify databse name along with table name with dot. Example: ref_enrich.supp_g5_pen_mfg. Incase of s3 path "delta.`s3://<bucket-name>/<instance-name>/`".
    Defaults to None.
  num_partitions: int
    If num_partitions is specified, repartition the DataFrame before writing. It ensures data is evenly distributed across all partitions.
  coalesce: int
    If coalesce is specified, reduce the number of partitions in the DataFrame before writing. If merges partition on the same worker node and minimizes data shuffling
    
    Returns
    ------
    None.
  """
  try:
    metric_dict = []
    STEP = "Writing DataFrame as delta format"
    # Writing to specified path in delta format with specified options 
    write_options = {
      'compression' : compression_type,
      'format' : 'delta'
    }
    if overwriteSchema:
      write_options['overwriteSchema'] = 'True'
    if mergeSchema:
      write_options['mergeSchema'] = 'True'
    #If repartition is specified, repartition the DataFrame before writing. It ensures data is evenly distributed across all partitions.
    if num_partitions:
      df = df.repartition(num_partitions)
    #If coalesce is specified, reduce the number of partitions in the DataFrame before writing. If merges partition on the same worker node and minimizes data shuffling
    elif coalesce:
      df = df.coalesce(coalesce)
    if partition_columns:
      partition_clause = ",".join(f'{col}' for col in partition_columns)
      df.write.mode(mode).options(**write_options).partitionBy(partition_clause).save(path)
    else:
      df.write.mode(mode).options(**write_options).save(path)

#     STEP = "Creating an external delta table in Databricks hive metastore"
#   # Creating an external delta table in Databricks hive metastore if it doesn't exist
#     if table_name:
#       spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")

    STEP = "Running optimize command with Z-ordering"
  # If specified, running optimize command to get rid of small files and performing z ordering for performance while querying. 
    if(optimize):
      if (z_order_columns):
        z_order_clause = ",".join(f"{col}" for col in z_order_columns)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_order_clause})")
      else:
        spark.sql(f"OPTIMIZE {table_name}")

    STEP = "Running Vacuum command with Z-ordering"
  # If specified, running Vacuum command to get rid of all unused data files.
    if(vacuum):
      spark.sql(f"VACUUM {table_name}")

 

  except Exception as e:
    exit_notebook(STEP,e)
    
    

# COMMAND ----------

###################################################################################################################################################
###                                                   Extracting lot_id for splunk dashboards                                                   ###
###################################################################################################################################################
def get_lot_id(s3_path, trigger_date = None, column_name = 'ingstn_lot_id'):
  try:
    STEP = "GET DATE FILTER TO EXTRACT LOT_ID"
    # regex pattern to verify if trigger date is specified in yyyy-MM-dd date format 
    regex = r"^\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$"
    # The below code checks if trigger date is specified. If specified it verifies whether it conforms to  yyyy-MM-dd date format. 
    #If trigger date is not specified, it uses current date 
    if trigger_date:
      if re.match(regex, trigger_date):
        filter_date = spark.sql(f"select TO_DATE('{trigger_date}','yyyy-MM-dd')").collect()[0][0]
      else:
        print("APP_ERROR#trigger_date_not_matches_yyyy-MM-dd_pattern")
        exit_notebook(STEP,"trigger_date does not match 'yyyy-MM-dd' pattern")
    else:
      filter_date = spark.sql("select current_date()").collect()[0][0]
    STEP = "EXTRACTING LOT_ID FROM SOURCE PATH"
    lot_id = spark.sql(f"SELECT max({column_name}) FROM delta.`{s3_dir}` WHERE date_format(ingstn_date, 'yyyy-MM-dd') = '{filter_date}'").collect()[0][0]
    return lot_id, filter_date
  except Exception as e:
    exit_notebook(STEP,e)

# COMMAND ----------

#generate splunk dictionary 
def splunk_integration(workflow_step,job_start_time,environment,instance, block='exception', metricsJsonDist= {}):
  try:
    JobExceptionStatus="Exception in "  
    ########### Calling API related info'
    Run_ID, run_link, trigger_type,job_health_status, DB_job_name, cron_exp = job_run_url(environment, job_id)
    cadence = derive_cadence(cron_exp)

    if block == "init":
      metricsJsonDist["enrich_job_name"] = source_key.get(instance).get('app').replace("_","-")
      metricsJsonDist["enrich_job_status"] ="Started"
      metricsJsonDist["enrich_lot_id"] =job_start_time

    if block == "exception":
      metricsJsonDist["enrich_job_name"] = source_key.get(instance).get('app').replace("_","-")
      metricsJsonDist["enrich_job_status"] = "Failed"
      metricsJsonDist["enrich_lot_id"] =job_start_time
      metricsJsonDist["Job_endtime"] = str(datetime.datetime.now().isoformat()).replace(':', '-')
      metricsJsonDist["Exception"] = JobExceptionStatus + workflow_step
    
    if block == 'end':
      metricsJsonDist = metricsJsonDist
    
    metricsJsonDist["app_type"] = "enterprise_downstream"
    metricsJsonDist["Run_URL"]=  run_link
    metricsJsonDist["trigger_type"] = trigger_type
    metricsJsonDist["Run_ID"]=  Run_ID
    metricsJsonDist["Job_ID"]=  job_id
    metricsJsonDist["job_health_status"] = job_health_status
    metricsJsonDist["DB_job_name"]=  DB_job_name
    metricsJsonDist["cadence"] = cadence    #update it for cadence

    metricsJson = json.dumps(metricsJsonDist, sort_keys = True)
    metricsJsonDist = json.loads(metricsJson)	
    # return dictionary   	
    return metricsJsonDist
  except Exception as e:
    raise e

# COMMAND ----------

######################### Splunk Ingestion
import json 
notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
# job_id = notebook_info['tags']['jobId']
job_id='75741'

username_itg = "dataos-databricks-team-enrichment-prod-run-user@external.groups.hp.com"
SecretId_itg = "arn:aws:secretsmanager:us-west-2:828361281741:secret:codeway/databricks/team-enrichment-prod-run-user-Ogp5kz"

username_itg_dsr = "dataos-deployment-dsr@external.groups.hp.com"
SecretId_itg_dsr = "codeway/databricks/deployment-dsr"

itg_api_endpoint= "https://dataos-prod.cloud.databricks.com"
api_endpoint_itg= itg_api_endpoint + "/api/2.1/jobs/runs/list?job_id={}"

job_api = "/api/2.1/jobs/get?job_id={}"

username_dev = "dataos-deployment-enrichment@external.groups.hp.com"
SecretId_dev = "codeway/databricks/deployment-enrichment"

dev_api_endpoint= "https://dataos-dev.cloud.databricks.com"
api_endpoint_dev= dev_api_endpoint + "/api/2.1/jobs/runs/list?job_id={}"

client = boto3.client('secretsmanager',region_name='us-west-2')

def job_run_url(environment, job_id):
  try:
    if environment.lower() in ('prod','itg'):
      username = username_itg
      SecretId = SecretId_itg
      endpoint = itg_api_endpoint
      api_endpoint =api_endpoint_itg

    if environment.lower() in ('prod_dsr','itg_dsr'):
      username = username_itg_dsr
      SecretId = SecretId_itg_dsr
      endpoint = itg_api_endpoint
      api_endpoint =api_endpoint_itg

    if environment.lower() in ('dev'):
      username = username_de
      SecretId = SecretId_dev
      endpoint = dev_api_endpoint
      api_endpoint =api_endpoint_dev

    token= fetch_token(client, SecretId)
    run_link, trigger_type = fetch_run_url(api_endpoint,username, token, job_id)
    Run_ID = run_link.split('/')[-1]
    response = requests.get((endpoint + job_api).format(job_id),auth=(username, token))
    temp= response.json()
    DB_job_name = temp['settings']['name']

    if 'schedule' in temp['settings'].keys():
      job_health_status = temp['settings']['schedule']['pause_status']
      cron_exp = temp['settings']['schedule']['quartz_cron_expression']
    else:
      job_health_status = 'Not scheduled job'
      cron_exp =''

    return Run_ID, run_link, trigger_type,job_health_status, DB_job_name, cron_exp
  except ClientError as e:
    raise Exception("Credentials are not valid: {}".format(e))
  except HTTPError as err:
    raise Exception("Error in request: {}".format(e))
  except Exception as e:
    raise e


# COMMAND ----------

def derive_cadence(cron_expr):
  try:
    cadence = ''
    schedule_str = ''

    if cron_expr == '':
      cadence = "Adhoc"
    else:
      schedule_str = get_description(cron_expr)

      if  "only on" in  schedule_str.lower():
        cadence = 'Weekly'
      elif "day of the month, only in"  in  schedule_str.lower():
        cadence ='Quarterly'
      elif "on day {} of the month".format(cron_expr.split(" ")[-3]) in schedule_str.lower():
        cadence= 'Monthly'
      elif "on day 1 and 15 of the month" in schedule_str.lower():
        cadence = 'Bi-weekly'
      else:
        cadence ='Daily'  
    return cadence 
    
  except Exception as e:
    raise e

# COMMAND ----------

def get_pipeline_status_and_lot_id(ingstn_status_path,column_name,filter_date=None):
  try:
    STEP = "GET DATE FILTER TO EXTRACT LOT_ID"
    # regex pattern to verify if filter date is specified in yyyy-MM-dd date format 
    regex = r"^\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$"
    # The below code checks if filter date is specified. If specified it verifies whether it conforms to  yyyy-MM-dd date format. 
    #If filter date is not specified, it uses current date 
    if filter_date:
      if re.match(regex, filter_date):
        filter_date = spark.sql(f"select TO_DATE('{filter_date}','yyyy-MM-dd')").collect()[0][0]
      else:
        exit_notebook(STEP,"filter_date does not match 'yyyy-MM-dd' pattern")
    else:
      filter_date = spark.sql("select current_date()").collect()[0][0]
    STEP = "Get lot_id"
    row = spark.sql(f"""
                    SELECT max({column_name}) as lot_id FROM delta.`{ingstn_status_path}` WHERE trigger_date = '{filter_date}' 
                    AND pipeline_name = '{instance}'
                    """).collect()[0]
    lot_id = row['lot_id'].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-4]
    STEP = "Get Upstream pipeline status"
    row = spark.sql(f"""
                    SELECT pipeline_status FROM delta.`{ingstn_status_path}` WHERE trigger_date = '{filter_date}' 
                    AND pipeline_name = '{instance}' AND {column_name} = '{lot_id}'
                    """).collect()[0]
    pipeline_status = row['pipeline_status']
    return filter_date,lot_id,pipeline_status
  except Exception as e:
    raise e
    

# COMMAND ----------

def convert_dtypes(df):
  STEP= "Type conversion for df"
  try:
    type_to_cast = {
        'double':DoubleType,
        'float':FloatType,
        'timestamp':TimestampType,
        'date':DateType,
        'smallint':ShortType,
        'byte':ByteType,
        'int':IntegerType,
        'integer':IntegerType,
        'numeric':IntegerType,
        'string':StringType,
        'varchar':StringType,
        'boolean' : BooleanType

      }
    for col_dtype in df_css_cdax.dtypes:
      if col_dtype[1] in type_to_cast.keys():
        # print(col_dtype[1],)
        type_cast= type_to_cast[col_dtype[1]]
        df = df.withColumn(col_dtype[0], F.col(col_dtype[0]).cast(type_cast()))
    return df

  except Exception as e:
    exit_notebook(STEP,e)

# COMMAND ----------

