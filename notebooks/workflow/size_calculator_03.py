# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

import boto3
from collections import OrderedDict
import json
import math

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

#Set the default values 

dbutils.widgets.text("s3_dir","", "s3_dir")
s3_dir = dbutils.widgets.get("s3_dir")

dbutils.widgets.text("delta_flags_write","", "delta_flags_write")
delta_flags_write = dbutils.widgets.get("delta_flags_write")

dbutils.widgets.text("delta_final_write","", "delta_final_write")
delta_final_write = dbutils.widgets.get("delta_final_write")

dbutils.widgets.text("delta_intermediate","", "delta_intermediate")
delta_intermediate = dbutils.widgets.get("delta_intermediate")


# COMMAND ----------

###########################################################################################################
###                Create the list of Dictionary Variables to be send on notebook exit                  ###
###########################################################################################################
metric_dict = OrderedDict()
metric_dict["total_inc_size"] = 0
metric_dict["total_flags_size"] = 0
metric_dict["total_intermediate_size"] = 0
metric_dict["total_final_size"] = 0
metric_dict["STATUS"] = 'FAILURE'
metric_dict["STEP_ERROR"] = 'CALCULATOR_START#NO_ERROR'

# COMMAND ----------

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
### Parse the path                                                                                      ###                                               
###########################################################################################################
def get_path_details(path):
  try:
    tokens = path.split('/')
    if 's3' in tokens[0]:
      bucket_name = tokens.pop(2)
      prefix_path = '/'.join(tokens[2:]) 
    else:
      bucket_name = tokens.pop(0)
      prefix_path = '/'.join(tokens) 
      
  except Exception as e:
      print (e)
  else:
    print (bucket_name,prefix_path)
    return bucket_name,prefix_path
  
###########################################################################################################
### Get data size of a given path                                                                       ###                                               
###########################################################################################################  
conn = boto3.client('s3')
# conn = get_boto3_conn ()

def get_data_size(bucket_name = '',prefix_path = '',*args,**kwargs):
    
    """ Latest File name in a specific location"""
    
    global conn
    
    file_name = ''    
    try:      
        response = conn.list_objects(Bucket=bucket_name,Prefix=prefix_path)
        contents = response.get('Contents',[])
        interested_objs = []
        interested_objs_size = []
        uninterested_objs = []
        uninterested_objs_size = []
        # get file names
        for each_obj_metadata in contents:
          file_name = each_obj_metadata.get('Key')
          # to ignore file_names that end with '/', as they are just prefix_path in general
          if file_name[-1] == '/':
            pass
          # ignore archive files 
          elif 'archive' in file_name:
            # size in bytes
            size = each_obj_metadata.get('Size')
            uninterested_objs.append(file_name)
            uninterested_objs_size.append(size)
          else:
            # size in bytes
            size = each_obj_metadata.get('Size')
            interested_objs.append(file_name)
            interested_objs_size.append(size)
            
        for file_name,file_size in zip(interested_objs,interested_objs_size):
          print ('{} & {}'.format(file_name,file_size))
       
    except Exception as e:
        print (e)
    else:
      return sum(interested_objs_size),sum(uninterested_objs_size)
    
###########################################################################################################
### Get size in bytes or MB or GB or TB                                                                 ###                                               
###########################################################################################################    
def memory_size(size,bytes_format):
  size = size*1.0
  if 'T' in bytes_format:
    return round(size/(1073741820*1024),4)
  elif 'G' in bytes_format:
    return round(size/(1048576*1024),4)
  elif 'M' in bytes_format:
    return round(size/(1024*1024),4)
  else:
    return round(size/1024,4)

# COMMAND ----------

###########################################################################################################
###                Calculate size for s3_dir (incremental i/p data)                                     ###
###########################################################################################################
try:
###########################################################################################################
  # Get the individual directory of the stage for each source
  STEP = 'Calculate size for s3_dir.'
  if s3_dir == '':
    print ("no s3_dir path given")
    total_inc_size = 0
  else:
    bucket_name, prefix_path = get_path_details(s3_dir)
    input_data_size,archive_size = get_data_size(bucket_name,prefix_path)
    total_data_stored = input_data_size + archive_size
    print ("size of input files in s3 location (in bytes): ", input_data_size)
  #   print (archive_size)
  #   print (total_data_stored)
    total_inc_size =  memory_size(input_data_size,'GB')
  #   total_archive_size =  memory_size(archive_size,'GB')
  #   total_data_size =  memory_size(total_data_stored,'GB')
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                Calculate size for flags data                                                        ###
###########################################################################################################
try:
###########################################################################################################
  # Get the individual directory of the stage for each source
  STEP = 'Calculate size for flags location.'
#   print (delta_flags_write)
  if delta_flags_write == '':
    print ("no delta_flags_write path given")
    total_flags_size = 0
  else:
    bucket_name, prefix_path = get_path_details(delta_flags_write)
    input_data_size,archive_size = get_data_size(bucket_name,prefix_path)
    total_data_stored = input_data_size + archive_size
    print ("size of files in flags location (in bytes): ", input_data_size)

    total_flags_size =  memory_size(input_data_size,'GB')
###########################################################################################################
except Exception as e:
  print (e)
#   exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                         Calculate size for intermediate data                                        ###
###########################################################################################################
try:
###########################################################################################################
  # Get the individual directory of the stage for each source
  STEP = 'Calculate size for intermediate location.'
  if delta_intermediate == '':
    print ("no delta_intermediate path given")
    total_intermediate_size = 0
  else:
    bucket_name, prefix_path = get_path_details(delta_intermediate)
    input_data_size,archive_size = get_data_size(bucket_name,prefix_path)
    total_data_stored = input_data_size + archive_size
    print ("size of files in intermediate location (in bytes): ", input_data_size)

    total_intermediate_size =  memory_size(input_data_size,'GB')
    print (total_intermediate_size)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###  Metrics for Logging the Stats and creating the Dictionary to be send as output of run              ###
###########################################################################################################
try:
  STEP = 'Create the metrics.'
  metric_dict["total_inc_size"] = total_inc_size
  metric_dict["total_flags_size"] = total_flags_size
  metric_dict["total_intermediate_size"] = total_intermediate_size
  metric_dict["total_final_size"] = total_intermediate_size
  metric_dict["STATUS"] = 'SUCCESS'
  metric_dict["STEP_ERROR"] = 'CALCULATOR_END#NO_ERROR'
  metric_json = json.dumps(metric_dict)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

dbutils.notebook.exit(metric_json)

# COMMAND ----------

