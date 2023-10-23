# Databricks notebook source
# -*- coding: UTF-8 -*-
#**This module contains the dependencies, functions, and data quality (DQ) rules required for the notebooks that process Enrichment data. Please refer to Enrichment documentation for DQ rule details: #**https://hp.sharepoint.com/:w:/s/DashboardsandEnrichmentScrum/EYjzBGsk_HNGmrHLvitQyUEBUGqMUzOXStNH1ySQTFgNqw?e=FhcApR

#**Planned Future Changes**
#- 2/27/18: Add material number back into channel_shipments flags table, ETA: TBD
#- 5/25/18: Return tuple from DQ/DM for multiple column change

# COMMAND ----------

# Import relevant libraries


# part of STL 
import time
from datetime import datetime
import json
from collections import OrderedDict
import math
import hashlib
from email.mime.application import MIMEApplication
from functools import reduce
from subprocess import call
import psycopg2
import fnmatch
import re
import ast

# 3rd party packages (by spark)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col, to_date, from_unixtime, unix_timestamp, substring, udf, size, coalesce, asc, desc, length, lit, current_timestamp, input_file_name, concat, rank, row_number, pandas_udf, PandasUDFType
from pyspark.sql import functions as F


# COMMAND ----------

RELATIVE_PATH_CREDENTIALS_FOR_ENTERPRISE = "../libraries/credentials_for_enterprise"
CREDENTIALS_FOR_ENTERPRISE_TIMEOUT = 600

# COMMAND ----------

# This is a list of characters that we filter into N/A for consistency in mod_list for DM14
unwanted_chars = ['??', 'N/A']

# List of invalid characters that filters a value into NULL for DM24
# The character are: 8220, 37, 710, 8240, 129, 8482, 141, 187, 161, 173, 174, 8218, 8221, 169, 171, 182, 177, 178, 179, 188, 189, 190, 191, 185, 124, 170, 186
invalid_chars_regex = "[“%ˆ‰™»¡­®‚”©«¶±²³¼½¾¿¹|ªº]"

#List of unknown values that filters a value into NULL for DM28 (empty string is handled in the UDF because "" doesn't work here)
unknown_list = ["Unknown", "UNKNOWN", "unknown", "?", "NULL", "#N/A, UNFILTERED FOR SOME MEASURES", "No Zip"]

# COMMAND ----------

#UDF to append errors to error_list, create comma delimited field of errors
udf_concat_list = udf(lambda *args: ','.join([i for i in args if i != None and i != ""]), StringType())

# Avoid the more than 255 arguments failure experienced in the Java eval of Python code with many check_dm cols
udf_concat_large_list = udf(lambda col_list: ','.join([i for i in col_list if i is not None and i != ""]), StringType())

#UDF to append new flags to existing list. Takes an unlimited amount of new flags.
udf_split_concat_list = udf(
  lambda x,*y: ','.join(
    [i for i in x.split(',') + [i for i in y] if i is not None and i != '']
  ), StringType())

#define step_error_code function
def step_error_code(value):
  return F.when((value == "") | F.isnull(value), F.lit(0)).otherwise(F.lit(1))


# COMMAND ----------

############### AUXILIARY FUNCTIONS ####################
def get_df_from_redshift(table_name, evnt_db):
  
  creds_str = dbutils.notebook.run(RELATIVE_PATH_CREDENTIALS_FOR_ENTERPRISE, CREDENTIALS_FOR_ENTERPRISE_TIMEOUT, {"envt_dbuser":evnt_db})
  creds = ast.literal_eval(creds_str)

  jdbc_url           = creds['rs_jdbc_url']
  iam                = creds['aws_iam']
  tempS3Dir          = creds['tmp_dir']
    
  df =  spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("tempdir", tempS3Dir) \
        .option("aws_iam_role", iam) \
        .load()
  
  return df


def get_rs_errors(dbname, user, pwd, host, port, tmp_bucket):
  
  conn = psycopg2.connect(dbname=dbname, user=user, password=pwd, host=host, port=port, sslmode ='require')
  cur=conn.cursor()
  query_id = ("select * from stl_load_errors where filename like '%s%s' order by starttime desc") % (tmp_bucket, '%')
  cur.execute(query_id)

  get_id = cur.fetchone()

  if get_id:
    x = get_id[5]

    cur.execute("select * from stl_load_errors where query = %s" % x)
    rs_load_errors = cur.fetchall()
    conn.close()
    return bool(get_id), rs_load_errors
  
  print("no RS load errors")
  conn.close()
  return bool(get_id), None

# COMMAND ----------

def get_biid_py3(uuid):
  for char in uuid:
    if not 0 < ord(char) < 128:
      return "N/A"
  message = bytes(uuid,'utf-8')
  secret = bytes("12a244f3c74c4134716944ab5caf73722c564ddc36d54a66c071434911a2f321293beeb41c271501fb059d4d77c55407408ba89228670bfa5cc6410b886363e3",'utf-8')
  signature = base64.b16encode(hmac.new(secret, message, digestmod=hashlib.sha256).digest()).lower()
  signature = str(signature).split("'")[1]
  return signature

#If biid has a non-ascii char, return N/A
def get_skey_py3(biid):
  if biid == "N/A":
    return "N/A"
  message = bytes(biid,'utf-8')
  secret = bytes("089F81F753F54A6B94931DD3DCD1A0976184D4DB7F481437A5EA5F6258D70958",'utf-8')
  signature = base64.b16encode(hmac.new(secret, message, digestmod=hashlib.sha256).digest()).lower()
  signature = str(signature).split("'")[1]
  return signature

# COMMAND ----------

def step_dq9_rank(df, partition_list, sort_list):
  window = Window.partitionBy(partition_list).orderBy(sort_list)
  df_result = df.select('*', row_number().over(window).alias('ranked'))
  return df_result

def udf_step_dq9(ranked):
  return F.when(ranked != 1, "DQ9").otherwise(None)

# Purpose - returns a DQ if values contain a non-ascii character
# Inputs - value (string)
#   value == the value(s) you are checking for non-ascii characters. Use concat() to check multiple values in the row (ex. udf_dq10_ascii(concat(df.col1, df.col2))
def udf_dq10_ascii(val):
  return F.when(F.regexp_extract(val, '^[\x00-\x7F]+$', 0) == "", "DQ10") \
    .otherwise(None)

# COMMAND ----------

# DSR RULES
def get_sn_blacklist_dsr_automation(environment):
  
  cols_ls = ["serial_number", "product_number"]
  
  if environment.lower() == "dev":
    table_name = "data_privacy.dsr_sn_blacklist_enrich"
    evnt_db = 'd3'
  elif environment.lower() == "itg":
    table_name = "data_privacy.dsr_sn_blacklist_enrich"
    evnt_db = 'i3'
  elif environment.lower() == "prod":
    table_name = "data_privacy.dsr_sn_blacklist_enrich"
    evnt_db = '2p3'
    
  df = get_df_from_redshift(table_name,evnt_db)
  
  return [(i.serial_number, i.product_number) for i in df.select(*cols_ls).collect()]

def get_cid_blacklist_dsr_automation(environment):

  cols_ls = ["cid"]

  if environment.lower() == "dev":
    table_name = "data_privacy.dsr_cid_blacklist_enrich"
    evnt_db = 'd3'
  elif environment.lower() == "itg":
    table_name = "data_privacy.dsr_cid_blacklist_enrich"
    evnt_db = 'i3'
  elif environment.lower() == "prod":
    table_name = "data_privacy.dsr_cid_blacklist_enrich"
    evnt_db = '2p3'

  df = get_df_from_redshift(table_name,evnt_db)

  return [(i.cid) for i in df.select(*cols_ls).collect()]

def get_cid_blacklist(environment):
  
  cols_ls = ["cid"]
  
  if environment.lower() == "dev":
    table_name = "data_privacy.dsr_cid_blacklist_ref_prod"
    evnt_db = 'd3'
  elif environment.lower() == "itg":
    table_name = "data_privacy.dsr_cid_blacklist_ref_prod"
    evnt_db = 'i3'
  elif environment.lower() == "prod":
    table_name = "data_privacy.dsr_cid_blacklist_ref_prod"
    evnt_db = '2p3'
    
  df = get_df_from_redshift(table_name,evnt_db)
  
  return [(i.cid) for i in df.select(*cols_ls).collect()]


def step_dq998(val1, val2, blacklist):
  
  for i in blacklist.value:
    if (val1 == "" or val1 == None or val1 == "NULL" or val1 == "null") \
    or (val2 == "" or val2 == None or val2 == "NULL" or val2 == "null"):
      return None
    elif val1 == i[0] and val2 == i[1]:
      return "DQ998_serial_number"
  return None

def udf_step_dq998_sn(blacklist): 
  return udf(lambda x,y: step_dq998(x,y, blacklist), StringType())

# COMMAND ----------

def udf_step_dq999_cid(val):
  return F.when((F.isnull(val)) | (val == ""), None) \
          .when(val.isin(b_cid_blacklist.value), "DQ999_cid") \
          .otherwise(None)

# COMMAND ----------

def udf_dq1_sn_new(val,regex):
  return F.when(F.isnull(val), "DQ1") \
    .when((F.length(val) < 10) | (F.length(val) > 14), "DQ1") \
    .when((val.substr(0,10) == "0000000000") | (val.substr(0,10) == "XXXXXXXXXX"), "DQ1") \
    .when(F.regexp_extract(val, regex, 0)== "", "DQ1") \
    .otherwise(None)

# Purpose - returns a DQ for an invalid serial number. Replaced with DQ34,DQ35.
# Inputs - serial_orig (string)
#   serial_orig == the serial number you are checking 
def dq1_sn_old(serial_orig):
  return F.when(F.isnull(serial_orig), "DQ1") \
    .when((F.length(serial_orig) < 10) | (F.length(serial_orig) > 14), "DQ1") \
    .when((serial_orig.substr(0,10) == "0000000000") | (serial_orig.substr(0,10) == "XXXXXXXXXX"), "DQ1") \
    .when((serial_orig.contains("\"")) | (serial_orig.contains(" ")) | (serial_orig.contains(".")) | (serial_orig.contains("*")), "DQ1") \
    .otherwise(None)

def udf_dq2_len(printer_uuid, source_file):
  return F.when((F.length(printer_uuid) != 36) & (F.regexp_extract(F.lower(source_file), "caspian", 0) == ""), "DQ2") \
      .when((printer_uuid.contains(" ")) | (printer_uuid.contains("\"")), "DQ2") \
      .otherwise(None)

# COMMAND ----------

# Purpose - returns a DM for an serial number not equal to a length of 10
# Inputs - serial_orig (string)
#   serial_orig == the serial number you are checking for a length of 10
def udf_dm4_sn(serial_orig):
  return F.when(F.length(serial_orig) != 10, "DM4") \
    .otherwise(None)

# COMMAND ----------

# Purpose - returns first 10 characters of serial number
def udf_mod_dm4_sn(serial_orig):
  return F.when(F.length(serial_orig) > 10, F.substring(serial_orig, 1, 10)) \
    .otherwise(serial_orig)

# COMMAND ----------

#Purpose - returns a DM if there is NO match with our supplied regex
def step_dm61_translate(value, regex, field):
  return F.when(F.regexp_extract(value, regex, 0) != "", None) \
    .otherwise(concat(lit("DM61_"), field))

#Purpose - returns a None if there is NO match with our supplied regex
def mod_dm61_translate(val, regex):
  return F.when(F.regexp_extract(val, regex, 0) != "", val) \
    .otherwise(None)

#Purpose - returns a DM if there is a match with our supplied regex
def step_dm62_complement_translate(value, regex, field): 
  return F.when(F.regexp_extract(value, regex, 0) == "", None) \
    .otherwise(concat(lit("DM62_"), field))

# COMMAND ----------

#Purpose - returns a None if there is a match with our supplied regex
def udf_mod_dm62_complement_translate(value, regex):
  return F.when(F.regexp_extract(value, regex, 0) == "", value) \
    .otherwise(None)

# COMMAND ----------

#Purpose - returns a DM if value starts with a given substring
def step_dm65_startswith(value, substr, field, dm_flag):
  return F.when(dm_flag.startswith("DM65"), dm_flag).when(value.startswith(substr), concat(lit("DM65_"), field)) \
    .otherwise(None)

# COMMAND ----------

#Purpose - returns a None if value starts with a given substring
def udf_mod_dm65_startswith(value, substr):
  return F.when(value.startswith(substr), None) \
    .otherwise(value)

# COMMAND ----------

#Purpose - returns a DM if column starts with " or ends with "
def step_dm58_quotes(val, field):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(val.startswith("\"") | val.endswith("\"")|(val.contains("\"")) | (val.contains(" ")) | (val.contains("$")), concat(lit("DM58_"), field)) \
    .when((val == "NULL") | (val == ""), concat(lit("DM58_"), field)) \
    .otherwise(None)

# Purpose - returns group 2 of regex, removes surrounding quotes
def udf_mod_dm58_quotes(val):
  return F.regexp_extract(val, "^(\"*)(.*?)(\"*)$", 2)

# COMMAND ----------

# Purpose - returns a DQ for an invalid product number
# Inputs - product_number (string)
#   product_number == the product number you are checking for validity
def udf_dq3_pn(product_number):
  return F.when(F.isnull(product_number), "DQ3") \
    .when((product_number == "NULL") | (product_number == ""), "DQ3") \
    .when((product_number.contains("\"")) | (product_number.contains(" ")) | (product_number.contains("$")), "DQ3") \
    .otherwise(None)

# Purpose - returns a DM for an serial number not equal to a length of 10
# Inputs - serial_orig (string)
#   serial_orig == the serial number you are checking for a length of 10
def dm4_sn(serial_orig):
  return F.when(F.length(serial_orig) != 10, "DM4") \
    .otherwise(None)


# COMMAND ----------

# suffix "_chk" indicates generic check
# Purpose - returns a DQ if value is not within the given range, or is not equal to the given length
# Inputs - value (string), field (string), length (int), min_val (int), max_val (int)
# ex: df.withColumn("check_dq34_serial_number", udf_dq34_length_chk(df.serial_number, lit("serial_number"), lit(10), lit(None), lit(None)))
def udf_dq34_length_chk(val, field, length, min_val, max_val):
  return F.when((F.length(val) > min_val) & (F.length(val) < max_val), None) \
    .when((F.length(val) == length), None) \
    .otherwise(concat(lit("DQ34_"),field))

# suffix "_chk" indicates generic check
# Purpose - returns a DQ if value doesn't match supplied regex pattern
# Inputs - value (string), field name (string), pattern (string)
def udf_dq35_regex_chk(val, field, pattern):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(F.regexp_extract(val, pattern, 0) != "", None) \
    .otherwise(concat(lit("DQ35_"),field))

# COMMAND ----------

# Purpose - returns a DM for values that are in the unknown list (above)
# Inputs - many possible values (string)    
def dm28_unknown(value, dm_flag):
  return F.when((dm_flag == "DM28") | (value == ""), "DM28") \
    .when(F.regexp_extract(value, b_regex_unknown_list.value, 0) != "", "DM28") \
    .otherwise(None)

# Purpose - returns NULL for values that are in the unknown list (above)
# Inputs - many possible values (string)
def mod_dm28(value):
  return F.when(value == "", None) \
    .when(F.regexp_extract(value, b_regex_unknown_list.value, 0) != "", None) \
    .otherwise(value)

def mod_dm29(column, str_to_replace, value):
  return F.when(column == str_to_replace, value) \
    .otherwise(column)
# Purpose - performs a lookup replacement on given dataframe and replacement pairs
# Note: similar to DM43 above except the original value is passed through (retained) when there isn't a match
def mod_dm43_decode_pass_through(df, replacements, field):
  for rule in replacements:
    df = df.withColumn(field, mod_dm29(df[field], rule[0], rule[1]))
  return df

def step_dm43_decode(val, valid_list, field):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(val.isin(valid_list), concat(lit("DM43_"), field)) \
    .otherwise(concat(lit("DM43_"), field))

def step_dm43_decode_pass_through(val, valid_list, field):
  return F.when((F.isnull(val)) | (val == ''), None) \
    .when(val.isin(valid_list), concat(lit("DM43_"), field)) \
    .otherwise(None)

# COMMAND ----------

# Purpose - To compare two date/timestamp columns	
#returns a DQ if a date1 is greater than date2.	
# Inputs - dates are of the Java form (string with 'yyyy-MM-dd')	
#   date_col1 == date/timestamp column you want to compare	
#   date_col2 == date/timestamp column you want to compare	
def dq71_compare_dts(date_col1, date_col2,field1,field2):	
  return F.when((((date_col1.isNull()) & (date_col2.isNotNull())) | ((date_col1.isNotNull()) & (date_col2.isNotNull()) &(date_col1>date_col2))),concat(lit("DQ71_"),field1,lit("_"),field2)) \
                .otherwise(lit(None))

# COMMAND ----------

def step_dq72(date_str,field):	
  date_str = date_str.split(',')[0]	
  pattern = '^\d{4}(\-)(((0)[1-9])|((1)[0-2]))(\-)(((0)[0-9])|[1-2][0-9]|(3)[0-1])(\ )(((0)[0-9])|((1)[0-9])|(2)[0-4])(\:)(((0)[0-9])|([1-5][0-9]))(\:)(((0)[0-9])|([1-5][0-9]))'	
  pattern_match = re.compile(pattern)	
  flag = "DQ72_" +  field	
  if pattern_match.search(date_str) == None:	
    return flag	
  else:	
    year = int(date_str[0:4])	
    mon = int(date_str[5:7])	
    day = int(date_str[8:10])	
    if day == 31 and (mon in [4,6,9,11]):	
      return flag	
    elif (day >= 30 and mon == 2):	
      return flag 	
    elif (mon == 2 and day == 29 and not (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))):	
      return flag	
    else:	
      return None	
  	
udf_step_dq72 = udf(lambda x,y: step_dq72(x,y), StringType())

# COMMAND ----------

#Purpose, Inputs, Value
def dq70_nullcheck(colValue,field):
  return F.when(F.isnull(colValue), concat(lit("DQ70_"),field)) \
    .when((colValue == "null")|(colValue == "NULL")|(colValue == None) | (colValue == ""), concat(lit("DQ70_"),field)) \
    .otherwise(None)

# COMMAND ----------

#Purpose, Inputs, Value
def dq94_nullcheck(colValue1,colValue2,field):
  return F.when(F.isnull(colValue1) & F.isnull(colValue2), concat(lit("DQ94_"),field)) \
    .when(((colValue1 == "null") & (colValue2 == "null"))|((colValue1 == "NULL") & (colValue2 == "NULL"))|((colValue1 == None) & (colValue2 == None)) | ((colValue1 == "") & (colValue2 == "")), concat(lit("DQ94_"),field)) \
    .otherwise(None)

# COMMAND ----------

def step_dq73(date_str, field, pattern,expected_format):
	
  # for any new expected_format, the have to be added to the if condition to be handled.
  #  the function itself is not generic, but over time when all usecases encountered are added ,will make it generic enough.


  standard_months_vocab = ['jan', 'feb', 'mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
  standard_months_num = range(1,13)
  months_with_30days_vocab = [standard_months_vocab[3], standard_months_vocab[5], standard_months_vocab[8], standard_months_vocab[10]]

  flag = "DQ73_" +  field
  date_str = str(date_str)

  pattern_match = re.compile(pattern)

  if pattern_match.match(date_str) == None: 
    return flag
  else:
		# if condition where the format needs to be added when a new usecase is encountered
    if expected_format == 'yyyy-MM-dd HH:mm:ss.SSS':
      year_floor_indx,year_ceiling_indx = 0,4
      month_floor_indx,month_ceiling_indx = 5,7
      day_floor_indx,day_ceiling_indx = 8,10
      mon = int(date_str[month_floor_indx:month_ceiling_indx])
    elif expected_format == 'dd-MMM-yy':
      year_floor_indx,year_ceiling_indx = -2,len(date_str)
      month_floor_indx,month_ceiling_indx = 3,6
      day_floor_indx,day_ceiling_indx = 0,2
      mon = str(date_str[month_floor_indx:month_ceiling_indx]).lower()
    elif expected_format == 'yyyy-MM-dd':
      year_floor_indx,year_ceiling_indx = 0,4
      month_floor_indx,month_ceiling_indx = 5,7
      day_floor_indx,day_ceiling_indx = 8,10
      mon = int(date_str[month_floor_indx:month_ceiling_indx])
    elif expected_format == 'yyyy-MM-dd HH:mm:ss':
      year_floor_indx,year_ceiling_indx = 0,4
      month_floor_indx,month_ceiling_indx = 5,7
      day_floor_indx,day_ceiling_indx = 8,10
      mon = int(date_str[month_floor_indx:month_ceiling_indx])
    else :
      year_floor_indx,year_ceiling_indx = 0,4
      month_floor_indx,month_ceiling_indx = 5,7
      day_floor_indx,day_ceiling_indx = 8,10
      mon = int(date_str[month_floor_indx:month_ceiling_indx])

    year,day = int(date_str[year_floor_indx:year_ceiling_indx]),int(date_str[day_floor_indx:day_ceiling_indx])

    if day == 31 and (mon in [4,6,9,11] or mon in months_with_30days_vocab):
      return flag
    elif (day >= 30 and (mon == 2 or mon == standard_months_vocab [1])):
      return flag 
    elif ((mon == 2 or mon == standard_months_vocab [1])  and day == 29 and not (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))):
      return flag
    else:
      return None

udf_step_dq73_check = udf(lambda x,y,r,s: step_dq73(x,y,r,s), StringType())

# COMMAND ----------

# Check one column for some value and being Null
def dq92_compare_value(val1, val2, field1, field2, check_val):
  return F.when((val1 == check_val) & ((val2 == "") | (val2.isNull()) | (val2 == "NULL") | (val2 == "null")), concat(lit("DQ92_"),field1,lit("_"),field2)) \
		  .otherwise(None)

# COMMAND ----------

def dq91_not_equal_length_chk(val, field, length):
  return F.when((F.length(val) != length), concat(lit("DQ91_"),field)) \
		  .otherwise(None)