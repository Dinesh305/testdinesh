# Databricks notebook source
# -*- coding: UTF-8 -*-
#**This module contains the dependencies, functions, and data quality (DQ) rules required for the notebooks that process Enrichment data. Please refer to Enrichment documentation for DQ rule details: #**https://hp.sharepoint.com/:w:/s/DashboardsandEnrichmentScrum/EYjzBGsk_HNGmrHLvitQyUEBUGqMUzOXStNH1ySQTFgNqw?e=FhcApR

#**Planned Future Changes**
#- 2/27/18: Add material number back into channel_shipments flags table, ETA: TBD
#- 5/25/18: Return tuple from DQ/DM for multiple column change

# COMMAND ----------

# MAGIC %run ./credentials_for_enrichment

# COMMAND ----------

# Import relevant libraries

from pyspark.sql.types import *
import time
from datetime import datetime
from pyspark.sql.functions import regexp_replace, col, to_date, from_unixtime, unix_timestamp, substring, udf, size, coalesce, asc, desc, length, lit, current_timestamp, input_file_name, concat, rank, row_number, pandas_udf, PandasUDFType
from pyspark.sql import functions as F
from subprocess import call
import psycopg2
import fnmatch
import re
import ast
from pyspark.sql.window import Window
import json
from collections import OrderedDict
import math
import hashlib
from email.mime.application import MIMEApplication
from functools import reduce

# COMMAND ----------

# This is a list of characters that we filter into N/A for consistency in mod_list for DM14
unwanted_chars = ['??', 'N/A']

# List of invalid characters that filters a value into NULL for DM24
# The character are: 8220, 37, 710, 8240, 129, 8482, 141, 187, 161, 173, 174, 8218, 8221, 169, 171, 182, 177, 178, 179, 188, 189, 190, 191, 185, 124, 170, 186
invalid_chars_regex = "[“%ˆ‰™»¡­®‚”©«¶±²³¼½¾¿¹|ªº]"

#List of unknown values that filters a value into NULL for DM28 (empty string is handled in the UDF because "" doesn't work here)
unknown_list = ["Unknown", "UNKNOWN", "unknown", "?", "NULL", "#N/A, UNFILTERED FOR SOME MEASURES", "No Zip"]

# COMMAND ----------

#Get list of accepted country codes, for DQ11
def get_country_list(environment):
  if environment.lower() == "dev":
    url = dev_jdbc_url_enrich
    tmp = dev_tmp
    iam = dev_iam
  elif environment.lower() == "itg":
    url = itg_jdbc_url_enrich
    tmp = itg_tmp
    iam = itg_iam
  else:
    url = pro02_jdbc_url_enrich
    tmp = prod_tmp
    iam = prod_iam
  df_country_ref = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", url) \
    .option("dbtable", "ref_enrich.country_ref") \
    .option("tempdir", tmp) \
    .option("aws_iam_role", iam) \
    .load()
  country_list = df_country_ref.select("iso_country_code").rdd.flatMap(lambda x: x).collect()
  return country_list

#Get list of accepted platform subset names
def get_platform_subset_name_list(environment):
  if environment.lower() == "dev":
    url = dev_jdbc_url_ref
    tmp = dev_tmp
    iam = dev_iam
  elif environment.lower() == "itg":
    url = itg_jdbc_url_ref
    tmp = itg_tmp
    iam = itg_iam
  else:
    url = pro02_jdbc_url_ref
    tmp = prod_tmp
    iam = prod_iam
  df_platform_subset_name_ref = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", url) \
    .option("dbtable", "ref_enrich.rdma_product_ref") \
    .option("tempdir", tmp) \
    .option("aws_iam_role", iam) \
    .load()
  platform_subset_name = df_platform_subset_name_ref.select("pltfrm_subset_nm").rdd.flatMap(lambda x: x).collect()
  return platform_subset_name

#Get df for channel_patterns, for DM16
def get_channel_patterns(environment):
  if environment.lower() == "dev":
    path = "s3a://hp-bigdata-dev-enrichment/ink_laser/channel_patterns/"
  elif environment.lower() == "itg":
    path = "s3a://hp-bigdata-itg-enrichment/ink_laser/channel_patterns/"
  else:
    path = "s3a://hp-bigdata-prod-enrichment/ink_laser/channel_patterns/"
  df_channel_patterns = spark.read.parquet(path)
  channel_patterns = df_channel_patterns.rdd.map(lambda x: (x.startswith, x.standardized)).collect()
  return channel_patterns

#Get list of business patterns for DQ17
def get_patterns(environment):	
  if environment.lower() == "dev":	
    path = "s3a://hp-bigdata-dev-enrichment/ink_laser/patterns/"	
  elif environment.lower() == "itg":	
    path = "s3a://hp-bigdata-itg-enrichment/ink_laser/patterns/"
  else:
    path = "s3a://hp-bigdata-prod-enrichment/ink_laser/patterns/"
  df_business_patterns = spark.read.parquet(path)	
  business_patterns = df_business_patterns.select("Pattern_Flag").rdd.flatMap(lambda x: x).collect()	
  return business_patterns

#Get list of business patterns for DQ19
def get_business_patterns(environment):
  if environment.lower() == "dev":
    path = "s3a://hp-bigdata-dev-enrichment/ink_laser/business_patterns/"
  elif environment.lower() == "itg":
    path = "s3a://hp-bigdata-itg-enrichment/ink_laser/business_patterns/"
  else:
    path = "s3a://hp-bigdata-prod-enrichment/ink_laser/business_patterns/"
  df_business_patterns = spark.read.parquet(path)
  business_patterns = df_business_patterns.select("business_patterns").rdd.flatMap(lambda x: x).collect()
  return business_patterns

#get dict of regex paterns for DM15
def get_regex_patterns(environment):
  if environment.lower() == "dev":
    url = dev_jdbc_url_enrich
    tmp = dev_tmp
    iam = dev_iam
  elif environment.lower() == "itg":
    url = itg_jdbc_url_enrich
    tmp = itg_tmp
    iam = itg_iam
  else:
    url = pro02_jdbc_url_enrich
    tmp = prod_tmp
    iam = prod_iam
  df_regex = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", url) \
    .option("dbtable", "ref_enrich.country_extended_intel_ref") \
    .option("tempdir", tmp) \
    .option("aws_iam_role", iam) \
    .load()
  postal_code_regex = df_regex.select("iso", "postal_code_regex")
  
  regex_dict = dict((postal_code_regex.rdd.map(lambda x: (x.iso, x.postal_code_regex)).collect()))
  return {k:(v if v is not None else '') for k,v in regex_dict.items()}

#get list of state codes for DM22
def get_state_codes(*args):
  state_codes = ["AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY", "AS", "FM", "MH", "MP", "PR", "PW", "VI", "GU", "AE", "AA", "AE", "AE", "AE", "AP", "UK"]
  return state_codes

#get dict of location_cd:location_desc for DM40
def get_location_cd(environment):
  if environment.lower() == "dev":
    path = "s3a://hp-bigdata-dev-enrichment/ink_laser/location_cd/location_cd.csv"
  elif environment.lower() == "itg":
    path = "s3a://hp-bigdata-itg-enrichment/ink_laser/location_cd/location_cd.csv"
  else:
    path = "s3a://hp-bigdata-prod-enrichment/ink_laser/location_cd/location_cd.csv"
  df = spark.read.format("com.databricks.spark.csv").option("header","true").option("sep", ",").load(path)
  df_dict = dict(df.collect())
  return {k:(v if v is not None else '') for k,v in df_dict.items()}

#get dict of localization_cd:localization_desc for DM40
def get_localization_cd(environment):
  if environment.lower() == "dev":
    path = "s3a://hp-bigdata-dev-enrichment/ink_laser/localization_cd/localization_cd.csv"
  elif environment.lower() == "itg":
    path = "s3a://hp-bigdata-itg-enrichment/ink_laser/localization_cd/localization_cd.csv"
  else:
    path = "s3a://hp-bigdata-prod-enrichment/ink_laser/localization_cd/localization_cd.csv"
  df = spark.read.format("com.databricks.spark.csv").option("header","true").option("sep", ",").load(path)
  df_dict = dict(df.collect())
  return {k:(v if v is not None else '') for k,v in df_dict.items()}

#get dict of country_code:currency_code for DM55
def get_currency_codes(environment):
  if environment.lower() == "dev":
    url = dev_jdbc_url_enrich
    tmp = dev_tmp
    iam = dev_iam
  elif environment.lower() == "itg":
    url = itg_jdbc_url_enrich
    tmp = itg_tmp
    iam = itg_iam
  else:
    url = pro02_jdbc_url_enrich
    tmp = prod_tmp
    iam = prod_iam
  df_currency = spark.read \
    .format("com.databricks.spark.redshift") \
    .option("url", url) \
    .option("dbtable", "ref_enrich.country_extended_intel_ref") \
    .option("tempdir", tmp) \
    .option("aws_iam_role", iam) \
    .load()
  currency_codes = df_currency.select(F.col("iso").alias("country_code"), "currency_code")
  currency_codes_dict = dict((currency_codes.rdd.map(lambda x: (x.country_code, x.currency_code)).collect()))
  return currency_codes_dict

def get_sn_blacklist(environment):
  if environment.lower() == "dev":
    url = dev_jdbc_url_dsr
    tmp = dev_tmp
    iam = dev_iam
    table_name = "data_privacy.dsr_sn_blacklist_ref_prod"
  elif environment.lower() == "itg":
    url = itg_jdbc_url_dsr
    tmp = itg_tmp
    iam = itg_iam
    table_name = "data_privacy.dsr_sn_blacklist_ref"
  elif environment.lower() == "prod":
    url = pro02_jdbc_url_dsr
    tmp = prod_tmp
    iam = prod_iam
    table_name = "data_privacy.dsr_sn_blacklist_ref"
  df_sn = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("tempdir", tmp) \
        .option("aws_iam_role", iam) \
        .load()
  return [(i.serial_number, i.product_number) for i in df_sn.select("serial_number", "product_number").collect()]

def get_cid_blacklist(environment):
  if environment.lower() == "dev":
    url = dev_jdbc_url_dsr
    tmp = dev_tmp
    iam = dev_iam
    table_name = "data_privacy.dsr_cid_blacklist_ref_prod"
  elif environment.lower() == "itg":
    url = itg_jdbc_url_dsr
    tmp = itg_tmp
    iam = itg_iam
    table_name = "data_privacy.dsr_cid_blacklist_ref"
  elif environment.lower() == "prod":
    url = pro02_jdbc_url_dsr
    tmp = prod_tmp
    iam = prod_iam
    table_name = "data_privacy.dsr_cid_blacklist_ref"

  df_sn = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("tempdir", tmp) \
        .option("aws_iam_role", iam) \
        .load()
  return [(i.cid) for i in df_sn.select("cid").collect()]
  
# get DSR records

def get_sn_blacklist_dsr_automation(environment):
  if environment.lower() == "dev":
    url = dev_jdbc_url_dsr
    tmp = dev_tmp
    iam = dev_iam
    table_name = "data_privacy.dsr_sn_blacklist_enrich"
  elif environment.lower() == "itg":
    url = itg_jdbc_url_dsr
    tmp = itg_tmp
    iam = itg_iam
    table_name = "data_privacy.dsr_sn_blacklist_enrich"
  elif environment.lower() == "prod":
    url = pro02_jdbc_url_dsr
    tmp = prod_tmp
    iam = prod_iam
    table_name = "data_privacy.dsr_sn_blacklist_enrich"
  df_sn = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("tempdir", tmp) \
        .option("aws_iam_role", iam) \
        .load()
  return [(i.serial_number, i.product_number) for i in df_sn.select("serial_number", "product_number").collect()]

def get_cid_blacklist_dsr_automation(environment):
  if environment.lower() == "dev":
    url = dev_jdbc_url_dsr
    tmp = dev_tmp
    iam = dev_iam
    table_name = "data_privacy.dsr_cid_blacklist_enrich"
  elif environment.lower() == "itg":
    url = itg_jdbc_url_dsr
    tmp = itg_tmp
    iam = itg_iam
    table_name = "data_privacy.dsr_cid_blacklist_enrich"
  elif environment.lower() == "prod":
    url =pro02_jdbc_url_dsr
    tmp = prod_tmp
    iam = prod_iam
    table_name = "data_privacy.dsr_cid_blacklist_enrich"
  df_sn = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("tempdir", tmp) \
        .option("aws_iam_role", iam) \
        .load()
  return [(i.cid) for i in df_sn.select("cid").collect()]

# COMMAND ----------

# Purpose - returns a DQ for an invalid serial number. Replaced with DQ34,DQ35.
# Inputs - serial_orig (string)
#   serial_orig == the serial number you are checking 
def udf_dq1_sn(serial_orig):
  return F.when(F.isnull(serial_orig), "DQ1") \
    .when((F.length(serial_orig) < 10) | (F.length(serial_orig) > 14), "DQ1") \
    .when((serial_orig.substr(0,10) == "0000000000") | (serial_orig.substr(0,10) == "XXXXXXXXXX"), "DQ1") \
    .when((serial_orig.contains("\"")) | (serial_orig.contains(" ")) | (serial_orig.contains(".")) | (serial_orig.contains("*")), "DQ1") \
    .otherwise(None)

# Purpose - returns a DQ for invalid printer_uuid
# Inputs - printer_uuid, source_file
def udf_dq2_len(printer_uuid, source_file):
  return F.when((F.length(printer_uuid) != 36) & (F.regexp_extract(F.lower(source_file), "caspian", 0) == ""), "DQ2") \
      .when((printer_uuid.contains(" ")) | (printer_uuid.contains("\"")), "DQ2") \
      .otherwise(None)
      
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
def udf_dm4_sn(serial_orig):
  return F.when(F.length(serial_orig) != 10, "DM4") \
    .otherwise(None)

# Purpose - returns a DQ if values contain a non-ascii character
# Inputs - value (string)
#   value == the value(s) you are checking for non-ascii characters. Use concat() to check multiple values in the row (ex. udf_dq10_ascii(concat(df.col1, df.col2))
def udf_dq10_ascii(val):
  return F.when(F.regexp_extract(val, '^[\x00-\x7F]+$', 0) == "", "DQ10") \
    .otherwise(None)

#Created by RCB Enrichment Team
def udf_dq10_ascii_check(val, field):
  return F.when(F.regexp_extract(val, '^[\x00-\x7F]+$', 0) == "", concat(lit("DQ10_"), field)) \
    .otherwise(None)

# Purpose - returns a DQ for invalid country codes
# Inputs - ship_to_country (string)
#   ship_to_country == the value of the country code
def udf_dq11_cc(ship_to_country):
  return F.when(F.upper(ship_to_country).isin(b_country_list.value), None) \
    .otherwise("DQ11")

# Purpose - returns a DM for product numbers with a '#' present
#         - returns a dm if value is longer than 8 chars and has '-'
# Inputs - product_number (string)
#   product_number == the value of the product number
def udf_dm12_left_pn(product_number):
  return F.when((F.length(product_number) > 8) & (product_number.contains('-')), "DM12") \
    .when(product_number.contains("#"), "DM12") \
    .otherwise(None)

# Purpose - returns a DQ if a date falls out of an acceptable range.
# Note: max_date formats the full string including the hour, minute and second to match the length
#       because the dates are actually compared as strings and max_date behaves as >= without HH:mm:ss
# Inputs - dates are of the Java form (string with 'yyyy-MM-dd')
#   date == the value date you want to check
#   min_date == records older than this will be marked
#   max_date == records newer than this will be marked (defaults to today)
def udf_dq13_date(date, min_date, max_date=None):
  if max_date is None:
    max_date = datetime.now().strftime('%Y-%m-%d')
  return F.when((date < F.date_format(lit(min_date), 'yyyy-MM-dd')) | \
                (date > F.date_format(lit(max_date), 'yyyy-MM-dd HH:mm:ss')), "DQ13") \
    .otherwise(None)

# Purpose - returns a DM for ship address with '??' or 'N/A' is present
# Inputs - ship_address_orig (string)
#   ship_address_orig == the value of the ship address
def udf_dm14_address(ship_address_orig):
  return F.when(F.regexp_extract(ship_address_orig, '\?\?', 0) != "", "DM14") \
    .when(F.regexp_extract(ship_address_orig, 'N/A', 0) != "", "DM14") \
    .otherwise(None)
    
# Purpose - returns a DM for zipcodes longer than 5 and based in the country 'US'
# Inputs - ship_to_country (string), ship_zip_orig (string)
#   ship_to_country == the value of the country code
#   ship_zip_orig == the zipcode that you may modify
def step_dm15(ship_to_country, ship_zip_orig):
  if ship_zip_orig is None:
    return None
  if ship_to_country in regex_dict:
    pattern = re.compile(regex_dict[ship_to_country])
    if pattern.match(ship_zip_orig):
      return None
    else:
      return "DM15"
  else:
    return "DM15"

# Purpose - returns a DM for ship addresses that match a channel pattern
# Inputs - ship_address_orig (string)
#   ship_address_orig == the value of the ship address that may match a channel pattern
def step_dm16(ship_address_orig):
  if ship_address_orig is not None:
    ship_address_orig = ship_address_orig.upper()
  if ship_address_orig is not None:
    for i in channel_patterns:
      if ship_address_orig.startswith(i[0]):
        return "DM16"
  return None

def step_dm17_individual(channel, channel_ship_cnt, ship_cnt_threshold, field):
  m_mixed_pattern = []	
  b_business_patterns = []	
  for i in patterns.value:
    if i.rsplit(':', 1)[1] == 'MIX':
      m_mixed_pattern.append(i.split(':', 1)[0])
    elif i.rsplit(':', 1)[1] == 'BUS':
      b_business_patterns.append(i.split(':', 1)[0])	
  if channel == None or channel == '':	
    return None	
  if any(i in channel for i in m_mixed_pattern):	
  	return 'DM17_' + field	
  elif any(i in channel for i in b_business_patterns):	
  	return None	
  elif channel_ship_cnt < ship_cnt_threshold:	
  	return 'DM17_' + field	
  else:	
  	return None
udf_dm17_individual = udf(lambda a,b,c,d: step_dm17_individual(a,b,c,d), StringType())

# Purpose - returns a DQ for CIDs that are invalid (not equal to length of 16 or invalid chars)
# Inputs - cid (string)
#   cid == the value of id which may be invalid
def udf_dq18_cid(cid):
  return F.when((F.isnull(cid)) | (F.length(cid) != 16) | (cid == "0000000000000000"), "DQ18") \
    .otherwise(None)

# Purpose - returns a DM for invalid segment_desc 
def udf_dm19(segment_desc):
  return F.when((F.isnull(segment_desc)) | (segment_desc == "") | (segment_desc == "Segment Desc") | (segment_desc == "Segment Desc2"), "DM19") \
    .otherwise(None)

# Purpose - returns a DM for ISO country codes that do not match an existing country code
# Inputs - country_code (string)
#   country_code == the value which may be an invalid country code
def udf_dm21(country_code, field):
  return F.when((country_code).isin(b_country_list.value), None) \
    .otherwise(concat(lit("DM21_"),field))

# Purpose - returns a DM if state code invalid
# Inputs - state (string)
#   state == value of state
def udf_dm22_state_code(s, dm_flag):
  return F.when(dm_flag == "DM22", dm_flag) \
    .when(F.upper(s).isin(b_state_codes.value), None) \
    .otherwise("DM22")
  
# Purpose - returns a DM for values containing the chars in the set of invalid chars (above)
# Inputs - many possible column values (string)
#   City == the value of City which may be invalid
def udf_dm24_invalid_char(value, field=None):
  if field is not None:
    field = concat(lit("DM24_"), field)
  else:
    field = lit('DM24')
  return F.when(F.regexp_extract(value, b_invalid_chars_regex.value, 0) == "", None) \
    .otherwise(field)
#replace this if python 3
# def udf_dm24_invalid_char(value):
#   return F.when(F.regexp_extract(value, b_invalid_chars.value, 0) != "", "DM24") \
#     .otherwise(None)

# Purpose - returns a DM for item_type_desc that are not a length of 2
# Inputs - item_type_desc (string)
#   item_type_desc == value of the item_type_desc that may be invalid
def udf_dm27_len(value, min_length, field=None, max_length=None):
  if field is not None:
    field = concat(lit("_"),field)
  else:
    field = lit('')
  if max_length is None:
    max_length = min_length
  return F.when((F.length(value) < min_length) | (F.length(value) > max_length), concat(lit("DM27"),lit(field))) \
    .otherwise(None)

# Purpose - returns a DM for values that are in the unknown list (above)
# Inputs - many possible values (string)    
def udf_dm28_unknown(value, dm_flag):
  return F.when((dm_flag == "DM28") | (value == ""), "DM28") \
    .when(F.regexp_extract(value, b_regex_unknown_list.value, 0) != "", "DM28") \
    .otherwise(None)

# Purpose - returns a DM when column is a provided value
# Inputs - column, str_to_replace
#   column == column to check
#   str_to_replace == provided value that will be flagged
def udf_dm29(column, str_to_replace):
  return F.when(column == str_to_replace, "DM29") \
    .otherwise(None)

# Purpose - returns a DM if a comma in city, state
# Inputs - city (string)
#   store_name == value of city state
#   whse_name == value of city state
def udf_dm30_city_state(s, dm_flag):
  return F.when(dm_flag == "DM30", dm_flag) \
    .when(s.contains(", "), "DM30") \
    .when(s.contains(","), "DM30") \
    .otherwise(None)

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
# def step_dq35(val, field, pattern):
#   pattern = re.compile(pattern)
#   if pattern.match(val):
#     return None
#   return "DQ35_" + field
def udf_dq35_regex_chk(val, field, pattern):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(F.regexp_extract(val, pattern, 0) != "", None) \
    .otherwise(concat(lit("DQ35_"),field))
  
# Purpose - returns a DQ if value is null or unknown
# Inputs - value (string)
#   value == value of entity desc
def step_dq36(value):
  for i in unknown_list:
    if value == i:
      return "DQ36"
  if value == None or value == "":
    return "DQ36"
  return None

# suffix "_chk" indicates generic check
# Purpose - returns a dm if value is NOT in provided list
def udf_dm37_list_chk(value, field, entries_list):
  return F.when(value.isin(entries_list), None) \
    .otherwise(concat(lit("DM37_"),field))

# suffix "_chk" indicates generic check
# Purpose - returns a DQ if value is IN provided list of invalid critical entries
def udf_dq38_val_in_list_chk(value, field, entries_list):
  return F.when(value.isin(entries_list), concat(lit("DQ38_"),field)) \
    .otherwise(None)

# Purpose - returns a dm if value has preceeding 0s
def udf_dm39_leading_zeros(value):
  return F.when(F.regexp_extract(value, "^0*", 0) != "", "DM39") \
    .otherwise(None)

#TODO: need to fix :)
# THIS ALSO PERFORMS THE MODIFICATION, APPLY IT AFTER OTHER MODIFICATIONS
# Purpose - returns a DM if: 1. value doesn't exist in key of lookup 
#                            2. value matches key, doesn't match value of lookup
# 
#usage: udf_dm40_lookup(df2, "location_cd", "location_desc", b_location_cd.value)
def udf_dm40_lookup(df, col1_name, col2_name, lookup):
  def is_in_list(value, field, entries_list):
    return F.when(value.isin(list(entries_list)), None) \
      .otherwise(concat(lit("DM40_"), field))
  def compare(col1, col2, field):
    return F.when(col1 == col2, None) \
      .otherwise(concat(lit("DM40_"), field))
  
  # step 1: clones 1st column, flag when 1st column not in lookup
  df2 = df.withColumn("temp1", col(col1_name)) \
  .withColumn("check1", is_in_list(col(col1_name), lit(col1_name), lookup.keys()))
  
  # step 2: replace cloned with lookup values, flag when replaced != lookup
  df3 = df2.replace(lookup, subset="temp1") \
  .withColumnRenamed("temp1", "temp2") \
  .withColumn("check2", compare(col(col2_name), col("temp2"), lit(col1_name)))
  
  # step 3: drop column 2, rename replaced to column 2, clean up check cols
  df4 = df3.withColumn("check_dm40_"+col1_name, F.array_join(F.array_distinct(F.array(col("check1"), col("check2"))), "")) \
  .drop("check1", "check2", col2_name) \
  .withColumnRenamed("temp2", col2_name)
  
  return df4

def step_dm43_decode(val, valid_list, field):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(val.isin(valid_list), concat(lit("DM43_"), field)) \
    .otherwise(concat(lit("DM43_"), field))

def step_dm43_decode_pass_through(val, valid_list, field):
  return F.when((F.isnull(val)) | (val == ''), None) \
    .when(val.isin(valid_list), concat(lit("DM43_"), field)) \
    .otherwise(None)

# Purpose - returns a DM if column matches specified invalid values
# def step_dm44_list_null_chk(val, field, entries_list):
#   return F.when(val.isin(entries_list), concat(lit("DM44_"), field)) \
#           .otherwise(None)

#Change
def step_dm44_list_null_chk(val, field, entries_list):
  return F.when(val.isin(entries_list), concat(lit("DM44_"), field)) \
		  .otherwise(None)

# Purpose - returns a DM when column is not only digits
def step_dm46_numeric(val, field):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(F.regexp_extract(val, "^[0-9]+$", 0) != "", None) \
    .otherwise(concat(lit("DM46_"),field))

# Purpose - returns a DM when column contains a provided value
# Inputs - 
def step_dm50_contains(val, field, str_to_replace, dm_flag=F.lit(None)):
  return F.when(~F.isnull(dm_flag), dm_flag) \
    .when(val.contains(str_to_replace), concat(lit("DM50_"), field)) \
    .otherwise(None)

# Purpose - returns a DM when column falls outside of acceptable date range
def step_dm53_date(val, field, min_date):
  return F.when((val < F.date_format(min_date, 'yyyy-MM-dd')) | (val > F.current_date()), concat(lit("DM53_"), field)) \
    .otherwise(None)

def step_dm53_date_hermes(val, field, min_date, max_date=F.current_date()):
  return F.when(
    (F.from_unixtime(F.unix_timestamp(val, 'dd-MMM-yy'), format='yyyy-MM-dd') < F.date_format(min_date, 'yyyy-MM-dd')) | 
    (F.from_unixtime(F.unix_timestamp(val, 'dd-MMM-yy'), format='yyyy-MM-dd') > F.date_format(max_date, 'yyyy-MM-dd')), 
    concat(lit("DM53_"), field)) \
    .otherwise(None)

# Purpose - returns a DM if postal code column does not match country code's regex
def step_dm54_zip(zip_code, country, field):
  if zip_code == None or zip_code == "":
    return None
  if country in b_zip_regex_patterns.value:
    pattern = re.compile((b_zip_regex_patterns.value[country]).strip())
    if pattern.search(zip_code):
        return None
    else:
        return "DM54_" + field
  else:
    return None

# Purpose - returns a DM if currency code is not in valid list
def step_dm55_currency_codes(val, field, valid_list):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(val.isin(valid_list), None) \
    .otherwise(concat(lit("DM55_"), field))

# Purpose - returns a DM if column contains any characters besides A-Z, a-z, 0-9
def step_dm56_nonalpha(val, field):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(F.regexp_extract(val, "^[A-Za-z0-9]+$", 0) != "", None) \
    .otherwise(concat(lit("DM56_"),field))

# Purpose - returns a DM if column contains any characters besides A-Z, a-z, spaces
def step_dm57_chars(val, field):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(F.regexp_extract(val, "^[A-Za-z\s]+$", 0) != "", None) \
    .otherwise(concat(lit("DM57_"),field))

#Purpose - returns a DM if column starts with " or ends with "
def step_dm58_quotes(val, field):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(val.startswith("\"") | val.endswith("\""), concat(lit("DM58_"), field)) \
    .otherwise(None)

#Purpose - returns a DM if cp1252 encoding of column is different than utf-8 encoding
def step_dm59_encode(val, field):
  return F.when((F.isnull(val)) | (val == '') , None) \
    .when(F.decode(F.encode(F.decode(val, 'utf-8'),'cp1252'),'utf-8') != val, concat(lit("DM59_"), field)) \
    .otherwise(None)

#Purpose - returns a DM if val1 is individual and val2 == val3
def step_dm60_individual(val1, val2, val3, field):
  return F.when((val1 == 'INDIVIDUAL') & (val2 == val3), concat(lit("DM60_"), field))

#Purpose - returns a DM if there is NO match with our supplied regex
def step_dm61_translate(value, regex, field):
  return F.when(F.regexp_extract(value, regex, 0) != "", None) \
    .otherwise(concat(lit("DM61_"), field))

#Purpose - returns a DM if there is a match with our supplied regex
def step_dm62_complement_translate(value, regex, field):
  return F.when(F.regexp_extract(value, regex, 0) == "", None) \
    .otherwise(concat(lit("DM62_"), field))

#Purpose - returns a DM if value contains nonprintable characters
def step_dm63_nonprint_chars(value, field):
  return F.when((F.isnull(value)) | (value == '') , None) \
    .when(F.regexp_extract(value, '^[ -~]+$', 0) != "", None) \
    .otherwise(concat(lit("DM63_"), field))

#Purpose - returns a DM if value is negative
def step_dm64_negatives(value, field):
  return F.when(value < 0, concat(lit("DM64_"), field)) \
    .otherwise(None)

#Purpose - returns a DM if value starts with a given substring
def step_dm65_startswith(value, substr, field, dm_flag):
  return F.when(dm_flag.startswith("DM65"), dm_flag).when(value.startswith(substr), concat(lit("DM65_"), field)) \
    .otherwise(None)

udf_dm15_zip_postal = udf(lambda x,y: step_dm15(x,y), StringType())
udf_dm16_channel = udf(lambda x: step_dm16(x), StringType())
udf_dq36_entity = udf(lambda x: step_dq36(x), StringType())
#udf_dm40_lookup_chk = udf(lambda x,y,z,i: step_dm40(x,y,z,i), StringType())
udf_dm54_zip = udf(lambda x,y,z: step_dm54_zip(x,y,z), StringType())

# COMMAND ----------

# Purpose - returns a DM for an serial number not equal to a length of 10
# Inputs - serial_orig (string)
#   serial_orig == the serial number you are checking for a length of 10
def udf_dm4_sn(serial_orig):
  return F.when(F.length(serial_orig) > 10, "DM4") \
    .otherwise(None)

# Purpose - returns first 10 characters of serial number
def udf_mod_dm4_sn(serial_orig):
  return F.when(F.length(serial_orig) > 10, F.substring(serial_orig, 1, 10)) \
    .otherwise(serial_orig)

# Purpose - returns a modified product numbers if '#' present, otherwise returns the same product number
# Inputs - product_number (string)
#   product_number == the value of the product number
def udf_mod_dm12_left_pn(product_number):
  return F.when((F.length(product_number) > 8) & (product_number.contains('-')), F.split(product_number, '[-#]')[0]) \
    .otherwise(F.split(product_number, '#')[0])

# Purpose - returns a modified ship address if '??' present, otherwise returns the same ship address
# Inputs - ship_address_orig (string)
#   ship_address_orig == the value of the ship address
def udf_mod_dm14_address(ship_address_orig):
  return F.when(F.regexp_extract(ship_address_orig, '\?\?', 0) != "", "N/A") \
    .when(F.regexp_extract(ship_address_orig, 'N/A', 0) != "", "N/A") \
    .otherwise(ship_address_orig)
    
# Purpose - returns a modified ship zip code, taking the first five digits if longer than 5 digits. If shorter than five digits, zipcode is unchanged
# Inputs - ship_zip_orig (string)
#   ship_zip_orig == the zipcode that you may modify
#   ship_to_country == the contry code
def mod_dm15(ship_to_country, ship_zip_orig):
  if ship_zip_orig is None:
    return None
  if ship_to_country == "US" and len(ship_zip_orig) > 5:
    ship_zip_orig = ship_zip_orig[0:5]
  if ship_to_country in regex_dict:
    pattern = re.compile(regex_dict[ship_to_country])
    if not pattern.match(ship_zip_orig):
      return None
    else:
      return ship_zip_orig
  else:
    return None
  
# Purpose - returns a standardized ship address if ship_address_orig matches a channel pattern, otherwise return the original ship address
# Inputs - ship_address_orig (string)
#   ship_address_orig == the value of the ship address that may match a channel pattern
def mod_dm16(ship_address_orig):
  if ship_address_orig is not None:
    ship_address_orig = ship_address_orig.upper()
  if ship_address_orig is not None:
    for i in channel_patterns:
      if ship_address_orig.startswith(i[0]):
        return i[1]
  return ship_address_orig

def mod_dm17_individual(channel, channel_ship_cnt, ship_cnt_threshold, orig_column):
  m_mixed_pattern = []	
  b_business_patterns = []	
  for i in patterns.value:
    if i.rsplit(':', 1)[1] == 'MIX':
      m_mixed_pattern.append(i.split(':', 1)[0])
    elif i.rsplit(':', 1)[1] == 'BUS':
      b_business_patterns.append(i.split(':', 1)[0])   	
  if channel == None or channel == '':	
    return None	
  elif any(i in channel for i in m_mixed_pattern):	
  	return 'INDIVIDUAL'	
  elif any(i in channel for i in b_business_patterns):	
  	return orig_column	
  elif channel_ship_cnt < ship_cnt_threshold:	
  	return 'INDIVIDUAL'	
  else:	
  	return orig_column
udf_mod_dm17_individual = udf(lambda a,b,c,d: mod_dm17_individual(a,b,c,d), StringType())
def mod_dm17_individual_hash(channel, channel_ship_cnt, ship_cnt_threshold, orig_column):
  if orig_column is None or orig_column == '':	
    return None	
  if channel is None or channel == '':	
    return None	
  m_mixed_pattern = []	
  b_business_patterns = []	
  for i in patterns.value:
    if i.rsplit(':', 1)[1] == 'MIX':
      m_mixed_pattern.append(i.split(':', 1)[0])
    elif i.rsplit(':', 1)[1] == 'BUS':
      b_business_patterns.append(i.split(':', 1)[0])	
  if any(i in channel for i in b_business_patterns):	
  	return orig_column	
  	
  elif any(i in channel for i in m_mixed_pattern):	
    orig_ascii_only = re.sub('[^\x00-\x7F]', ' ', orig_column)	
    return hashlib.sha256(orig_ascii_only).hexdigest()	
  elif channel_ship_cnt < ship_cnt_threshold:	
    orig_ascii_only = re.sub('[^\x00-\x7F]', ' ', orig_column)	
    return hashlib.sha256(orig_ascii_only).hexdigest()	
  else:	
    return orig_column
udf_mod_dm17_individual_hash = udf(lambda a,b,c,d: mod_dm17_individual_hash(a,b,c,d), StringType())

def udf_mod_dm17_individual_hash_if_marked(needs_hashed, orig_column):
  return F.when((F.isnull(orig_column)) | (orig_column == ''), None) \
    .when(F.isnull(needs_hashed), orig_column).otherwise(F.sha2(orig_column, 256))

#Purpose - returns the corresponding segment_desc according to segment_cd
def udf_mod_dm19(segment_desc, segment_cd):
  return F.when((segment_cd == '000'), "Unknown") \
    .when((segment_cd == '001'), "Business") \
    .when((segment_cd == '002'), "Personal Use") \
    .when((segment_cd == '003'), "Home-based Business") \
    .when((segment_cd == '004'), "Telecommuting") \
    .when((segment_cd == '005'), "Business with 9 or fewer employees") \
    .when((segment_cd == '006'), "Business with 10 or more employees") \
    .otherwise(None)

# Purpose - returns "XU" for ISO country codes that do not match an existing country code
# Inputs - country_code (string)
#   country_code == the value which may be modified to "XU"
def mod_dm21(country_code):
  return F.when((country_code).isin(b_country_list.value), country_code) \
    .otherwise("XU")
    
# Purpose - returns NULL if state code invalid
# Inputs - state (string)
#   state == value of state
def udf_mod_dm22_state_code(s):
  return F.when(F.upper(s).isin(b_state_codes.value), s)\
    .otherwise(None)

# Purpose - returns NULL for values that match an invalid char (above), otherwise returns the original value
# Inputs - value (string)
#   value == the value which may be modified to NULL
def udf_mod_dm24_invalid_char(value):
  return F.when(F.regexp_extract(value, b_invalid_chars_regex.value, 0) == "", value) \
    .otherwise(None)
#replace this if python 3
# def udf_mod_dm24_invalid_char(value):
#   return F.when(F.regexp_extract(value, b_invalid_chars.value, 0) == "", value) \
#     .otherwise(None)

# Purpose - returns NULL for item_type_desc that are not a given length
# Inputs - value, length
def udf_mod_dm27_len(value, min_length, max_length=None):
  if max_length is None:
    max_length = min_length
  return F.when((F.length(value) < min_length) | (F.length(value) > max_length), None) \
    .otherwise(value)

# Purpose - returns NULL for values that are in the unknown list (above)
# Inputs - many possible values (string)
def mod_dm28(value):
  return F.when(value == "", None) \
    .when(F.regexp_extract(value, b_regex_unknown_list.value, 0) != "", None) \
    .otherwise(value)

# Purpose - returns replacement value when column is a provided value
# Inputs - column, str_to_replace, value
#   column == column to check
#   str_to_replace == provided value that will be replaced
#   value == replacement value
def udf_mod_dm29(column, str_to_replace, value):
  return F.when(column == str_to_replace, value) \
    .otherwise(column)
  
# Purpose - replaces a comma if in city, state
# Inputs - city (string)
#   store_name == value of city state
#   whse_name == value of city state
def udf_mod_dm30_city_state(value):
  return F.regexp_replace(value, ",\s|,", " ")

# Purpose - returns value if value is in provided valid entries list
def udf_mod_dm37_list_chk(value, entries_list):
  return F.when(value.isin(entries_list), value) \
    .otherwise(None)

# Purpose - trims preceding 0s but not when value is only 0s
def udf_mod_dm39_leading_zeros(value):
  return F.when(F.regexp_extract(value, "^0+$", 0) != "", F.regexp_replace(value, "^0+$", "0")) \
    .otherwise(F.regexp_replace(value, "^0*", ""))

# Purpose - performs a lookup replacement on given dataframe and replacement pairs
def udf_mod_dm43_decode(df, replacements, field):
  valid_vals = [i[1] for i in replacements]
  for item in replacements:
    df = df.withColumn(field, udf_mod_dm29(F.col(field), item[0], item[1]))
  df = df.withColumn(field, udf_mod_dm37_list_chk(F.col(field), valid_vals))
  return df

# Purpose - performs a lookup replacement on given dataframe and replacement pairs
# Note: similar to DM43 above except the original value is passed through (retained) when there isn't a match
def udf_mod_dm43_decode_pass_through(df, replacements, field):
  for rule in replacements:
    df = df.withColumn(field, udf_mod_dm29(df[field], rule[0], rule[1]))
  return df

# Purpose - replaces invalid values with None after regex match with invalid value
def udf_mod_dm44_list_null_chk(value, regex):
  return F.when(F.regexp_extract(value, regex, 0) != "", None) \
    .otherwise(value)

# Purpose - returns None when column contains characters other than 0-9
def udf_mod_dm46_numeric(val):
  return F.when(F.regexp_extract(val, "^[0-9]+$", 0) == "", None) \
    .otherwise(val)

# Purpose - returns value when column contains value
def udf_mod_dm50_contains(val, str_to_replace, str_to_find=None):
  if str_to_find == None:
    str_to_find = str_to_replace
  return F.when(val.contains(str_to_find), str_to_replace) \
    .otherwise(val)

# Purpose - returns None if column falls outside of acceptable range
def udf_mod_dm53_date(val, min_date):
  return F.when((val < F.date_format(min_date, 'yyyy-MM-dd')) | (val > F.current_date()), None) \
    .otherwise(val)

def udf_mod_dm53_date_hermes(val, min_date, max_date=F.current_date()):
  return F.when(
    (F.from_unixtime(F.unix_timestamp(val, 'dd-MMM-yy'), format='yyyy-MM-dd') < F.date_format(min_date, 'yyyy-MM-dd')) | 
    (F.from_unixtime(F.unix_timestamp(val, 'dd-MMM-yy'), format='yyyy-MM-dd') > F.date_format(max_date, 'yyyy-MM-dd')), 
    None) \
    .otherwise(val)
 
# Purpose - returns postal code if country is null or is XU
#         - returns first 5 digits of postal code if country is US
#         - returns None if postal code doesn't match country regex
def mod_dm54(zip_code, country):
  if country == "" or country == None or country == "XU":
    return zip_code
  if country == "US" and len(zip_code) > 5:
    zip_code = zip_code[0:5]
  if country in b_zip_regex_patterns.value:
    pattern = re.compile((b_zip_regex_patterns.value[country]).strip())
    if not pattern.search(zip_code):
      return None
    else:
      return zip_code
  else:
    return zip_code

# Purpose - returns currency code if currency code is valid
#         - returns currency code of country if country code was put in currency code column
def mod_dm55(val):
  if val in b_currency_codes.value.values():
    return val
  elif val in b_currency_codes.value.keys():
    return b_currency_codes.value[val]
  return None

# Purpose - returns column if it only contains A-Z, a-z, 0-9, else null
def udf_mod_dm56_nonalpha(val):
  return F.when(F.regexp_extract(val, "^[A-Za-z0-9]+$", 0) != "", val) \
    .otherwise(None)

# Purpose - returns column if it only contains A-Z, a-z, spaces, else null
def udf_mod_dm57_chars(val):
  return F.when(F.regexp_extract(val, "^[A-Za-z\s]+$", 0) != "", val) \
    .otherwise(None)

# Purpose - returns group 2 of regex, removes surrounding quotes
def udf_mod_dm58_quotes(val):
  return F.regexp_extract(val, "^(\"*)(.*?)(\"*)$", 2)

# Purpose - returns cp1252 encoding
def udf_mod_dm59_encode(val):
  return F.decode(F.encode(F.decode(val, 'utf-8'),'cp1252'),'utf-8')

# Purpose - returns INDIVIDUAL if val1 == INDIVIDUAL and val2 == val3
def udf_mod_dm60_individual(val1, val2, val3, val_to_replace):
  return F.when((val1 == 'INDIVIDUAL') & (val2 == val3), 'INDIVIDUAL') \
    .otherwise(val_to_replace)

#Purpose - returns a None if there is NO match with our supplied regex
def udf_mod_dm61_translate(val, regex):
  return F.when(F.regexp_extract(val, regex, 0) != "", val) \
    .otherwise(None)

#Purpose - returns a None if there is a match with our supplied regex
def udf_mod_dm62_complement_translate(value, regex):
  return F.when(F.regexp_extract(value, regex, 0) == "", value) \
    .otherwise(None)

#Purpose - returns column with all nonprintable characters removed
def udf_mod_dm63_nonprint_chars(value):
  return F.regexp_replace(value, '[^ -~]+', '')

#Purpose - returns a None if value is negative
def udf_mod_dm64_negatives(val):
  return F.when(val < 0, None) \
    .otherwise(val)

#Purpose - returns a None if value starts with a given substring
def udf_mod_dm65_startswith(value, substr):
  return F.when(value.startswith(substr), None) \
    .otherwise(value)
  
udf_mod_dm15_zip_postal = udf(lambda x, y: mod_dm15(x, y), StringType())
udf_mod_dm16_channel = udf(lambda x: mod_dm16(x), StringType())
udf_mod_dm54_zip = udf(lambda x,y: mod_dm54(x,y), StringType())
udf_mod_dm55_currency = udf(lambda x: mod_dm55(x), StringType())

# COMMAND ----------

# Purpose - returns the uppercase value for cities
# Inputs - city (string)
#   city == value of the city
def udf_mod_upper(val):
  return F.upper(val)

# COMMAND ----------

# Purpose - creates a rank column that eliminates duplicates and keeps only the top record
# Inputs - df, partition_list, sort_list
#   df == the df that will be  of the channel that may match a business pattern
#   channel_ship_cnt == the number of shipments for the specifc product_group and channel

# Examples of how list hould be passed
# partition_list = [df['serial_number'], df['product_number']]
# sort_list = [df['stg_rec_guid'].asc(), df['load_ts'].desc()]

def step_dq9_rank(df, partition_list, sort_list):
  window = Window.partitionBy(partition_list).orderBy(sort_list)
  dfy = df.select('*', row_number().over(window).alias('ranked'))
#  dfy.cache()
  return dfy
  
def udf_step_dq9(ranked):
  return F.when(ranked != 1, "DQ9").otherwise(None)

# COMMAND ----------

# def step_dq9(val, field, partition_list, sort_list):
#   window_clause = Window.partitionBy(partition_cols).orderBy(f.col(sort_list).desc())
#   df = df.withColumn(field,f.row_number().over(window_clause))
#   return F.when(val == 0, "DQ9")

# COMMAND ----------

# DSR RULES

# flags record if matches both serial number and product number
# def step_dq998(val1, val2):
#   for i in b_sn_blacklist.value:
#     if i[0] == "" or i[1] == "": # in case the blacklist contains empty strings
#       return None
#     elif val1 == i[0] and val2 == i[1]:
#       return "DQ998_serial_number"
#   return None
# udf_step_dq998_sn = udf(lambda x,y: step_dq998(x,y), StringType())

# Modified by Enterprise team on 07/06 to fix the existing issue
def step_dq998(val1, val2):
  for i in b_sn_blacklist.value:
    if (val1 == "" or val1 == None or val1 == "NULL" or val1 == "null") \
    or (val2 == "" or val2 == None or val2 == "NULL" or val2 == "null"):
      return None
    elif val1 == i[0] and val2 == i[1]:
      return "DQ998_serial_number"
  return None
udf_step_dq998_sn = udf(lambda x,y: step_dq998(x,y), StringType())

# flags record if matches CID
def udf_step_dq999_cid(val):
  return F.when((F.isnull(val)) | (val == ""), None) \
          .when(val.isin(b_cid_blacklist.value), "DQ999_cid") \
          .otherwise(None)

# COMMAND ----------

#Purpose, Inputs, Value
def udf_dq70_nullcheck(colValue,field):
  return F.when(F.isnull(colValue), concat(lit("DQ70_"),field)) \
    .when((colValue == "null")|(colValue == "NULL")|(colValue == None) | (colValue == ""), concat(lit("DQ70_"),field)) \
    .otherwise(None)

# COMMAND ----------

# Purpose - To compare two date/timestamp columns	
#returns a DQ if a date1 is greater than date2.	
# Inputs - dates are of the Java form (string with 'yyyy-MM-dd')	
#   date_col1 == date/timestamp column you want to compare	
#   date_col2 == date/timestamp column you want to compare	
def udf_dq71_compare_dts(date_col1, date_col2,field1,field2):	
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

def step_dq72_ib(date_str,field):
  date_str = date_str.split(',')[0]
  pattern = '^\d{4}(\-)(((0)[1-9])|((1)[0-2]))(\-)(((0)[0-9])|[1-2][0-9]|(3)[0-1])'
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
  
udf_step_dq72_ib = udf(lambda x,y: step_dq72_ib(x,y), StringType())

# COMMAND ----------

#Purpose - returns a DQ when column is non-numeric
def udf_dq66_numeric(value,field):
  return F.when((F.isnull(value))|(value == "null")|(value == "NULL")|(value == None)|(value == "") , None)\
          .when((value).cast("double").isNotNull(), None)\
         .otherwise(concat(lit("DQ66_"),field))

# COMMAND ----------

# Purpose - returns a DQ if value is NOT in provided list
def udf_dq67_list_chk(value, entries_list,field):
  return F.when(value.isin(entries_list), None) \
    .otherwise(concat(lit("DQ67_"),field))
def step_dq68(col1,col2,expectValues1,expectValues2,field):
  for i,val in enumerate(expectValues1):
    if(col1 == val and col2 != expectValues2[i]): 
      return ("DQ68_"+ field) 
  return None
def udf_dq68_fill(expectValues1,expectValues2):
  return udf(lambda a,b,c: step_dq68(a,b,expectValues1,expectValues2,c))
# # Purpose - returns a DM if value is null for an expected filled col
# # Inputs - value (string)
# # value == value of entity desc  
def step_dm69_fill_pair_chk(col1, col2,expectValues,field):
  for val in expectValues:
    if(col1 == val and ((col2 == "null") or (col2=="NULL")or(col2=="") or(col2 ==None))):
       return ("DM69_"+ field) 
  return None  
def udf_dq69_filldm(expectValues):
  return udf(lambda a,b,c: step_dm69_fill_pair_chk(a,b,expectValues,c))

# COMMAND ----------

def mod_dm69_update(col1, col2, expectValues1,expectValues2):
  for i,val in enumerate(expectValues2):
    if(col2 == val and ((col1 == "null") or (col1=="NULL")or(col1=="") or (col1 ==None))):
       return expectValues1[i]
  return col1
def udf_dm69_fillcol(expectValues1,expectValues2):
  return udf(lambda a,b: mod_dm69_update(a,b,expectValues1,expectValues2))

# COMMAND ----------

def step_dm80_null_value_translate(value, field):
  return F.when((value == "NULL"), concat(lit("DM80_"), field)) \
    .otherwise(None)

# COMMAND ----------

def mod_dm80_null_value_translate(value):
  return F.when(value == "NULL", None) \
    .otherwise(value)

# COMMAND ----------

def step_dq64_negatives(value, field):
  return F.when(value < 0, concat(lit("DQ64_"), field)) \
    .otherwise(None)

# COMMAND ----------

def udf_dm81(platform_subset_name, field):
  return F.when((platform_subset_name).isin(b_platform_subset_name_list.value), None) \
    .otherwise(concat(lit("DM81_"),field))

# COMMAND ----------

def mod_dm81(platform_subset_name):
  return F.when((platform_subset_name).isin(b_platform_subset_name_list.value), platform_subset_name) \
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

def udf_step_dq76(value,field,pattern):
  return F.when((F.isnull(value))|(value == "null")|(value == "NULL")|(value == None)|(value == ""), None)\
          .when(F.regexp_extract(value,pattern,0) != "", None)\
         .otherwise(concat(lit("DQ76_"),field))

# COMMAND ----------

# Purpose - returns a DQ for value in column is not in given range
def udf_dq80_range(val, min, field=None, max=None):
  if field is not None:
    field = concat(lit("_"),field)
  else:
    field = lit('')
  if max is None:
    max = min
  return F.when((F.isnull(val)) | (val == '') | (val == None), None) \
    .when((val < min) | (val > max), concat(lit("DQ80"),lit(field))) \
    .otherwise(None)

#Purpose - returns a DQ if value is not in valid list (enum)
def udf_dq81_valid_list(val, valid_list, field):
  return F.when(~val.isin(valid_list), concat(lit("DQ81_"), field)) \
  .otherwise(None)

#Purpose - returns a DQ if value is not in valid list (enum)
def udf_dq85_valid_list(val, valid_list, field):
  return F.when((F.isnull(val)) | (val == '') | (val == None), None) \
   .when(~val.isin(valid_list), concat(lit("DQ85_"), field)) \
  .otherwise(None)


#Purpose - returns a DQ if there is No match with our supplied regex
def udf_dq82_regex_match(val, regex, field):
  return F.when((F.isnull(val)) | (val == '') | (val == None), None) \
    .when(F.regexp_extract(val, regex, 0)!= "", None) \
    .otherwise(concat(lit("DQ82_"), field))

#Purpose - returns a DM if value is not in valid list (enum)
def step_dm83_valid_list(val, valid_list, field):
  return F.when((F.isnull(val)) | (val == '') | (val == None), concat(lit("DM83_"), field)) \
  .when(~val.isin(valid_list), concat(lit("DM83_"), field)) \
  .otherwise(None)

#Purpose - modifies the data if value is not in valid list (enum)
def mod_dm83_valid_list(val, valid_list):
  return F.when(~val.isin(valid_list), lit('Ink')) \
  .when((F.isnull(val)) | (val == '') | (val == None), lit('Ink')) \
  .otherwise(lit('Ink'))
  
def udf_dq90_equal_length_chk(val, field, length):
  return F.when((F.length(val) == length), concat(lit("DQ90_"),field)) \
		  .otherwise(None)
          
def udf_dq91_not_equal_length_chk(val, field, length):
  return F.when((F.length(val) != length), concat(lit("DQ91_"),field)) \
		  .otherwise(None)

# COMMAND ----------

# Check one column for some value and being Null
def dq92_compare_value(val1, val2, field1, field2, check_val):
  return F.when((val1 == check_val) & ((val2 == "") | (val2.isNull()) | (val2 == "NULL") | (val2 == "null")), concat(lit("DQ92_"),field1,lit("_"),field2)) \
		  .otherwise(None)
          

# COMMAND ----------

# DQ93
def udf_dq93_regex_check(field, regex):
  return F.when(~F.col(field).rlike(regex), concat(lit("DQ93"), field)).otherwise(None)

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
      
def killCurrentProcess(msg):
  raise Exception(msg) 

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

# def udf_concat_list(*args):
#   return F.concat_ws(",", *args)

# COMMAND ----------

#UDF to trim source file. Removes the path and leaves only the file name
udf_source_file = udf(lambda x: x.rsplit('/', 1)[-1], StringType())

# COMMAND ----------

# get file names for source col in stl_load_errors log
def get_file_names(df):
  files = df.select('source_file').distinct().collect()
  trunc_files = []
  
  for file in files:
    for trunc_file in file.asDict().values():
      trunc_files.append(trunc_file.rsplit('/', 1)[-1])

  return ','.join(trunc_files)

# COMMAND ----------

#UDF to create error_code partition column
def udf_step_error_code(value):
  return F.when((value == "") | F.isnull(value), F.lit(0)).otherwise(F.lit(1))

# COMMAND ----------

#Capturing Redshift load errors

def get_load_error(raw_line, idx):
  x = raw_line.split("|")
  if idx > len(x):
    return "N/A"
  return str(x[idx])

udf_get_load_error = udf(lambda x,y: get_load_error(x,y), StringType())

# COMMAND ----------

# Get error ID from Redshift
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

#Connect to RS and create a table for Databricks to write into
def conn_rs_load_errors(bucket_lot_id, cols, input_file_names, table_name, errors_log_dir):
  get_id, rs_load_errors = get_rs_errors(dbname, user, pwd, host, port, bucket_lot_id)

  if get_id:
    newColumns = ["userid", "slice","tbl","starttime","session","query","filename","line_number","colname","type","col_length","position","raw_line","raw_field_value","err_code","err_reason"]
    df_errors = spark.createDataFrame(rs_load_errors, schema=newColumns)
    oldColumns = df_errors.schema.names
    df_errors2 = reduce(lambda df_errors, idx: df_errors.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), df_errors)

    lot_id_idx = cols.index('lot_id')
    guid_idx = cols.index('stg_rec_guid')
    
#     if "channel_shipment" not in table_name and "flags" not in errors_log_dir:
#       source_file_idx = cols.index('source_file')
    
    if "stg" in errors_log_dir:
      df_errors3 = df_errors2.withColumn("source_file", lit(input_file_names)) \
                                         .withColumn("lot_id", lit(str(lot_id))) \
                                         .withColumn("stg_rec_guid", udf_get_load_error(df_errors2.raw_line, lit(guid_idx))) \
                                         .withColumn("table_name", lit(table_name))
    else:
      if "channel_shipment" in table_name:
        df_errors3 = df_errors2.withColumn("lot_id", udf_get_load_error(df_errors2.raw_line, lit(lot_id_idx))) \
                                           .withColumn("stg_rec_guid", udf_get_load_error(df_errors2.raw_line, lit(guid_idx))) \
                                           .withColumn("table_name", lit(table_name))
      else:
        source_file_idx = cols.index('source_file')
        df_errors3 = df_errors2.withColumn("source_file", udf_get_load_error(df_errors2.raw_line, lit(source_file_idx))) \
                                           .withColumn("lot_id", udf_get_load_error(df_errors2.raw_line, lit(lot_id_idx))) \
                                           .withColumn("stg_rec_guid", udf_get_load_error(df_errors2.raw_line, lit(guid_idx))) \
                                           .withColumn("table_name", lit(table_name))

    if "stg" in errors_log_dir:
      #Write to logs, append if already exists, overwrite to create
      try:
        df_errors3.write.mode("append").option("compression", "snappy").parquet(errors_log_dir)
      except:
        df_errors3.write.mode("overwrite").option("compression", "snappy").parquet(errors_log_dir)
    else:
        df_errors3.write.mode("overwrite").option("compression", "snappy").parquet(errors_log_dir)

# COMMAND ----------

###########################################################################################################
###                  Creating Functions Required for DM60                                               ###
###########################################################################################################
#### Function to Modify the column similar to DM60 Individual with extra column to check if column hashed or not
def udf_mod_dm60_hash(val1, val2, val3,val4, field):
  return F.when((val1 != val2) & (val3 == val4) & (~(F.isnull(val1)) ) & (val1 != '') & (val4 != ''),F.sha2(field, 256)).otherwise(field)
#### Function to Mark the invalid column values as DM60
def step_dm60_hash(val1, val2, val3,val4, field):
  return F.when((val1 != val2) & (val3 == val4) & (~(F.isnull(val1)) ) & (val4 != '') & (val1 != ''), concat(lit("DM60_"), field))

# COMMAND ----------

#Change
import boto3
#import datetime
import dateutil.relativedelta

# COMMAND ----------

#Change
def last_update_check_read(bucket_name, prefix_path):
  s3 = boto3.resource('s3')
  bucket = s3.Bucket(bucket_name)
  name_replaced = name.replace(prefix_path,'')
  
  global conn
  conn = boto3.client('s3')
  key = prefix_path + name_replaced 
  responce = conn.get_object(Bucket = bucket_name, Key = key)
  body = responce.get('Body')
  file_in_bytes = body.read()
  file = file_in_bytes.decode('utf-8')
  
  return file

# COMMAND ----------

#Change
#Change
def last_update_check_write(bucket_name, prefix_path, frq):
  split_path = parquet_flags_read.split('/')

  if frq == 'weekly':
    split_path[3] = str(write_year)
    split_path[4] = str(write_month)
    split_path[5] = str(write_day)

  else: split_path[4] = str(write_month)

  split_path_updated = '/'.join(split_path)
  s3 = boto3.resource('s3')
  bucket = s3.Bucket(bucket_name)
  
  object = s3.Object(bucket_name, name)
  object.put(Body=split_path_updated)
  
  return split_path_updated

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

def udf_dq1_sn_new(val,regex):
  return F.when(F.isnull(val), "DQ1") \
    .when((F.length(val) < 10) | (F.length(val) > 14), "DQ1") \
    .when((val.substr(0,10) == "0000000000") | (val.substr(0,10) == "XXXXXXXXXX"), "DQ1") \
    .when(F.regexp_extract(val, regex, 0)== "", "DQ1") \
    .otherwise(None)