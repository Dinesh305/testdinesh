# Databricks notebook source
# MAGIC  %run ../libraries/ent_dq_module

# COMMAND ----------

def step_dq998_validate(col1, col2,blacklist,error_list):
  for val in blacklist:
    if((col1==None or col1=='' or col1 == "NULL" or col1 == "null" or\
        col2==None or col2=='' or col2 == "NULL" or col2 == "null") and (("DQ998") in (error_list))):
      return False
    elif((col1==None or col1=='' or col1 == "NULL" or col1 == "null" or\
          col2==None or col2=='' or col2 == "NULL" or col2 == "null") and (("DQ998") not in (error_list))):
      return True
    elif((col1==val[0]) and (col2==val[1]) and (("DQ998") not in (error_list))):
      return False
    elif((col1==val[0]) and (col2==val[1]) and (("DQ998") in (error_list))):
      return True
  else:
    if ("DQ998") in (error_list):
      return False
    else :
      return True
def udf_DQ998_re_validate(blacklist): 
  return udf(lambda a,b,c: step_dq998_validate(a,b,blacklist,c), BooleanType())

# COMMAND ----------

def step_dq999_validate(col1,blacklist,error_list):
  for val in blacklist:
    if((col1==None or col1=='' or col1 == "NULL" or col1 == "null") and (("DQ999") in (error_list))):
      return False
    elif((col1==None or col1=='' or col1 == "NULL" or col1 == "null") and (("DQ999") not in (error_list))):
      return True
    elif((col1==val) and (("DQ999") not in (error_list))):
      return False
    elif((col1==val) and (("DQ999") in (error_list))):
      return True
  else:
    if ("DQ999") in (error_list):
      return False
    else :
      return True  

def udf_DQ999_re_validate(blacklist):
  return udf(lambda a,b: step_dq999_validate(a,blacklist,b))

# COMMAND ----------

#Created by RCB Enrichment Team
def DQ3_validation(product_number, error_list):
  return F.when(((product_number == None) | (product_number == "NULL") | (product_number == "$") | (product_number == "") | (product_number.contains("\"")) | (product_number.contains(" "))) & (~error_list.contains("DQ3")), False) \
    .when((product_number != None) & (product_number != "NULL") & (product_number != "$") & (product_number != "") & (~product_number.contains("\"")) & (~product_number.contains(" ")) & (error_list.contains("DQ3")), False) \
    .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def DQ34_validation(val, field, length, min_val, max_val, error_list):
  flagName = "DQ34_"+field
  return F.when(((F.length(val) <= min_val) | (F.length(val) >= max_val)) & (~error_list.contains(flagName)), False) \
     .when((F.length(val) > min_val) & (F.length(val) < max_val) & (error_list.contains(flagName)), False) \
     .when((F.length(val) == length) & (error_list.contains(flagName)), False) \
     .when((F.length(val) != length) & (~error_list.contains(flagName)), False) \
     .otherwise(True)

# COMMAND ----------

def DQ34_length_chk(val, field, length, min_val, max_val, error_list):
  flagName = "DQ34_"+field
  return F.when((F.length(val) <= min_val) & (F.length(val) >= max_val) & (~error_list.contains(flagName)), False) \
     .when((F.length(val) > min_val) & (F.length(val) <= max_val) & (error_list.contains(flagName)), False) \
     .when((F.length(val) == length) & (error_list.contains(flagName)), False) \
     .when((F.length(val) != length) & (~error_list.contains(flagName)), False) \
     .otherwise(True)

# COMMAND ----------

def DQ35_regex_chk(val, field, pattern, error_list):
  flagName = "DQ35_"+field
  #case for match and not flagged, then case for not match and flagged
  return F.when(((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '')) & (~error_list.contains(flagName)),False) \
        .when((F.regexp_extract(val,pattern,0) != "") & (F.isnull(val)) & (val == '') & (error_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_dq3_pn(product_number):
  return F.when(F.isnull(product_number), "DQ3") \
    .when((product_number == "NULL") | (product_number == ""), "DQ3") \
    .when((product_number.contains("\"")) | (product_number.contains(" ")) | (product_number.contains("$")), "DQ3") \
    .otherwise(None)

# COMMAND ----------

def udf_DM4_re_validate(serial_number,mod_list):
  return F.when(((F.length(serial_number) > 10) | (F.length(serial_number) < 10)) & (~mod_list.contains("DM4")),False) \
        .otherwise(True)

# COMMAND ----------

def DM28_unknown(value, mod_list):
  for i in unknown_list:
    return F.when((F.regexp_extract(value, b_regex_unknown_list.value, 0) != "") & (value != "") & (~mod_list.contains("DM28")), False) \
            .when((F.regexp_extract(value, b_regex_unknown_list.value, 0) == "") & (value == "")& (~mod_list.contains("DM28")), False) \
            .otherwise(True)

# COMMAND ----------

# based on Jira ticket PBDPS-12809 adding pass through for DM43 drop_ship_flag field
def DM43_decode_val_pass_through(val,oldVal,newVal,field,error_list):
	flagName = 'DM43_'+field
	return F.when(((F.isnull(val)) | (val == '')) & (~error_list.contains(flagName)),True) \
			.when(val.isin(oldVal),False) \
			.when((val.isin(newVal)) & (error_list.contains(flagName)),True) \
			.when((~val.isin(newVal)) & (~error_list.contains(flagName)),True) \
			.when((val.isin(newVal)) & (~error_list.contains(flagName)),True) \
			.otherwise(False)

# COMMAND ----------

def DM44_replace_invalid_val_chk(val, invalid_value,field, error_list):
  flagName = "DM44_"+field
  return F.when((F.regexp_extract(val,invalid_value, 0) != "") & (~error_list.contains(flagName)), False) \
        .when((((~F.isnull(val)) & (val != ""))& (F.regexp_extract(val,invalid_value, 0) == "")) & (error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

def DM61_str_trans_chk(val, field, pattern, error_list):
  flagName = "DM61_"+field
  #case for match and not flagged, then case for not match and flagged
  return F.when(((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '')) & (~error_list.contains(flagName)),False) \
        .when((F.regexp_extract(val,pattern,0) != "") & (F.isnull(val)) & (val == '') & (error_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def udf_DM62_str_pattern_validation(val, field, pattern, mod_list, error_list):
  flagName = "DM62_"+field
  flagNameDM4 = "DM4"
  return F.when(((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '')) & (error_list.contains(flagName)),False) \
        .when(((F.regexp_extract(val,pattern,0) != "")) & (~error_list.contains(flagName) & (~mod_list.contains(flagNameDM4))),False) \
        .when(((F.isnull(val)) | (val == '')),True) \
        .otherwise(True)

# COMMAND ----------

#Updated function for CDM Xref pipeline
def DM62_str_pattern_validation_updated(val, field, pattern, mod_list):
  flagName = "DM62_"+field

  return F.when(((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '')) & (mod_list.contains(flagName)), False) \
        .when(((F.regexp_extract(val,pattern,0) != "")) & (~mod_list.contains(flagName)), False) \
        .when(((F.isnull(val)) | (val == '')),True) \
        .otherwise(True)

# COMMAND ----------

def udf_DM65_strt_substr_chk(val, substring, field, error_list):
  flagName = "DM65_"+field
  return F.when((val == ''), True) \
        .when(val.startswith(substring), False) \
        .when((error_list.contains(flagName)) & (~F.isnull(val)), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM58_replace_quotation(val,pattern,field,mod_list):
  flagName = "DM58_"+field
  #case for not match and col Null
  return F.when((F.regexp_extract(val,pattern,0)== "") & (~F.isnull(val)) & (val != "") & (~mod_list.contains(flagName)),False) \
        .when(((F.isnull(val)) & (val == "")) & (~mod_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_ascii(value, error_list):
  return F.when((F.regexp_extract(value,'^[\x00-\x7F]+$',0) == "") & (~error_list.contains("DQ10")),False) \
    .when((F.regexp_extract(value,'^[\x00-\x7F]+$',0) != "") & (error_list.contains("DQ10")),False) \
    .otherwise(True)

#Created by RCB Enrichment Team

def udf_DQ10_validate(value, field, error_list):
  flagName = "DQ10_"+field
  return F.when((F.regexp_extract(value,'^[\x00-\x7F]+$',0) == "") & (~error_list.contains(flagName)),False) \
    .when((F.regexp_extract(value,'^[\x00-\x7F]+$',0) != "") & (error_list.contains(flagName)),False) \
    .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
# After DM4 serial_number would be stripped to 10 Character, hence we are checking F.length(serial_number) > 10 instead of F.length(serial_number) >= 10 
def DQ1_validation(serial_number, error_list):
  return F.when(((serial_number == None)|(serial_number.isNull())) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "NULL")|(F.length(serial_number) < 10) | (F.length(serial_number) > 14)) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "0000000000") | (serial_number == "XXXXXXXXXX")) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number.contains("\"")) | (serial_number.contains(" ")) | (serial_number.contains(".")) | (serial_number.contains("*")))& (~error_list.contains("DQ1")), False) \
        .when((F.length(serial_number) > 10) & (F.length(serial_number) <= 14) & (error_list.contains("DQ1")), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DQ1_re_validate_new(serial_number, error_list, regex):
  return F.when(((serial_number == None)|(serial_number.isNull())) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "NULL")|(F.length(serial_number) < 10) | (F.length(serial_number) > 14)) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "0000000000") | (serial_number == "XXXXXXXXXX")) & (~error_list.contains("DQ1")), False) \
        .when((F.regexp_extract(serial_number, regex, 0)== "") & (~error_list.contains("DQ1")), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DQ2_re_validate(printer_uuid, source_file, error_list):
  return F.when((F.length(printer_uuid) != 36) & (F.regexp_extract(F.lower(source_file), "caspian", 0) == "") & (printer_uuid.contains(" ")) & (printer_uuid.contains("\"")) & (~error_list.contains("DQ2")), False) \
        .when((F.length(printer_uuid) == 36)& (F.regexp_extract(F.lower(source_file),"caspian",0) != "")& (~printer_uuid.contains(" ")) & (~printer_uuid.contains("\"")) & (error_list.contains("DQ2")), False) \
        .otherwise(True)

def udf_DQ3_re_validate(product_number, error_list):
  return F.when(((product_number == None) | (product_number == "NULL") | (product_number == "") | (product_number.contains("\"")) | (product_number.contains(" "))) & (~error_list.contains("DQ3")), False) \
    .when((product_number != None) & (product_number != "NULL") & (product_number != "") & (~product_number.contains("\"")) & (~product_number.contains(" ")) & (error_list.contains("DQ3")), False) \
    .otherwise(True)

# COMMAND ----------

def DQ71_validate(value,field,error_list):
  flag_name = 'DQ71_' + field
  
  # case: there is value greather than or equal zero & it is not flagged , then return True
  # case : the value is less than or equal to zero,then return False, regardless of being flagged or not..
  # case : return True for any other case
  return F.when((value >= 0) & (~error_list.contains(flag_name)), True) \
          .when(((value < 0)& error_list.contains(flag_name)), True) \
          .otherwise(False)

# COMMAND ----------

def DQ72_validate(actual_value,expected_value,field,error_list):
  flag_name = 'DQ72_' + field
  
  # case: there is value match & it is not flagged , then return True
  # case : the value doesn't match & it is flagged return True
  # case : the value doesn't match & it is not flagged return False
  # case : the value is null or empty then return true
  # case : return True for any other case
  return F.when(((actual_value == expected_value) &(~error_list.contains(flag_name))), True) \
          .when((actual_value != expected_value) & (error_list.contains(flag_name)), True) \
          .when((actual_value != expected_value) & (~error_list.contains(flag_name)), False) \
          .when(((F.isnull(actual_value) | (actual_value == '')) & (~error_list.contains(flag_name))), True) \
          .otherwise(True)


# COMMAND ----------

def step_dq998_validate_updated(col1, col2, error_list):
  
  return F.when(((F.isnull(col1) | (col1 == "NULL") | (col1 == "null") | (col1 == "")) & (error_list.contains("DQ998"))) \
               |  ((F.isnull(col2) | (col2 == "NULL") | (col2 == "null") | (col2 == "")) & (error_list.contains("DQ998"))), False) \
         .when(((F.isnull(col1) | (col1 == "NULL") | (col1 == "null") | (col1 == "")) & ~(error_list.contains("DQ998"))) \
               |  ((F.isnull(col2) | (col2 == "NULL") | (col2 == "null") | (col2 == "")) & ~(error_list.contains("DQ998"))), True) \
         .when((~(F.isnull(col1)) & ~(F.isnull(col2))) & ~(error_list.contains("DQ998")), False) \
         .when((~(F.isnull(col1)) & ~(F.isnull(col2))) & ~(error_list.contains("DQ998")), True) \
         .when((F.isnull(col1) & F.isnull(col2)) & (error_list.contains("DQ998")), False) \
         .otherwise(True)

# COMMAND ----------

def step_dq999_validate_updated(col1, error_list, blacklist):
  
  return F.when(((F.col('person_id').isNull()) | (F.col('person_id') == "NULL") | (F.col('person_id') == "null") | (F.col('person_id')== "")) \
           & (F.col('error_list').contains("DQ999")), False) \
         .when(((F.col('person_id').isNull()) | (F.col('person_id') == "NULL") | (F.col('person_id') == "null") | (F.col('person_id') == "")) \
           & (~F.col('error_list').contains("DQ999")), True) \
         .when ((F.col('person_id').isin(cid_blacklist)) & (F.col('error_list').contains("DQ999")), True) \
         .when ((F.col('person_id').isin(cid_blacklist)) & (~F.col('error_list').contains("DQ999")), False) \
         .when ((~F.col('person_id').isin(cid_blacklist)) & (F.col('error_list').contains("DQ999")), False) \
         .otherwise(True)

# COMMAND ----------

def DQ70_null_check(val, field, error_list):
  flagName = 'DQ70_'+field
  return F.when(((val == '') | (val == 'NULL') | (val == 'null') | (val == None) | (F.isnull(val))) & (error_list.contains(flagName)), True) \
          .when(((val == '') | (val == 'NULL') | (val == 'null') | (val == None) | (F.isnull(val))) & (~error_list.contains(flagName)), False) \
          .when(((val != '') | (val != 'NULL') | (val != 'null') | (val != None) | (~F.isnull(val))) & (error_list.contains(flagName)), False) \
          .when(((val != '') | (val != 'NULL') | (val != 'null') | (val != None) | (~F.isnull(val))) & (~error_list.contains(flagName)), True) \
          .otherwise(False)

# COMMAND ----------

def DQ94_null_check(val1,val2, field, error_list):
  flagName = field
  return F.when((((val1 == '') & (val2 == '')) | ((val1 == 'NULL') & (val2 == 'NULL')) | ((val1 == 'null') & (val2 == 'null')) | ((val1 == None) &(val2 == None))| ((F.isnull(val1)) & (F.isnull(val2)))) & (error_list.contains(flagName)), True) \
  .when((((val1 == '') & (val2 == '')) | ((val1 == 'NULL') & (val2 == 'NULL')) | ((val1 == 'null') & (val2 == 'null')) | ((val1 == None) &(val2 == None))| ((F.isnull(val1)) & (F.isnull(val2)))) & (~error_list.contains(flagName)), False) \
  .when((((val1 != '') & (val2 != '')) | ((val1 != 'NULL') & (val2 != 'NULL')) | ((val1 != 'null') & (val2 != 'null')) | ((val1 != None) &(val2 != None))| ((~F.isnull(val1)) & (~F.isnull(val2)))) & (error_list.contains(flagName)), False) \
  .when((((val1 != '') & (val2 != '')) | ((val1 != 'NULL') & (val2 != 'NULL')) | ((val1 != 'null') & (val2 != 'null')) | ((val1 != None) &(val2 != None))| ((~F.isnull(val1)) & (~F.isnull(val2)))) & (~error_list.contains(flagName)), True) \
    .when((((val1 != '') & (val2 == ''))  | ((val1 != 'NULL') & (val2 == 'NULL')) | ((val1 != 'null') & (val2 == 'null')) | ((val1 != None) &(val2 == None))| ((~F.isnull(val1)) & (F.isnull(val2)))) & (~error_list.contains(flagName)), True) \
    .when((((val1 == '') & (val2 != '')) | ((val1 == 'NULL') & (val2 != 'NULL')) | ((val1 == 'null') & (val2 != 'null')) | ((val1 == None) &(val2 != None))| ((F.isnull(val1)) & (~F.isnull(val2)))) & (~error_list.contains(flagName)), True) \
  .otherwise(False)

# COMMAND ----------

###########################################################################################################
###                         DQ70 validation                                           ###
###########################################################################################################
def validateDQ70rule(df_enrich, parquetName, col_name):
  try:
    df_enrich_for_DQ70 = df_enrich.withColumn('verify_dq70', DQ70_null_check(df_enrich.enrolled_on_date, col_name, df_enrich.error_list))
    DQ70_violation_count = df_enrich_for_DQ70.filter(df_enrich_for_DQ70.verify_dq70 != True).count()
    compliance_check_status(DQ70_violation_count,"DQ70")
    
  except Exception as e:
    print(e)
    failedTest.append("Failed to validate DQ70 rule for col "+col_name+" from "+parquetName+" file.")

# COMMAND ----------

###########################################################################################################
###                         DQ94 validation for two column combination null check                                            ###
###########################################################################################################
def validateDQ94rule(df_enrich, parquetName, col_name):
  try:
    df_enrich_for_DQ94 = df_enrich.withColumn('verify_dq94', DQ94_null_check(df_enrich.subscription_id,df_enrich.replacement_index,field, df_enrich.    error_list))
    DQ94_violation_count = df_enrich_for_DQ94.filter(df_enrich_for_DQ94.verify_dq94 != True).count()
    compliance_check_status(DQ94_violation_count,"DQ94")

  except Exception as e:
    print(e)
    failedTest.append("Failed to validate DQ94 rule for col "+col_name+" from "+parquetName+" file.")

# COMMAND ----------

def DQ73_validate_new(val,pattern,field,flag_name,error_list):
  
  return F.when(((val.isNull()) | (val == '') | (val == 'NULL') | (val == 'null')) & (~error_list.contains(flag_name)),False) \
         .when((F.regexp_extract(val, pattern,0)=='') & (~error_list.contains(flag_name)) , False) \
         .when((F.regexp_extract(val, pattern,0)!='') & (error_list.contains(flag_name)) , False) \
         .when((F.regexp_extract(val, pattern,0)=='') & (error_list.contains(flag_name)) , True) \
         .when((F.regexp_extract(val, pattern,0)!='') & (~error_list.contains(flag_name)) , True) \
         .otherwise(True)

# COMMAND ----------

def DQ91_validate(value, field, expected_length, error_list):
  
  flag = 'DQ91_' + field
  
  return F.when((value == expected_length) & (~error_list.contains(flag)),True) \
          .when((value != expected_length) & (error_list.contains(flag)),True) \
          .when((value != expected_length) & (~error_list.contains(flag)),False) \
          .when((value == expected_length) & (error_list.contains(flag)),False) \
          .otherwise(True)

# COMMAND ----------

def DQ92_validation(val1, val2, field1, field2, check_val,error_list):
  
  flag = 'DQ92_' + field1 + '_' + field2
  
  return F.when((val1 != check_val) & error_list.contains(flag),False) \
          .when(((val2 != "") & (val2 != None) & (val2 != "NULL") & (val2 != "null")) & error_list.contains(flag),False) \
          .when((val1 == check_val) & ((val2 == "") | (val2 == None) | (val2 == "NULL") | (val2 == "null")) & ~error_list.contains(flag),False) \
          .otherwise(True)