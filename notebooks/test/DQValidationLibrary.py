# Databricks notebook source
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col, size, upper
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, concat_ws, lit
import re

# COMMAND ----------

# MAGIC  %run ../libraries/enrich_dq_module

# COMMAND ----------

if 'dsr' not in environment.lower():
  state_codes = sc.broadcast(get_state_codes())
  country_list = sc.broadcast(get_country_list(environment))
  regex_patterns = sc.broadcast(get_regex_patterns(environment))
  currency_code = sc.broadcast(get_currency_codes(environment))
  bus_patterns = sc.broadcast(list(map(lambda x: x.replace('*', ''), get_business_patterns(environment))))
  channel_patterns = sc.broadcast(list(map(lambda x:x[0], get_channel_patterns(environment))))
  regex_unknown_list = sc.broadcast('|'.join(map(lambda x: '^' + re.escape(x) + '$', unknown_list)))
  b_invalid_char_regex = sc.broadcast(invalid_chars_regex)

# COMMAND ----------

def udf_DQ1_re_validate(serial_number, error_list):
  return F.when(((serial_number == None)|(serial_number.isNull())) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "NULL")|(F.length(serial_number) < 10) | (F.length(serial_number) > 14)) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "0000000000") | (serial_number == "XXXXXXXXXX")) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number.contains("\"")) | (serial_number.contains(" ")) | (serial_number.contains(".")) | (serial_number.contains("*")))& (~error_list.contains("DQ1")), False) \
        .otherwise(True)

def udf_DQ1_re_validate_new(serial_number, error_list, regex):
  return F.when(((serial_number == None)|(serial_number.isNull())) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "NULL")|(F.length(serial_number) < 10) | (F.length(serial_number) > 14)) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "0000000000") | (serial_number == "XXXXXXXXXX")) & (~error_list.contains("DQ1")), False) \
        .when((F.regexp_extract(serial_number, regex, 0)== "") & (~error_list.contains("DQ1")), False) \
        .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
# After DM4 serial_number would be stripped to 10 Character, hence we are checking F.length(serial_number) > 10 instead of F.length(serial_number) >= 10 
def udf_DQ1_validation(serial_number, error_list):
  return F.when(((serial_number == None)|(serial_number.isNull())) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "NULL")|(F.length(serial_number) < 10) | (F.length(serial_number) > 14)) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number == "0000000000") | (serial_number == "XXXXXXXXXX")) & (~error_list.contains("DQ1")), False) \
        .when(((serial_number.contains("\"")) | (serial_number.contains(" ")) | (serial_number.contains(".")) | (serial_number.contains("*")))& (~error_list.contains("DQ1")), False) \
        .when((F.length(serial_number) > 10) & (F.length(serial_number) <= 14) & (error_list.contains("DQ1")), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DQ2_re_validate(printer_uuid, source_file, error_list):
  return F.when((F.length(printer_uuid) != 36) & (F.regexp_extract(F.lower(source_file), "caspian", 0) == "") & (printer_uuid.contains(" ")) & (printer_uuid.contains("\"")) & (~error_list.contains("DQ2")), False) \
        .when((F.length(printer_uuid) == 36)& (F.regexp_extract(F.lower(source_file),"caspian",0) != "")& (~printer_uuid.contains(" ")) & (~printer_uuid.contains("\"")) & (error_list.contains("DQ2")), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DQ3_re_validate(product_number, error_list):
  return F.when(((product_number == None) | (product_number == "NULL") | (product_number == "") | (product_number.contains("\"")) | (product_number.contains(" "))) & (~error_list.contains("DQ3")), False) \
    .when((product_number != None) & (product_number != "NULL") & (product_number != "") & (~product_number.contains("\"")) & (~product_number.contains(" ")) & (error_list.contains("DQ3")), False) \
    .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def udf_DQ3_validation(product_number, error_list):
  return F.when(((product_number == None) | (product_number == "NULL") | (product_number == "$") | (product_number == "") | (product_number.contains("\"")) | (product_number.contains(" "))) & (~error_list.contains("DQ3")), False) \
    .when((product_number != None) & (product_number != "NULL") & (product_number != "$") & (product_number != "") & (~product_number.contains("\"")) & (~product_number.contains(" ")) & (error_list.contains("DQ3")), False) \
    .otherwise(True)

# COMMAND ----------

def udf_DM4_re_validate(serial_number,error_list):
  return F.when((F.length(serial_number) > 10) & (~error_list.contains("DM4")),False) \
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

maxdate = datetime.now().strftime('%Y-%m-%d')
def udf_DQ13_re_validate(date,mindate, error_list_full):  
  return F.when((date < F.date_format(lit(mindate), 'yyyy-MM-dd')) & (date > F.date_format(lit(maxdate), 'yyyy-MM-dd')) & (~error_list_full.contains("DQ13")), False) \
          .when((date > F.date_format(lit(mindate), 'yyyy-MM-dd')) & (date < F.date_format(lit(maxdate), 'yyyy-MM-dd')) & (error_list_full.contains("DQ13")), False) \
          .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
maxdate = datetime.now().strftime('%Y-%m-%d')
def udf_DQ13_validation(date,mindate, error_list_full):  
  return F.when(((date < F.date_format(lit(mindate), 'yyyy-MM-dd')) | (date > F.date_format(lit(maxdate), 'yyyy-MM-dd'))) & (~error_list_full.contains("DQ13")), False) \
          .when((date >= F.date_format(lit(mindate), 'yyyy-MM-dd')) & (date <= F.date_format(lit(maxdate), 'yyyy-MM-dd')) & (error_list_full.contains("DQ13")), False) \
          .otherwise(True)

# COMMAND ----------

def udf_DQ11_validate(ship_to_country, error_list):
   return F.when((~ship_to_country.isin(country_list.value)) & (~error_list.contains("DQ11")), False) \
   .when((ship_to_country.isin(country_list.value)) & (error_list.contains("DQ11")), False) \
   .otherwise(True)


# COMMAND ----------

#Created by RCB Enrichment Team

def udf_DQ11_validation(ship_to_country, error_list):
   return F.when((~ship_to_country.isin(country_list)) & (~error_list.contains("DQ11")), False) \
    .when((ship_to_country.isin(country_list)) & (error_list.contains("DQ11")), False) \
    .otherwise(True)

# COMMAND ----------

def udf_DM12_re_validate(product_number, error_list):
  return F.when(product_number.contains('#') & (error_list.contains("DM12")),False) \
    .when(product_number.contains('#') & (~error_list.contains("DM12")),False) \
    .when((F.length(product_number) > 8) & (product_number.contains('#')) & (~error_list.contains("DM12")),False) \
    .otherwise(True)

# COMMAND ----------

def udf_DM14_re_validate(ship_address_orig, error_list):
  return F.when((~ship_address_orig.contains('??') & ~ship_address_orig.contains('N/A')) & (error_list.contains("DM14")),False) \
    .when((ship_address_orig.contains('??') | ship_address_orig.contains('N/A')) & (~error_list.contains("DM14")),False) \
    .otherwise(True)

# COMMAND ----------

def validate_DM15(ship_to_country,ship_zip, error_list):
#   regex_dict = ast.literal_eval(regex_dict)
  if not error_list:
    error_list = ''
  error_array_list = error_list.split(",")
  
  if ship_zip == 'null' or ship_zip == None or ship_zip == "":
    return True
  if ship_to_country not in regex_dict and "DM15" not in error_array_list:
    return False
  
  if ship_to_country in regex_dict:
    pattern = re.compile(regex_dict[ship_to_country])
    if not pattern.match(ship_zip) and "DM15" not in error_array_list:
      return False

  if((ship_to_country == "US" and len(ship_zip) > 5) and ("DM15" not in error_array_list)):
    return False
#   if((ship_to_country == "US" and len(ship_zip_orig) <= 5) and ("DM15" in error_array_list)):
#     return False
#   if((ship_to_country != "US" and len(ship_zip_orig) > 5) and ("DM15" in error_array_list)):
#     return False
#   if((ship_to_country != "US" and len(ship_zip_orig) <= 5) and ("DM15" in error_array_list)):
#     return False
  return True
  
udf_DM15_re_validate = udf(lambda w,x,y: validate_DM15(w,x,y), StringType())  

# COMMAND ----------

def validate_DM16(ship_address_orig,error_list):
  if ship_address_orig is None:
    ship_address_orig = ''
    
  error_array_list = error_list.split(",")
  
  if ((ship_address_orig.upper().startswith(tuple(map(lambda x:x[0],channel_patterns)))) and ("DM16" not in error_array_list)):
    return False
  if (( not ship_address_orig.upper().startswith(tuple(map(lambda x:x[0],channel_patterns)))) and ("DM16" in error_array_list)):
    return False
  
udf_DM16_re_validate = udf(lambda x,y: validate_DM16(x,y), StringType())  

# COMMAND ----------

def validate_DM17(channel_orig, channel_ship_cnt,expt_total,error_list):
  
  error_array_list = error_list.split(",")
  
  ListOfValues = map(lambda x:x if fnmatch.fnmatchcase(channel_orig.upper(), x.upper()) else None, business_patterns)
  if ((len(set(ListOfValues))>1) and ("DM17" in error_array_list)):
    return False
  if ((len(set(ListOfValues))== 1) and ("DM17" not in error_array_list)):
    return False
  if ((len(set(ListOfValues))>1) and (channel_ship_cnt < expt_total) and ("DM17" not in error_array_list)):
    return False
  else:
    return True   
  
udf_DM17_re_validate = udf(lambda x,y,z,j: validate_DM17(x,y,z,j), StringType()) 

# COMMAND ----------

def udf_DQ18_re_validate(cid, error_list):
  # checking data with invalid and valid cid marked with DQ18
  return F.when(((cid == None)| (cid == "") | (F.length(cid) != 16) | (cid == "0000000000000000")) & (~error_list.contains("DQ18")), False) \
    .when((cid != None) & (cid != "") & (F.length(cid) == 16) & (cid != "0000000000000000") & (error_list.contains("DQ18")), False) \
    .otherwise(True)

# COMMAND ----------

def udf_DM19_re_validate(segment_cd, segment_desc, error_list):
  #checking for invalid segment desc and not flagged with DM19
  return F.when(((segment_desc == None) | (segment_desc == "") | (segment_desc == "Segment Desc") | (segment_desc == "Segment Desc2")) & (~error_list.contains("DM19")),False) \
        .when((segment_desc != None) & (segment_desc != "") & (segment_desc != "Segment Desc") & (segment_desc != "Segment Desc2") & (error_list.contains("DM19")),False) \
        .when(((segment_cd == "000") & (segment_desc != "Unknown")) | ((segment_cd == "001") & (segment_desc != "Business")) | ((segment_cd == "002") & (segment_desc != "Personal Use")),False) \
        .when(((segment_cd == "003") & (segment_desc != "Home-based Business")) | ((segment_cd == "004") & (segment_desc != "Telecommuting")),False) \
        .when(((segment_cd == "005") & (segment_desc != "Business with 9 or fewer employees")) | ((segment_cd == "006") & (segment_desc != "Business with 10 or more employees")),False) \
        .otherwise(True)

# COMMAND ----------

def validate_DM20():
  pass

# COMMAND ----------

def udf_DM21_validate(country_code,field, mod_list):
  flagName = "DM21_"+field
  return F.when((~country_code.isin(country_list.value)) & (country_code != "XU") & (~mod_list.contains(flagName)),False) \
    .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
# XU can come as country code and it's a valid country code on ref_enrich.country_ref table
def udf_DM21_verify(country_code,field, mod_list):
  flagName = "DM21_"+field
  return F.when((~country_code.isin(country_list.value)) & (~mod_list.contains(flagName)),False) \
	.when((country_code.isin(country_list.value)) & (country_code != "XU") & (mod_list.contains(flagName)),False) \
    .when((~country_code.isin(country_list.value)) & (country_code != "XU") & (~mod_list.contains(flagName)),False) \
    .otherwise(True)

# COMMAND ----------

def udf_DM22_state_code(shipvalue, mod_list):
  for i in state_codes.value:
    return F.when((~F.upper(shipvalue).isin(state_codes.value)) & (~mod_list.contains("DM22")), False) \
          .when((F.upper(shipvalue).isin(state_codes.value)) & (mod_list.contains("DM22")), False) \
          .otherwise(True)

# COMMAND ----------

def validate_DM23():
  pass

# COMMAND ----------

# Created by RCB team
def udf_DM24_invalid_char_validate(cityVal,mod_list,field):
  return F.when((F.regexp_extract(cityVal, b_invalid_char_regex.value, 0) == "" ) & (cityVal != "") & (mod_list.contains("DM24_" + field)), False) \
          .when((F.regexp_extract(cityVal, b_invalid_char_regex.value, 0) != "" ) & (~mod_list.contains("DM24_" + field)), False) \
          .otherwise(True)

# COMMAND ----------

def udf_DM24_invalid_char(cityVal,mod_list):
  return F.when((F.regexp_extract(cityVal, b_invalid_char_regex.value, 0) == "" ) & (cityVal != "") & (mod_list.contains("DM24")), False) \
          .when((F.regexp_extract(cityVal, b_invalid_char_regex.value, 0) != "" ) & (~mod_list.contains("DM24")), False) \
          .otherwise(True)

# COMMAND ----------

def validate_DM25():
  pass

# COMMAND ----------

def validate_DM26():
  pass

# COMMAND ----------

def udf_DM27_item_type_len2(value, mod_list, minlen, field=None, maxlen=None):
  if field is not None:
    field = concat(lit("_"),field)
  else:
    field = lit('')
  if maxlen is None:
    maxlen = minlen
    
  return F.when(((F.length(value)< minlen)| (F.length(value)> maxlen)) & (~mod_list.contains("DM27"+field)),False) \
        .when(((F.length(value) == minlen) | (F.length(value)== maxlen)) & (mod_list.contains("DM27"+field)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM28_unknown(value, mod_list):
  for i in unknown_list:
    return F.when((F.regexp_extract(value, regex_unknown_list.value, 0) != "") & (value != "") & (~mod_list.contains("DM28")), False) \
            .when((F.regexp_extract(value, regex_unknown_list.value, 0) == "") & (value == "")& (~mod_list.contains("DM28")), False) \
            .otherwise(True)

# COMMAND ----------

def udf_DM29(value, to_replace, mod_list):
  return F.when((value == to_replace) & (~mod_list.contains("DM29")), False) \
          .otherwise(True)

# COMMAND ----------

def udf_DM30_city_state(value, mod_list):
  return F.when((value.contains(',')) & (~mod_list.contains("DM30")),False) \
        .when((~value.contains(',')) & (mod_list.contains("DM30")),False) \
        .otherwise(True)

# COMMAND ----------

def validate_DM31():
  pass

# COMMAND ----------

def validate_DM33():
  pass

# COMMAND ----------

def udf_DQ34_length_chk(val, field, length, min_val, max_val, error_list):
  flagName = "DQ34_"+field
  return F.when((F.length(val) <= min_val) & (F.length(val) >= max_val) & (~error_list.contains(flagName)), False) \
     .when((F.length(val) > min_val) & (F.length(val) <= max_val) & (error_list.contains(flagName)), False) \
     .when((F.length(val) == length) & (error_list.contains(flagName)), False) \
     .when((F.length(val) != length) & (~error_list.contains(flagName)), False) \
     .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def udf_DQ34_validation(val, field, length, min_val, max_val, error_list):
  flagName = "DQ34_"+field
  return F.when(((F.length(val) <= min_val) | (F.length(val) >= max_val)) & (~error_list.contains(flagName)), False) \
     .when((F.length(val) > min_val) & (F.length(val) < max_val) & (error_list.contains(flagName)), False) \
     .when((F.length(val) == length) & (error_list.contains(flagName)), False) \
     .when((F.length(val) != length) & (~error_list.contains(flagName)), False) \
     .otherwise(True)

# COMMAND ----------

def udf_DQ35_regex_chk(val, field, pattern, error_list):
  flagName = "DQ35_"+field
  #case for match and not flagged, then case for not match and flagged
  return F.when(((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '')) & (~error_list.contains(flagName)),False) \
        .when((F.regexp_extract(val,pattern,0) != "") & (F.isnull(val)) & (val == '') & (error_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def udf_DQ35_validation(val, field, pattern, error_list,mod_list):
  flagName  = "DQ35_"+field
  flagName2 = "DM4"
  flagName3 = "DM62"
  flagName4 = "DM65"
  #case for match and not flagged, then case for not match and flagged
  return F.when((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '') & (~error_list.contains(flagName)),False) \
        .when(((F.regexp_extract(val,pattern,0) != "") & (error_list.contains(flagName) & ~mod_list.contains(flagName2) & ~mod_list.contains(flagName3) & ~mod_list.contains(flagName4))),False) \
        .when(((F.isnull(val) | (val == "")) & (error_list.contains(flagName) & ~mod_list.contains(flagName2) & ~mod_list.contains(flagName3) & ~mod_list.contains(flagName4))),False) \
        .otherwise(True)

# COMMAND ----------

## Commenting as flagName was not defined, which was causing failure
# def udf_DQ36_entity(val, error_list):
#   for i in unknown_list:
#     return F.when(((val.contains(i)) | (val == "") | (val== "null") | (val == None)) & (~error_list.contains(flagName)), False) \
#           .when((~val.contains(i)) & (val != "") & (val!= "null") & (val != None) & (error_list.contains(flagName)), False) \
#           .otherwise(True)

#Created by RCB Enterprise Team
def udf_DQ36_entity(val, error_list):
  flagName="DQ36"
  for i in unknown_list:
    return F.when(((val.contains(i)) | (val == "") | (val== "null") | (val == None)) & (~error_list.contains(flagName)), False) \
          .when((~val.contains(i)) & (val != "") & (val!= "null") & (val != None) & (error_list.contains(flagName)), False) \
          .otherwise(True)

# COMMAND ----------

def udf_DM37_valid_in_list_chk(val, field, entries_list, error_list):
  flagName = "DM37_"+field
  return F.when((val.isin(entries_list)) & (val != None) & (~error_list.contains(flagName)), False) \
      .when((~val.isin(entries_list)) & (val != None) & (error_list.contains(flagName)), False) \
      .otherwise(True)

# COMMAND ----------

def udf_DQ38_val_in_list_chk(val, field, entries_list, error_list):
  flagName = "DQ38_"+field
  return F.when((val.isin(entries_list)) & (~error_list.contains(flagName)), False) \
        .when((~val.isin(entries_list)) & (error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

def validate_DM39(val, error_list):
  if not error_list:
    error_list = ''
  error_array_list = error_list.split(",")
  
  #check if value is none - the flag isn't applied if it's none
  if (val == "" or val == None) and "DM39" not in error_array_list:
    return True
  
  #check for preceeding 0s
  #both flagged and unflagged is ok
  if val == (val.lstrip("0") or "0"):
    return True
  return False

udf_DM39_preceeding_zeros = udf(lambda x,y: validate_DM39(x,y), StringType())

# COMMAND ----------

def validate_DM40(val1, val2, field, lookup, error_list):
  if not error_list:
    error_list = ''
  error_array_list = error_list.split(",")
  
  lookup = json.loads(lookup)
  
  #check if val1 is none, flagged
  if (val1 == "" or val1 == None) and "DM40_"+field in error_array_list:
    return True
  
  #check if val1 is not in lookup, flagged
  if val1 not in lookup and "DM40_"+field in error_array_list:
    return True
  
  #check if val2 corresponds with val1, can be flagged or unflagged
  if val2 == lookup.get(val1):
    return True
  
  return False

udf_DM40_lookup = udf(lambda x,y,z,i,j: validate_DM40(x,y,z,i,j), StringType())

# COMMAND ----------

def udf_DM43_decode_val(val,oldVal,newVal,field,error_list):
  flagName = 'DM43_'+field
  return F.when(((F.isnull(val)) | (val == '')) & (~error_list.contains(flagName)),True) \
        .when((val.isin(oldVal)) & (~error_list.contains(flagName)),False) \
        .when((val.isin(newVal)) & (~error_list.contains(flagName)),True) \
        .otherwise(True)

# COMMAND ----------

# based on Jira ticket PBDPS-12809 adding pass through for DM43 drop_ship_flag field
def udf_DM43_decode_val_pass_through(val,oldVal,newVal,field,error_list):
	flagName = 'DM43_'+field
	return F.when(((F.isnull(val)) | (val == '')) & (~error_list.contains(flagName)),True) \
			.when(val.isin(oldVal),False) \
			.when((val.isin(newVal)) & (error_list.contains(flagName)),True) \
			.when((~val.isin(newVal)) & (~error_list.contains(flagName)),True) \
			.when((val.isin(newVal)) & (~error_list.contains(flagName)),True) \
			.otherwise(False)

# COMMAND ----------

def udf_DM44_replace_invalid_val_chk(val, invalid_value,field, error_list):
  flagName = "DM44_"+field
  return F.when((F.regexp_extract(val,invalid_value, 0) != "") & (~error_list.contains(flagName)), False) \
        .when((((~F.isnull(val)) & (val != ""))& (F.regexp_extract(val,invalid_value, 0) == "")) & (error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

def validate_DM54(zip_code, country_code,field, error_list):
  if not error_list:
    error_list = ''
  error_array_list = error_list.split(",")
  global regex_patterns
  pr("coming here")
  if (country_code == "" or country_code == None or country_code == 'XU' or country_code == 'XX' or country_code == 'XW') and (zip_code != "" or zip_code != None):
    return True
  if (country_code != "" or country_code != None or country_code != 'XU' or country_code != 'XW' or country_code != 'XW') and (zip_code == "" or zip_code == None):
    return True
  if country_code == "US" and len(zip_code) != 5:
    return False
  try:
    if country_code in regex_patterns.value:
      pattern = re.compile((regex_patterns.value[country_code]).strip())
      if not pattern.search(zip_code) and country_code != None and "DM54_"+field not in error_array_list:
        return False
      if pattern.search(zip_code) and country_code == None:
        return False
      return True
    if country_code not in regex_patterns.value:
      return None
  except:
    return False
  return False

udf_DM54_replace_valid_zip_code = udf(lambda x,y,z,i: validate_DM54(x,y,z,i), StringType())

# COMMAND ----------

#Created by RCB Enrichment Team
def DM54_validation(zip_code, country_code,field, error_list):
  if not error_list:
    error_list = ''
  error_array_list = error_list.split(",")
  flagName = "DM54_"+field
  global regex_patterns

  if (country_code == "" or country_code == None or country_code == 'XU' or country_code == 'XX' or country_code == 'XW') and (zip_code != "" or zip_code != None):
    return True
  if (country_code != "" or country_code != None or country_code != 'XU' or country_code != 'XW' or country_code != 'XW') and (zip_code == "" or zip_code == None):
    return True
  if flagName in error_array_list:
   
    if country_code == "US" and len(zip_code) != 5:
      return True
    elif country_code in regex_patterns.value:
      pattern = re.compile((regex_patterns.value[country_code]).strip())
      if not pattern.search(zip_code) and country_code != None:
        return True
      elif pattern.search(zip_code) and country_code == None:
        return True
      else:
        return False
    else:
      return False
       
  elif flagName not in error_array_list:
    
    if country_code == "US" and len(zip_code) != 5:
      return False
    elif country_code in regex_patterns.value:
      pattern = re.compile((regex_patterns.value[country_code]).strip())
      if not pattern.search(zip_code) and country_code != None:
         return False
      elif pattern.search(zip_code) and country_code == None:
        return False
      else:
        return True
    else:
      return True
  else:
    return True
    
udf_DM54_invalid_zip_code = udf(lambda x,y,z,i: DM54_validation(x,y,z,i), StringType())

# COMMAND ----------

def udf_DM55_replace_invalid_currency_code(currency_col_val,field,mod_list):
#   global currency_code
  flagName = "DM55_"+field
  currencyCodeVal = currency_code.value.values()
  print(currency_col_val)
  return F.when((~currency_col_val.isin(currencyCodeVal)) & (~mod_list.contains(flagName)), False) \
        .when((F.length(currency_col_val) != 3) & (~F.isnull(currency_col_val)) & (currency_col_val != "") & (~mod_list.contains(flagName)), False) \
        .when(((~F.isnull(currency_col_val)) & (currency_col_val == "")) & (mod_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def udf_DM55_validation(currency_col_val,field,mod_list):
  flagName = "DM55_"+field
  currencyCodeVal = currency_code.value.values()
  return F.when((~currency_col_val.isin(currencyCodeVal)) & (~mod_list.contains(flagName)), False) \
        .when((F.length(currency_col_val) != 3) & (~F.isnull(currency_col_val)) & (currency_col_val != "") & (~mod_list.contains(flagName)), False) \
        .when(((F.isnull(currency_col_val)) | (currency_col_val == "")) & (mod_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM53_replace_outide_dates_with_null(dale_cols,strtDate,endDate,field,mod_list):  
  col_date = dale_cols
  flagName = "DM53_"+field
  nullVal = str("null")
  return F.when((( col_date < strtDate) | (col_date > endDate)) & ((col_date != None) & (col_date != "") & (col_date != "null")) & (~mod_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def udf_DM53_validation(col_date,strtDate,endDate,field,mod_list):  
  flagName = "DM53_"+field
  return F.when((( col_date < strtDate) | (col_date > endDate)) & ((col_date != None) & (col_date != "") & (col_date != "null")) & (~mod_list.contains(flagName)),False) \
          .when(((col_date >= strtDate) & (col_date <= endDate) & (mod_list.contains(flagName))),False) \
          .otherwise(True)

# COMMAND ----------

def udf_DM50_replace_std_specific_value(std_col_val,valid_val,field,mod_list):
  flagName = "DM50_"+field
  #check for not match invalid value
  return F.when((std_col_val.isin(valid_val)) & (~mod_list.contains(flagName)),False) \
        .when((std_col_val != None) & (std_col_val != ""),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM46_replace_non_num_with_null(val,pattern,field,mod_list):
  flagName = "DM46_"+field
  #case for not match and col Null
  return F.when((F.regexp_extract(val,pattern,0)!= "") & (mod_list.contains(flagName)),False) \
        .when(((~F.isnull(val)) & (val != "")) & (mod_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM56_replace_non_alpanum_with_null(val, pattern,field,mod_list):
  flagName = "DM56_"+field
  #case for not match and col Null
  return F.when((F.regexp_extract(val,pattern,0)!= "") & (F.isnull(val)) & (val == "") & (mod_list.contains(flagName)),False) \
        .when(((~F.isnull(val)) & (val != "")) & (mod_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM57_replace_non_alpanum_with_null(val,pattern,field,mod_list):
  flagName = "DM57_"+field
  #case for not match and col Null
  return F.when((F.regexp_extract(val,pattern,0)!= "") & (F.isnull(val)) & (val == "") & (mod_list.contains(flagName)),False) \
        .when(((~F.isnull(val)) & (val != "")) & (mod_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM58_replace_quotation(val,pattern,field,mod_list):
  flagName = "DM58_"+field
  #case for not match and col Null
  return F.when((F.regexp_extract(val,pattern,0)== "") & (~F.isnull(val)) & (val != "") & (~mod_list.contains(flagName)),False) \
        .when(((F.isnull(val)) & (val == "")) & (~mod_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def validate_DM59_ShipAddr(val,colName):
  dcVal = val.encode('utf-8','strict')
  if dcVal.decode('cp1252','strict') != val and (val == '' or val == None):
    return False
  return True

def validate_DM59(val,colName):
  dcVal = val.decode('utf-8','strict')
  ecVal = dcVal.encode('cp1252','strict')
  #case for verify decoded string match with encoded string
  if ecVal.decode('cp1252','strict') != val and (val == '' or val == None):
    return False
  return True

udf_DM59_replace_unicode_shipaddr = udf(lambda x,y: validate_DM59_ShipAddr(x,y), StringType())
udf_DM59_replace_unicode = udf(lambda x,y: validate_DM59(x,y), StringType())

# COMMAND ----------

def udf_DM17_modify_cols_indiv(chanl_cols, ship_cnt, expt_total,field,error_list):
  flagName ="DM17_"+field
  return F.when((~chanl_cols.isin(bus_patterns.value)) & (ship_cnt < expt_total) & (~error_list.contains(flagName)), False) \
        .when((chanl_cols.isin(bus_patterns.value)) & (ship_cnt > expt_total) & (error_list.contains(flagName)), False) \
        .when((chanl_cols.isin(bus_patterns.value)) & (ship_cnt < expt_total) & (error_list.contains(flagName)), False) \
        .when((~chanl_cols.isin(bus_patterns.value)) & (ship_cnt > expt_total) & (error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def udf_DM17_validation_hash(chanl_cols,verify_col, ship_cnt, expt_total,field,error_list):
  flagName ="DM17_"+field

  return F.when((~chanl_cols.isin(bus_patterns.value)) & (~F.isnull(chanl_cols)) & (chanl_cols !="") & (~F.isnull(verify_col)) & (verify_col !="") & (ship_cnt < expt_total) & (length(chanl_cols)==64) & (length(verify_col)==64) & (~error_list.contains(flagName)), False) \
        .when((length(chanl_cols)==64) & (length(verify_col)==64) & (ship_cnt < expt_total) & (error_list.contains(flagName)), True) \
        .when((length(chanl_cols)==64) & (length(verify_col)==64) & (ship_cnt < expt_total) & (~error_list.contains(flagName)), False) \
        .when((chanl_cols.isin(bus_patterns.value)) & (error_list.contains(flagName)), False) \
        .when((~chanl_cols.isin(bus_patterns.value)) & (ship_cnt > expt_total) & (error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM60_modify_bill_ship_cols(cust_name, enduser_addr, bill_addr, field, error_list):
  flagName = "DM60_"+field
  return F.when((cust_name == 'INDIVIDUAL') & (enduser_addr == bill_addr) & (field != 'INDIVIDUAL') & (~error_list.contains(flagName)),False) \
        .when((cust_name != 'INDIVIDUAL') & (enduser_addr != bill_addr) & (field == 'INDIVIDUAL') & (error_list.contains(flagName)),False) \
        .when((cust_name != 'INDIVIDUAL') & (enduser_addr == bill_addr) & (field == 'INDIVIDUAL') & (error_list.contains(flagName)),False) \
        .when((cust_name == 'INDIVIDUAL') & (enduser_addr != bill_addr) & (field == 'INDIVIDUAL') & (error_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

#Created by RCB Enrichment Team
def udf_DM60_verify(chanl_cols, cust_name,cust_addr,addr1,field17,field,mod_list):
  flagName ="DM60_"+field
  flagName17 = "DM17_"+field17+"_orig"
  return F.when((length(cust_name)==64) & (length(cust_addr)==64) & (length(addr1)==64) & (mod_list.contains(flagName)) & (mod_list.contains(flagName17)),True)\
          .when((length(cust_name)==64) & (length(cust_addr)==64) & (length(addr1)==64) & (~mod_list.contains(flagName)) & (mod_list.contains(flagName17)),False)\
          .when((length(cust_name)==64) & (length(cust_addr)==64) & (length(addr1)==64) & (mod_list.contains(flagName)) & (~mod_list.contains(flagName17)),False)\
          .when((length(cust_name)==64) & (length(cust_addr)==64) & (length(addr1)==64) & (~mod_list.contains(flagName)) & (~mod_list.contains(flagName17)),False)\
          .otherwise(True)

# COMMAND ----------

def udf_DM61_str_trans_chk(val, field, pattern, error_list):
  flagName = "DM61_"+field
  #case for match and not flagged, then case for not match and flagged
  return F.when(((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '')) & (~error_list.contains(flagName)),False) \
        .when((F.regexp_extract(val,pattern,0) != "") & (F.isnull(val)) & (val == '') & (error_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM62_str_trans_compl_chk(val, field, pattern, error_list):
  flagName = "DM62_"+field
  #case for match and not flagged, then case for not match and flagged
  return F.when(((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '')) & (error_list.contains(flagName)),False) \
        .when(((F.regexp_extract(val,pattern,0) != "")) & (~error_list.contains(flagName)),False) \
        .when(((F.isnull(val)) | (val == '')),True) \
        .otherwise(True)

# COMMAND ----------

def udf_DM62_str_trans_compl_chk_hash(val, field, pattern,hashPattern,hashLen, error_list):
  flagName = "DM62_"+field
  # case : for value, if it does not match the pattern and it is flagged, then mark it as False
  # case : for value, if it matchs directly to pattern, then mark it as False
  # case : for value, if it is matching hastpattern & conforms to lenght constraints, and is flagged, then mark it as True.
  # case : for value, if it is null or empty string, then mark it as True
  # case : for any other value, then mark it as True
  return F.when(((F.regexp_extract(val,pattern,0) == "") & (~F.isnull(val)) & (val != '')) & ((F.regexp_extract(val,hashPattern,0) == "")) & (error_list.contains(flagName)),False) \
        .when(((F.regexp_extract(val,pattern,0) != "")),False) \
        .when(((F.regexp_extract(val,hashPattern,0) != "")) & (F.length(val) == hashLen) & (error_list.contains(flagName)),True) \
        .when(((F.isnull(val)) | (val == '')),True) \
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

def udf_DM63_non_prntble_chk(val,field,error_list):
  flagName = "DM63_"+field
  pattern = "^[ -~]+$"
  return F.when(((F.isnull(val)) | (val == ''))  & (error_list.contains(flagName)),False) \
        .when((F.regexp_extract(val,pattern,0) == "") & (error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM64_neg_val_chk(val,field,error_list):
  flagName = "DM64_"+field
  return F.when((val < 0) & (~error_list.contains(flagName)), False) \
        .when((val.startswith('-')) & (~error_list.contains(flagName)), False) \
        .when(((val > 0) | (val == 0)) & (error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM65_strt_substr_chk(val, substring, field, error_list):
  flagName = "DM65_"+field
  return F.when((val == ''), True) \
        .when(val.startswith(substring), False) \
        .when((error_list.contains(flagName)) & (~F.isnull(val)), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DQ66_check_non_num(val, pattern, field, error_list):
  flagName = "DQ66_"+field
  return F.when((F.regexp_extract(val,pattern,0)!= "") & (error_list.contains(flagName)),False) \
        .when(((~F.isnull(val)) & (val != "")) & (error_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DQ67_valid_list(val, valid_list, field, error_list):
  flagName = "DQ67_"+field
  return F.when((~val.isin(valid_list)) & (error_list.contains(flagName)), True) \
          .when((val.isin(valid_list)) & (~error_list.contains(flagName)), True) \
          .when((val.isin(valid_list)) & (error_list.contains(flagName)), False) \
          .when((~val.isin(valid_list)) & (~error_list.contains(flagName)), False) \
          .otherwise(False)


# COMMAND ----------

def udf_DQ68_cohort_col(val1, val2, exp_val1, exp_val2, field, error_list):
  flagName = "DQ68_"+field
  return F.when(((val1 == exp_val1) & (val2 == exp_val2)) & (error_list.contains(flagName)), False) \
          .when(((val1 == exp_val1) & (val2 == exp_val2)) & (~error_list.contains(flagName)), True) \
          .when(((val1 != exp_val1) & (val2 != exp_val2)) & (error_list.contains(flagName)), False) \
          .when(((val1 != exp_val1) & (val2 != exp_val2)) & (~error_list.contains(flagName)), True) \
          .when((((val1 == exp_val1) & (val2 != exp_val2)) | ((val1 != exp_val1) & (val2 == exp_val2))) & (error_list.contains(flagName)), True) \
          .when((((val1 == exp_val1) & (val2 != exp_val2)) | ((val1 != exp_val1) & (val2 == exp_val2))) & (~error_list.contains(flagName)), False) \
          .otherwise(False)

# COMMAND ----------

def udf_DM69_cohort_col_null(val1, val2, exp_val1, exp_val2, field, error_list):
  flagName = "DM69_"+field
  return F.when(F.isnull(val1) & F.isnull(val2) & (error_list.contains(flagName)), False) \
          .when(F.isnull(val1) & F.isnull(val2) & (~error_list.contains(flagName)), True) \
          .when(((val1 == ' ') | (val1 == 'NULL') | (val1 == 'null') | (val1 == None) | (F.isnull(val1)) | ((val2 == ' ') | (val2 == 'NULL') | (val2 == 'null') | (val2 == None) | (F.isnull(val2)))), False) \
          .otherwise(udf_DQ68_cohort_col(val1, val2, exp_val1, exp_val2, field, error_list))

# COMMAND ----------

def udf_DQ70_null_check(val, field, error_list):
  flagName = 'DQ70_'+field
  return F.when(((val == '') | (val == 'NULL') | (val == 'null') | (val == None) | (F.isnull(val))) & (error_list.contains(flagName)), True) \
          .when(((val == '') | (val == 'NULL') | (val == 'null') | (val == None) | (F.isnull(val))) & (~error_list.contains(flagName)), False) \
          .when(((val != '') | (val != 'NULL') | (val != 'null') | (val != None) | (~F.isnull(val))) & (error_list.contains(flagName)), False) \
          .when(((val != '') | (val != 'NULL') | (val != 'null') | (val != None) | (~F.isnull(val))) & (~error_list.contains(flagName)), True) \
          .otherwise(False)

# COMMAND ----------

# def step_dq998_validate(col1, col2,blacklist,error_list):
#   for val in blacklist:
#     if((col1==None and val[0]==None) and (col2==None and val[1]==None) and (("DQ998") not in (error_list))):
#       return False
#     elif((col1==val[0]) and (col2==val[1]) and (("DQ998") not in (error_list))):
#       return False
#     elif((col1==val[0]) and (col2==val[1]) and (("DQ998") in (error_list))):
#       return True
#   else:
# #   return True
#     if ("DQ998") in (error_list):
#       return False
#     else :
#       return True  

# Modified by Enterprise team on 07/06 to fix the existing issue

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
  return udf(lambda a,b,c: step_dq998_validate(a,b,blacklist,c))

# COMMAND ----------

# Modified by Enterprise team on 07/06 to fix the existing issue

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

def udf_DQ71_validate(value,field,error_list):
  flag_name = 'DQ71_' + field
  
  # case: there is value greather than or equal zero & it is not flagged , then return True
  # case : the value is less than or equal to zero,then return False, regardless of being flagged or not..
  # case : return True for any other case
  return F.when((value >= 0) & (~error_list.contains(flag_name)), True) \
          .when(((value < 0)& error_list.contains(flag_name)), True) \
          .otherwise(False)


# COMMAND ----------

def udf_DQ72_validate(actual_value,expected_value,field,error_list):
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

def udf_DQ73_validate(actual_value,expected_value,field,error_list):
  
  flag_name = 'DQ73_' + field
  
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


def udf_DQ73_validate_new(val,pattern,field,flag_name,error_list):
  
  return F.when(((val.isNull()) | (val == '') | (val == 'NULL') | (val == 'null')) & (~error_list.contains(flag_name)),False) \
         .when((F.regexp_extract(val, pattern,0)=='') & (~error_list.contains(flag_name)) , False) \
         .when((F.regexp_extract(val, pattern,0)!='') & (error_list.contains(flag_name)) , False) \
         .when((F.regexp_extract(val, pattern,0)=='') & (error_list.contains(flag_name)) , True) \
         .when((F.regexp_extract(val, pattern,0)!='') & (~error_list.contains(flag_name)) , True) \
         .otherwise(True)

# COMMAND ----------

def udf_DQ76_check_non_num(val, pattern, field, error_list):
  
  flagName = "DQ76_"+field
  
  # if value is alphabetics and it is flagged then True
  # if value is not purely alphabetic and it is not flagged then return False
  # if value is not null and not empty and it is flagged then return False
  # any other case True
  
  return F.when((F.regexp_extract(val,pattern,0)!= "") & (error_list.contains(flagName)),True) \
        .when((F.regexp_extract(val,pattern,0)!= "") & ~(error_list.contains(flagName)),True) \
		.when((F.regexp_extract(val,pattern,0) == "") & (~error_list.contains(flagName)),False) \
        .when(((~F.isnull(val)) & (val != "")) & (error_list.contains(flagName)),False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM80_validate(value,field,mod_list):
  
  flag = 'DM80_' + field
  
  # if the value is null & mod_list contains the flag then return True
  # if the value is null, and it is not flagged then return True
  # if value is NULL string and it is not flagged then return False {this condition essentially means, the modification step of load script failed}
  # for any other case return True, since value 
  
  return F.when((value == None) & mod_list.contains(flag),True) \
          .when((value == None) & (~mod_list.contains(flag)),True) \
          .when((F.upper(value) == 'NULL') & (~mod_list.contains(flag)),False) \
          .otherwise(True)



# COMMAND ----------

def udf_dq80_validation(val, min_val, error_list, field=None, max_val=None):
  if field is not None:
    flagName = concat(lit("DQ80"),lit("_"),field)
  else:
    flagName = concat(lit("DQ80"),lit("_"))

  if max_val is None:
    max_val = min_val
  
  return F.when(((F.isnull(val)) | (val == '') | (val == None)) & (error_list.contains(flagName)), False)\
       .when(((val < min_val) | (val > max_val)) & (~error_list.contains(flagName)), False) \
       .when((val >= min_val) & (val <= max_val) & (error_list.contains(flagName)), False) \
       .otherwise(True)

# COMMAND ----------

def udf_dq81_validation(val, valid_list, field, error_list):
  flagName = concat(lit("DQ81_"),field)
    
  return F.when((val.isin(valid_list)) & (error_list.contains(flagName)), False)\
        .when((~val.isin(valid_list)) & (~error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

def udf_dq82_validation(val, regex, field, error_list):
  flagName = concat(lit("DQ82_"),field)
    
  return F.when(((F.isnull(val)) | (val == '') | (val == None)) & (error_list.contains(flagName)), False)\
        .when((F.regexp_extract(val, regex, 0)!= "") & (error_list.contains(flagName)), False)\
        .when((F.regexp_extract(val, regex, 0)== "") & (~error_list.contains(flagName)), False) \
        .otherwise(True)

# COMMAND ----------

def udf_DM83_re_validate(val, valid_list, field, error_list):
  flagName = concat(lit("DM83_"),field)
  return F.when(((F.isnull(val)) | (val == '') | (val == None)) & (~error_list.contains(flagName)),False) \
    .when((~val.isin(valid_list)) & (~error_list.contains(flagName)),False) \
    .when((val.isin(valid_list)) & (error_list.contains(flagName)),False) \
    .otherwise(True)

# COMMAND ----------

def udf_dq85_validation(val, valid_list, field, error_list):
  flagName = concat(lit("DQ85_"),field)
    
  return F.when(((F.isnull(val)) | (val == '') | (val == None)) & (error_list.contains(flagName)), False)\
        .when((val.isin(valid_list)) & (error_list.contains(flagName)), False)\
        .when((~val.isin(valid_list)) & (~error_list.contains(flagName)), False) \
        .otherwise(True)


# COMMAND ----------

def udf_DQ90_validate(value, field, expected_length, error_list):
  
  flag = 'DQ90_' + field
  
  return F.when((value == expected_length) & ~error_list.contains(flag),False) \
          .when((value != expected_length) & error_list.contains(flag),False) \
          .when((value != expected_length) & ~error_list.contains(flag),True) \
          .when((value == expected_length) & error_list.contains(flag),True) \
          .otherwise(True)

# COMMAND ----------

def udf_DQ91_validate(value, field, expected_length, error_list):
  
  flag = 'DQ91_' + field
  
  return F.when((value == expected_length) & (~error_list.contains(flag)),True) \
          .when((value != expected_length) & (error_list.contains(flag)),True) \
          .when((value != expected_length) & (~error_list.contains(flag)),False) \
          .when((value == expected_length) & (error_list.contains(flag)),False) \
          .otherwise(True)

# COMMAND ----------

def udf_DQ92_validation(val1, val2, field1, field2, check_val,error_list):
  
  flag = 'DQ92_' + field1 + '_' + field2
  
  return F.when((val1 != check_val) & error_list.contains(flag),False) \
          .when(((val2 != "") & (val2 != None) & (val2 != "NULL") & (val2 != "null")) & error_list.contains(flag),False) \
          .when((val1 == check_val) & ((val2 == "") | (val2 == None) | (val2 == "NULL") | (val2 == "null")) & ~error_list.contains(flag),False) \
          .otherwise(True)
          
def udf_dq93_validation(val, regex, field, error_list):
  '''
  if val is None/null/empty string  or val matches the regex - No Flag
  Flag Otherwise
  '''
  flagName = F.concat(F.lit("DQ93_"),field)
  return F.when(((F.isnull(val)) | (val == '') | (val == None)) & (error_list.contains(flagName)), False) \
          .when(~val.rlike(regex) & (~error_list.contains(flagName)), False) \
          .when(val.rlike(regex) & (error_list.contains(flagName)), False) \
          .otherwise(True)