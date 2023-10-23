# Databricks notebook source
dbutils.widgets.removeAll() 

# COMMAND ----------

dbutils.widgets.text("environment","", "environment")   
environment = dbutils.widgets.get("environment")   

dbutils.widgets.text("instance","", "instance")   
instance = dbutils.widgets.get("instance")

dbutils.widgets.text("s3_dir","", "s3_dir")
s3_dir = dbutils.widgets.get("s3_dir")

dbutils.widgets.text("stg_dir", "", "stg_dir")
stg_dir = dbutils.widgets.get("stg_dir")

dbutils.widgets.text("delta_flags_write", "", "delta_flags_write")
delta_flags_write = dbutils.widgets.get("delta_flags_write")

dbutils.widgets.text("delta_intermediate_write","", "delta_intermediate_write")
delta_intermediate_write = dbutils.widgets.get("delta_intermediate_write")

dbutils.widgets.text("output_location","", "output_location")
output_location = dbutils.widgets.get("output_location")

dbutils.widgets.text("sender","", "sender")
sender = dbutils.widgets.get("sender")

dbutils.widgets.text("email","", "email")
email = dbutils.widgets.get("email")

dbutils.widgets.text("email_result","", "email_result")
email_result = dbutils.widgets.get("email_result")

dbutils.widgets.text("mapanet_master_table","", "mapanet_master_table")
mapanet_master_table = dbutils.widgets.get("mapanet_master_table")

dbutils.widgets.text("geonames_master_table","", "geonames_master_table")
geonames_master_table = dbutils.widgets.get("geonames_master_table")

dbutils.widgets.text("envt_dbuser","", "envt_dbuser")
envt_dbuser = dbutils.widgets.get("envt_dbuser")

dbutils.widgets.text("lot_id","", "lot_id")
lot_id = dbutils.widgets.get("lot_id")

dbutils.widgets.text("flags_archive_location","", "flags_archive_location")
flags_archive_location = dbutils.widgets.get("flags_archive_location")

dbutils.widgets.text("filter_date","", "filter_date")
filter_date = dbutils.widgets.get("filter_date")

dbutils.widgets.text("s3_file_retention_period","", "s3_file_retention_period")
s3_file_retention_period = dbutils.widgets.get("s3_file_retention_period")

dbutils.widgets.text("vacuum_file_retention_duration","", "vacuum_file_retention_duration")
vacuum_file_retention_duration = dbutils.widgets.get("vacuum_file_retention_duration")

dbutils.widgets.text("delta_log_retention_duration","", "delta_log_retention_duration")
delta_log_retention_duration = dbutils.widgets.get("delta_log_retention_duration")


# COMMAND ----------

# MAGIC %run ../libraries/ent_dq_module

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

############################################################################################################
###                Create the list of Dictionary Variables to be send on notebook exit                  ###
###########################################################################################################
from collections import OrderedDict
metric_dict = OrderedDict()
metric_dict["lot_id"] = lot_id

metric_dict["total_inc_count"] = 0

metric_dict["total_final_count"] = 0

metric_dict["total_good_count"] = 0

metric_dict["total_error_count"] = 0

metric_dict["STATUS"] = 'FAILURE'

metric_dict["STEP_ERROR"] = 'APP_START#NO_ERROR'

# COMMAND ----------

passedTests = []
failedTests = []
email_body = []
Underline = "----------------------------------------------------------------------------------------------------------------"
sep = '\n' + ("--------------------------------------------------------------------------------------") + '\n'
lookup_status = "FAILED"

# COMMAND ----------

##########################################################################################################
##    Read Lookup File                                                      ###
##########################################################################################################
try:
  STEP = 'Read the Look up Sheet.'
  df_look_up_data_raw = spark.read.format("com.databricks.spark.csv").option("header","true").option("sep", ",").load(s3_dir)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

# Defining expected column names
expected_lookup_cols = ['derived_country_code','geo_datasource','mapanet_language','ent_onboard_date']

# COMMAND ----------

## Taking backup of lookup sheet before doing trimming & upper case conversion
back_up_look_up_dir = stg_dir + 'backup_lookup_raw/'
write_as_delta(df_look_up_data_raw,path=back_up_look_up_dir,mode="overwrite",compression_type="snappy")

# COMMAND ----------

# Taking count of the lookup
initial_lookup_count = df_look_up_data_raw.count()
print(initial_lookup_count)

# COMMAND ----------

###########################################################################################################
###                   Check if column names are correct                                                 ###
###########################################################################################################
try:
  STEP = 'Check if column names are correctfrom lookup sheet not in expected column list'
  lookup_header_ok    = True
  body_text_lookup = "The lookup sheet has the incorrect columns names : " 
  
  # Fill in body text if columns are not good
  for col in df_look_up_data_raw.columns:
    if col not in expected_lookup_cols:
      lookup_header_ok  = False
      print(col)
      body_text_lookup = body_text_lookup + col + ','
       
  
  # reset body text if columns are good
  if lookup_header_ok:
    passedTests.append(Underline + "\n" + "All column names are correct")
  else:
    body_text = body_text_lookup + sep 
    failedTests.append(Underline + "\n" + body_text_lookup + "\n")
    print(failedTests)
    email_body.append("\n" + body_text_lookup)
 
 ##########################################################################################################
except Exception as e:
  print(e)

# COMMAND ----------

###########################################################################################################
###                   Check if columns are missing                                                   ###
###########################################################################################################
try:
  STEP = 'Check if expected columns not missing from lookup table'
  lookup_col_ok = True
  body_text_lookup = 'The lookup sheet has missing columns : ' 
  
  # Fill in body text if columns are missing
  for col in expected_lookup_cols:
    if col not in df_look_up_data_raw.columns:
      lookup_col_ok = False
      body_text_lookup = body_text_lookup + col + ','
      
  print(lookup_col_ok)
  
  # reset body text if columns are good
  if lookup_col_ok:
    passedTests.append(Underline + "\n" + "No missing columns")
  else:
    failedTests.append(Underline + "\n" + body_text_lookup)
    email_body.append( "\n" + body_text_lookup + "\n")
    
###########################################################################################################
except Exception as e:
  print (e)

# COMMAND ----------

## Trimming all the 4 cols
try:
  for col_name in expected_lookup_cols:
    df_look_up_data= df_look_up_data_raw.withColumn(col_name, F.trim(F.col(col_name)))
except Exception as e:
  failedTests.append(Underline + "\n" + "Trimming Failed")

# COMMAND ----------

## Converting df to upper case
try:
  column_list = ['derived_country_code','geo_datasource', 'mapanet_language']
  for col_name in column_list:
      df_look_up_data = df_look_up_data_raw.withColumn(col_name, F.upper(F.col(col_name)))
except Exception as e:
  failedTests.append(Underline + "\n" + "Converting to uppercase Failed")

# COMMAND ----------

## Checking for duplicate country codes
try:
  df_look_up_data.createOrReplaceTempView('lookup_table')
  df_count_lookup = spark.sql("SELECT derived_country_code, count(derived_country_code) as country_code_count FROM lookup_table GROUP BY derived_country_code HAVING country_code_count > 1")
  count = df_count_lookup.count()
  if count == 0:
    passedTests.append(Underline + "\n" + "No duplicate Country Code.")
  else:
    df = df_count_lookup.toPandas()
    msg = df.values.tolist()
    msg = str(msg).replace("['", "[").replace("',","-")
    failedTests.append(Underline + "\n" + "Duplicate Country Codes:" + msg)
    email_body.append("Duplicate country codes found in the lookup sheet : " + msg)
except Exception as e: 
  print(e)

# COMMAND ----------

## Check to see preferred geo datasource contains GEONAMES or MAPANET only
try:
  acceptable_list = ['MAPANET', 'GEONAMES']
  acceptable_list.sort() 
  print(acceptable_list)

  data_source_list = [row[0] for row in df_look_up_data.select('geo_datasource').distinct().collect()]
  data_source_list.sort()
  print(data_source_list)
  
  if data_source_list == acceptable_list:
    passedTests.append(Underline + "\n" + "Only GEONAMES & MAPANET sources present in geo_datasource column")
  else:
    failedTests.append(Underline + "\n" "Invalid Geo_datasource {}".format(data_source_list))
    email_body.append("\n" + "One or more invalid geo_datasource value found in the geo_datasource column. The list of values found in the columns are: {}".format(data_source_list) + "\n")

except Exception as e:
  print(e)

# COMMAND ----------

TotalTestsCount = len(passedTests) + len(failedTests)
passedCount = len(passedTests)
failedCount = len(failedTests)
passedValidations = ''
failedvalidations = ''

for testpassed in passedTests:
  passedValidations += "\n" + testpassed
for testpassed in failedTests:
  failedvalidations += "\n" + testpassed

# COMMAND ----------

TestOutputNotebook = "Enrichment Validation Test Output for LOOKUP Sheet"
Environment        = "Environment is    : " + environment 
Counts             = "Counts:"
TotalTestCount     = "Total Test Count  : " + str(TotalTestsCount)
passedTestsCount   = "Passed Test Count : " + str(len(passedTests))
failedTestsCount   = "Failed Test Count : " + str(len(failedTests))
PassedTestsText    = "\nPassed Tests:"
passedValidations  = passedValidations
FailedTestsText    = "\nFailed Tests:" 
failedvalidations  = failedvalidations

fullTestOutput = "\n".join([TestOutputNotebook,Underline,Environment,Underline,Counts,TotalTestCount,passedTestsCount,failedTestsCount,Underline,PassedTestsText,passedValidations,Underline,FailedTestsText,failedvalidations])

print(fullTestOutput)

# COMMAND ----------

if 'Failed Test Count : 0' in fullTestOutput:
  lookup_status = "PASSED"
  print("Lookup Validation Success")
else:
  lookup_status = "FAILED"
  print("Lookup Validation Failed")

# COMMAND ----------

###########################################################################################################
###  Handle if Lookup validation test cases fail                                                             ###
###########################################################################################################
try:
  STEP = "Handle if Lookup sheet validation fails"
  final_email_body = ''
  final_email_body  = "Hi Country Postal Ref data users," + "\n" +"We have found the following discrepencies in the look up sheet:" + "\n"+ "\n".join(email_body) + "\n" + "\n" + "Regards," + "\n" + "RCB Enterprise Team"
  
  if lookup_status == "FAILED": 
    STEP='Send email for failed test cases'
    load_status = "Lookup Validation Result"
    subject = '%s - %s - %s -%s'%(environment, instance, load_status, lookup_status)
    print (subject)
    status = SendEmail(final_email_body, sender, email_result, subject)
    print("Email Status: " + status)
  
    STEP='Metrics creation if Lookup sheet validation fails'
    
    metric_dict = OrderedDict()
    metric_dict["lot_id"] = lot_id
    metric_dict["total_lookup_count"] = initial_lookup_count
    metric_dict["total_final_count"] = 0
    metric_dict["STATUS"] = 'FAILURE' 
    metric_dict["Email Status"] = status
    metric_dict["STEP_ERROR"] = 'LOOKUP VALIDATIONS FAILED'
    metric_dict["body_text"] =  final_email_body
    
    metric_json = json.dumps(metric_dict)
    dbutils.notebook.exit(metric_json)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

##########################################################################################################
##                   Get GEONAMES data                                                                   ###
##########################################################################################################
try:
  STEP = "Get GEONAMES data"
  creds_str = dbutils.notebook.run("../libraries/credentials_for_enterprise", 600, {"envt_dbuser":envt_dbuser})
  creds = ast.literal_eval(creds_str)
  
  jdbc_url           = creds['rs_jdbc_url']
  iam                = creds['aws_iam']
  tempS3Dir          = creds['tmp_dir']
  
  df_geonames = sqlContext.read.format("com.databricks.spark.redshift").option("url",jdbc_url).option("tempdir", tempS3Dir).option("dbtable", geonames_master_table).option("aws_iam_role",iam).load()
  df_geonames.createOrReplaceTempView("geonames_table")
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

##########################################################################################################
##                   Get MAPANET data                                                                   ###
##########################################################################################################
try:
  STEP = "Get MAPANET data"
  df_mapanet = sqlContext.read.format("com.databricks.spark.redshift").option("url",jdbc_url).option("tempdir", tempS3Dir).option("dbtable", mapanet_master_table).option("aws_iam_role",iam).load()
  df_mapanet.createOrReplaceTempView("mapanet_table")
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

# Taking Count
try:
  STEP = 'Taking count'
  print('Count of mapanet df:' , df_mapanet.count())
  print('Count of geoname df:' , df_geonames.count())
  initial_count = df_mapanet.count() + df_geonames.count()

except Exception as e:
  exit_notebook(STEP,e)  

# COMMAND ----------

# ##################################### Writing data to staging location  #####################################################################
try:
  STEP = "Writing data to stg"
  geonames_stg_dir = stg_dir + 'geonames/'
  mapanet_stg_dir = stg_dir + 'mapanet/'
  look_up_stg_dir = stg_dir + 'look_up/'

  write_data_to_stage_location('geonames',df_geonames ,geonames_stg_dir,'delta','overwrite')
  write_data_to_stage_location('mapanet',df_mapanet,mapanet_stg_dir,'delta','overwrite')
  write_data_to_stage_location('lookup_data',df_look_up_data,look_up_stg_dir,'delta','overwrite')
  
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

#### Refining schema of Mapanate dataframe
try: 
  STEP = 'Keeping required cols and renaming'
  keep_cols_mapanet = ['countrya2', 'postalcode','locality', 'region1code', 'region1name', 'region2code', 'region2name', 'region3code', 'region3name', 'language']
  df_mapanet = df_mapanet.select(*keep_cols_mapanet)
  
  df_mapanet = df_mapanet.withColumnRenamed('countrya2','country_code')
  df_mapanet = df_mapanet.withColumnRenamed('postalcode','postal_code')
  df_mapanet = df_mapanet.withColumnRenamed('locality','place_name')
  df_mapanet = df_mapanet.withColumnRenamed('region1code','admin_code1')
  df_mapanet = df_mapanet.withColumnRenamed('region1name','admin_name1')
  df_mapanet = df_mapanet.withColumnRenamed('region2code','admin_code2')
  df_mapanet = df_mapanet.withColumnRenamed('region2name','admin_name2')
  df_mapanet = df_mapanet.withColumnRenamed('region3code','admin_code3')
  df_mapanet = df_mapanet.withColumnRenamed('region3name','admin_name3')
  df_mapanet = df_mapanet.withColumnRenamed('language','mapanet_language')
  
except Exception as e:  
  exit_notebook(STEP,e)

# COMMAND ----------

#### Refining schema of Geonames dataframe - Dropping latitude, longitude, accuracy cols
try: 
  STEP = 'Droping required cols'
  keep_cols_geonames = ['country_code','postal_code','place_name','admin_code1','admin_name1','admin_code2','admin_name2','admin_code3','admin_name3']
  df_geonames = df_geonames.select(*keep_cols_geonames)
except Exception as e:
      exit_notebook(STEP,e)

# COMMAND ----------

# Taking Count
try:
  STEP = 'Taking count'
  print('Initial count of mapanet df:' , df_mapanet.count())
  df_mapanet = df_mapanet.distinct()
  print('Final count of mapanet df:' , df_mapanet.count())
  
  print('Initial count of geonames df:' , df_geonames.count())
  df_geonames = df_geonames.distinct()
  print('Final count of geonames df:' , df_geonames.count())

#f.monotonic
except Exception as e:
  exit_notebook(STEP,e) 

# COMMAND ----------

## Filtering records from MAPANET dataset with look up table
try:
  df_look_up_data.createOrReplaceTempView("df_lookup_table")
  df_mapanet.createOrReplaceTempView("df_mapanet_table")
  df_mapanet_filtered = spark.sql("select m.*, l.geo_datasource from df_mapanet_table m INNER JOIN df_lookup_table l ON m.country_code = l.derived_country_code and l.mapanet_language = m.mapanet_language WHERE l.geo_datasource == 'MAPANET'")

except Exception as e:
      exit_notebook(STEP,e)

# COMMAND ----------

## Filtering records from GEONAMES dataset with look up table
try:
  df_look_up_data.createOrReplaceTempView("df_lookup_table")
  df_geonames.createOrReplaceTempView("df_geonames_table")
  df_geonames_filtered = spark.sql("select g.*, l.geo_datasource, l.mapanet_language from df_geonames_table g INNER JOIN df_lookup_table l ON g.country_code = l.derived_country_code WHERE l.geo_datasource == 'GEONAMES'")

except Exception as e:
      exit_notebook(STEP,e)

# COMMAND ----------

# Taking Count
try:
  STEP = 'Taking count'
  print('Count of mapanet df:' , df_mapanet_filtered.count())  
  print('Count of geonames df:' , df_geonames_filtered.count())
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

## Combining mapanet & geonames df
try:
  df_final = df_mapanet_filtered.unionByName(df_geonames_filtered)
  df_final= df_final.withColumn("insert_ts", current_timestamp())
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

# Taking Count
try:
  STEP = 'Taking count'
  print('Count of combined df:' , df_final.count())
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ** Applying DQs:**

# COMMAND ----------

# Combining admin columns to check if they are null or not
try:
  df_final = df_final.withColumn('temporary_composite_key',F.concat(F.coalesce(F.col('admin_name1'),F.lit("")),F.coalesce(F.col('admin_code1'),F.lit("")),F.coalesce(F.col('admin_name2'),F.lit("")),F.coalesce(F.col('admin_code2'),F.lit("")),F.coalesce(F.col('admin_name3'),F.lit("")),F.coalesce(F.col('admin_code3'),F.lit(""))))
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

## Applying DQ70 for checking NULLS in all the 6 admin columns and in composite key
try:
  col_list = ['country_code','postal_code','place_name','temporary_composite_key']
  for col_name in col_list:
    df_final = df_final.withColumn('check_dq70_'+ col_name, dq70_nullcheck(F.col(col_name), lit(col_name)))
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###               Concatenate the  DQ list to single column                                             ###
###########################################################################################################
try:
  STEP = 'Concatenate the DQ columns to single column before applying DQ9.'
  #Append errors to error_list and create comma delimited field of errors
  error_cols = [F.col(i) for i in df_final.columns if 'check_dq' in i]
  df_cpr_final= df_final.withColumn("check_dq_all", udf_concat_list(*error_cols))
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###               Apply the DQ and DM on the Dataframe                                                  ###
###              DQ Applied : DQ9 (Marking Duplicates)                                                  ###
###########################################################################################################
try:
  PROCESS_STEP = 'Applying DQ9 on the Dataframe for country_code + postal_code + place_name + admin_name1.'
# Step DQ9. - Flag rows to establish a single record for each country_code + postal_code + place_name + admin_name1 combination.
# If more than one source has a unique key duplicate, rank and take the first one

  partition_list = [df_cpr_final['country_code'], df_cpr_final['postal_code'], df_cpr_final['place_name']]
  
  sort_list = [df_cpr_final["check_dq_all"].asc(), df_cpr_final['admin_code1'].asc(), df_cpr_final['admin_name1'].asc(), df_cpr_final['admin_code2'].asc(), df_cpr_final['admin_name2'].asc(),df_cpr_final['admin_code3'].asc(), df_cpr_final['admin_name3'].asc()]

  # get the ranks for each record in partition list
  rankDF = step_dq9_rank(df_cpr_final, partition_list, sort_list)
  # Mark the records where Rank !=1 as DQ9
  # Composite Key here is country_code + posta_code + place_name 
  df_cpr_final = rankDF.withColumn("check_dq9_compositekey", udf_step_dq9(rankDF.ranked)).drop("ranked").drop("check_dq_all")

###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###         Final processing on the dataframes to make it ready to be written to S3                     ###
###         Activity 1: Append dms to mod_list and create comma delimited field of dm's                 ###
###         Activity 2: Create the partition column based on error code                                 ###
###         Activity 3: Select Required Columns                                                         ###
###         Activity 4: Sort the dataframes, useful for redshift loading                                ###
###########################################################################################################
try:
  # create error_list column by concatenating the columns which has names like check_dq (created earlier)
  STEP = 'Create the Column error_list.'
  df_cpr_final = create_error_mod_list(df_cpr_final,'check_dq','error_list')
  
  #Append dms to mod_list and create comma delimited field of dm's
  STEP = 'Create the Column mod_list.'
  df_cpr_final = create_error_mod_list(df_cpr_final,'check_dm','mod_list')
  # create error_code partition column
  df_cpr_final = df_cpr_final.withColumn("error_code", step_error_code(F.col("error_list")))
  df_cpr_final.createOrReplaceTempView('tbl')
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select check_dq70_country_code, check_dq70_postal_code, check_dq70_place_name,check_dq70_temporary_composite_key, check_dq9_compositekey, count(*) from tbl group by 1,2,3,4,5

# COMMAND ----------

##########################################################################################################################################
#############################   Sending mail: when all admin codes are NULL  ########################################
##########################################################################################################################################
try:
  STEP = "Sending email for NULL admins cols"
  c1 = spark.sql("select country_code, count(*) as record_count from tbl where admin_code1 is NULL and admin_code2 is NULL and admin_code3 is NULL group by 1")
  null_count = c1.count()
  if null_count > 0:    
    body_text = "Hi Country Postal Ref Data Users,\n" + "\n Please find attached the list of country codes having all three Admin code columns as NULL.\n" + "\n"+ "Regards," + "\n" + "Enterprise Team"
    attachment = c1.toPandas().to_csv(index=False)
    subject = "Email Alert : All admin codes are NULL for some countries"
    status = SendEmailWithAttachment(attachment, 'null_country_code.csv', body_text, sender, email_result, subject)
    print("Email Status: " + status)
        
except Exception as e:
  exit_notebook(STEP,e) 

# COMMAND ----------

###########################################################################################################
## Select only relevant columns                                                               #############
###########################################################################################################
try:
  STEP = 'Drop the extra Columns.'
  df_drop_cols = ['check_dq70_country_code',
'check_dq70_postal_code',
'check_dq70_place_name',
'check_dq70_temporary_composite_key',
'check_dq9_compositekey',
'temporary_composite_key']
  df_cpr_final = df_cpr_final.drop(*df_drop_cols)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###         Write the Dataframe to Flags location in S3                                                 ###
###########################################################################################################
try:
  # Write the Dataframe to the Flags Location
  enrich_total_final_count = df_cpr_final.count()
  print(enrich_total_final_count)
  STEP = 'Write the data to Flags location. '
  write_as_delta(df_cpr_final,path=delta_flags_write,mode="overwrite",compression_type="snappy")
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                             Read from flags location                                                ###
###########################################################################################################
try:
  # Read the existing transformed dataframe so that spark doesn't recompute the transformations again
  # Reading the delta from flags location
  STEP = 'Getting Good & Error Record Counts.'
  total_good_count = df_cpr_final.where(F.col('error_code') == 0).count()
  total_error_count = df_cpr_final.where(F.col('error_code') == 1).count()
  print("good_count",  total_good_count )
  print("error_count",  total_error_count)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                         Filtering only good records                                                  ###
###########################################################################################################
try:
  # Read the existing transformed dataframe so that spark doesn't recompute the transformations again
  # Reading the delta from flags location
  STEP = 'Dropping columns for final the delta final location'
  keep_cols = ['country_code', 'postal_code', 'place_name', 'admin_code1', 'admin_name1', 'admin_code2', 'admin_name2','admin_code3','admin_name3','mapanet_language', 'geo_datasource','insert_ts']
  
  df_cpr_final = df_cpr_final.where(F.col('error_code') == 0)
  df_cpr_final = df_cpr_final.select(*keep_cols)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                Write the Dataframe to Intermediate location in S3                                                 
###########################################################################################################
try:
  STEP = "Write the data to Intermediate write"
  df_cpr_final.write.mode("overwrite").option("compression", "snappy").format("delta").save(delta_intermediate_write)
  df_cpr_final.createOrReplaceTempView("cpr_table")

except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

##########################################################################################################
# # ###        Taking datewise backup of Flags location in S3    ###########################################################################################################
try:
  import datetime
  STEP = 'Writing the data to Flags Archive location.'
  current_date = '{:%Y%m%d}'.format(datetime.datetime.now())
  df_cpr_final.write.mode("overwrite").option("compression", "snappy").format("delta").save(flags_archive_location + current_date)
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

##########################################################################################################
###        Cleaning up flag archive location files older than 90 days                                  ###
##########################################################################################################
import datetime
try:
  STEP = "Cleaning up flag archive location files older than 90 days"
  delete_s3_files_by_age(s3_path = flags_archive_location , no_of_days = s3_file_retention_period)
except Exception as e:
  exit_notebook(STEP,e)
########################################################################################################## 

# COMMAND ----------

# ###########################################################################################################
# ###          SET DELETE FILE RETENTION PERIOD IF NOT SET TO DESIRED INTERVAL                            ###
# ###########################################################################################################
# try:
#   STEP = "CHECK FILE RETENTION DURATION & SET TO DESIRED DURATION IF NOT ALREADY SET"
#   delta_list = [delta_flags_write]
  
#   for table in delta_list:
#     current_file_retention_interval = spark.sql(f"SHOW TBLPROPERTIES delta.`{table}` (delta.deletedFileRetentionDuration)").collect()[0][1]
#     if current_file_retention_interval != vacuum_file_retention_duration:
#       spark.sql(f"ALTER TABLE delta.`{table}` SET TBLPROPERTIES('delta.deletedFileRetentionDuration'= '{vacuum_file_retention_duration}')")
# except Exception as e:
#   exit_notebook(STEP,e)

# COMMAND ----------

# ###########################################################################################################
# ###          SET DELETE LOG RETENTION PERIOD IF NOT SET TO DESIRED INTERVAL                            ###
# ###########################################################################################################
# try:
#   STEP = "CHECK LOG RETENTION DURATION & SET TO DESIRED DURATION IF NOT ALREADY SET"
#   for table in delta_list:
#     current_delta_log_retention_interval = spark.sql(f"SHOW TBLPROPERTIES delta.`{table}` (delta.logRetentionDuration)").collect()[0][1]
#     if current_delta_log_retention_interval != delta_log_retention_duration:
#       spark.sql(f"ALTER TABLE delta.`{table}` SET TBLPROPERTIES('delta.logRetentionDuration'= '{delta_log_retention_duration}')")
# except Exception as e:
#   exit_notebook(STEP,e)

# COMMAND ----------

#########################################################################################################
#         Write the LOOKUP Dataframe to Intermediate location in S3 in delta Format                        ###
#########################################################################################################
try:
  STEP = 'Write the LOOKUP data to Final location.'

  delta_final_lookup = 's3://ref-data-itg/country_postal_ref/'+'country_postal_ref_lookup'
  df_look_up_data.write.mode("overwrite").option("compression", "snappy").format("delta").save(delta_final_lookup)
###########################################################################################################
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###         Write the Dataframe to Final location in S3 for validation by stakeholders in CSV             ###
###########################################################################################################
## One of the main reason of doing this duplicate check here is: Floyd & Austin may revise the de duplication logic in the future, as they have found discrepencies in it. 
## So, we are keeping an extra check over here for duplicates. In case there are duplicates, code will break here and we would have to inform our stake holders. 

try:
  STEP = "Extra Check of Duplicates"
  df_cnt = spark.sql("select country_code, postal_code, place_name, count(*) as cnt from cpr_table group by 1,2,3 having cnt > 1")
  count = df_cnt.count()
  if count == 0:
    STEP = 'Write the data to Output location.'
    output_location_final = output_location + 'country_postal_ref/'
    temp_csv_location = output_location_final +'temp_folder'
    final_csv_file_location = output_location_final + 'country_postal_ref_dataset.csv'
    df_cpr_final.repartition(1).write.csv(path=temp_csv_location, mode="overwrite", header="true")
    file = dbutils.fs.ls(temp_csv_location)[-1].path
    dbutils.fs.cp(file, final_csv_file_location)
    dbutils.fs.rm(temp_csv_location, recurse=True)

  else:
    STEP = 'Handle incase of duplicate records & Metrics creation for the same '
    total_inc_count = df_cpr_final.count()
    metric_dict = OrderedDict()
    metric_dict["lot_id"] = lot_id
    metric_dict["total_inc_count"] = total_inc_count
    metric_dict["total_final_count"] = 0
    metric_dict["total_duplicate_records"] = count
    metric_dict["STATUS"] = 'SUCCESS'
    metric_dict["STEP_ERROR"] = 'Duplicate Records found even after DQ9' 
    metric_json = json.dumps(metric_dict)
    dbutils.notebook.exit(metric_json)
   ##########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###         Write the LOOKUP Dataframe to Final location in S3 in CSV                                   ###
###########################################################################################################
try:
  # Write the Dataframe to the Final Location
  STEP = 'Write the LOOKUP data to Lookup Output location.'
  df_lookup = df_look_up_data.toPandas()
  df_lookup.to_csv(output_location + 'country_postal_ref_lookup/lookup.csv', index = False)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###  Metrics for Logging the Stats and creating the Dictionary to be send as output of run              ###
###########################################################################################################
try:
  STEP = 'Get the Counts for the Metrics.'
  total_inc_count = initial_count
  
  metric_dict = OrderedDict()
  metric_dict["lot_id"] = lot_id
  
  STEP = 'Create the metrics.'
  metric_dict["total_inc_count"] = total_inc_count
  metric_dict["total_final_count"] = enrich_total_final_count
  metric_dict["total_good_count"] = total_good_count
  metric_dict["total_error_count"] = total_error_count
  
  metric_dict["STATUS"] = 'SUCCESS'

  metric_dict["STEP_ERROR"] = 'APP_END#NO_ERROR'
  
  metric_json = json.dumps(metric_dict)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

dbutils.notebook.exit(metric_json)

# COMMAND ----------

