# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("environment", "itg")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("instance", "country_postal_ref")
instance =dbutils.widgets.get("instance")

dbutils.widgets.text("filter_date","", "filter_date")
filter_date = dbutils.widgets.get("filter_date")

# COMMAND ----------

# MAGIC %run ./source_key_constants

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

workflow_start_time = time.time()	
time_now=datetime.datetime.now()
start_time = '{:%Y-%m-%d %H:%M:%S.%f}'.format(time_now)[:-4]	
now = "{:%Y-%m-%dT%H:%M:%S.%-S}".format(time_now)	
print (now)

# COMMAND ----------

workflow_step = 'workflow params started'
try:
  workflow_configuration = dbutils.notebook.run("./workflow_params", 600, {"environment":environment})
except Exception as e:
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess_with_metrics('Failed to run workflow params: {0}'.format(e))

# COMMAND ----------

#create individual widgets
pairs = parse_input_widgets(workflow_configuration)
for x in pairs:
  exec("%s = '%s'" % (x[0],x[1]))
#assigns instance to whatever source is running
print ("\n\nInstance".ljust(27) + ": " + instance)
#set more workflow constants
set_constants()

# COMMAND ----------

####################################################################################
### Setting start_time, workflow_start_time & now for the ingestion pipeline ###
####################################################################################
import datetime
workflow_step = "Setting start_time, workflow_start_time & now(format used in splunk) for the ingestion pipeline"
current_time = datetime.datetime.now()
workflow_start_time = current_time.timestamp()
start_time = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
now = current_time.strftime("%Y-%m-%dT%H:%M:%S.%-S")

# COMMAND ----------

workflow_step = "Calculating Filter date"
if not filter_date: 
  filter_date = time_now.strftime("%Y-%m-%d")
print(filter_date)

# COMMAND ----------

###########################################################################################################
###             Uploading Metrics json to splunk                                                       ###
###########################################################################################################
workflow_step = 'Uploading initial metrics to splunk'
metadata = {}
source_type = prod_switch
if source_type == 'inklaser':
  source_type = 'MIXED'
else :
  source_type = source_type.upper()
metadata["app_type"] = "enterprise_downstream"
metadata["app"] = source_key.get(instance).get('app').replace("_","-")
metadata["source_type"] = source_type
metadata["environment"] = environment
try:
  metricsJsonDist = splunk_integration(workflow_step,now,environment, instance, 'init')
  dataos_splunk.send_to_splunk(metadata, metricsJsonDist)
###########################################################################################################
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)  
  killCurrentProcess_with_metrics('Failed Metrics upload '+e)
###########################################################################################################

# COMMAND ----------

###########################################################################################################
###    Check if stakeholders have put lookup sheet in S3 for processing               ###
###########################################################################################################

def check_if_file_present(path):
  db_ret = dbutils.fs.ls(path)
  print(db_ret)
  return(db_ret)
  
try: 
  workflow_step = "Checking for looksheet in S3 directory"
  lookup_ip_path = s3_dir
  lookup_exist = False
  db_ret_lookup = check_if_file_present(lookup_ip_path)
  
  # Doing this to ensure that inside the s3 location, at all times we should have archive folder & lookup sheet only.
  # There is a possibilty that our stakeholder can put multiple lookup files in the s3 location by mistake. In such cases, we shouldn't be reading files.
  if len(db_ret_lookup) == 2 and [i.name for i in dbutils.fs.ls(lookup_ip_path) if i.name.endswith('.csv')]:
    lookup_exist = True
  else:
    body_text = 'Hi Country Postal Ref Data Users, \n' + '\n'+ 'Discrepancies found in the input lookup sheet S3 location : {}'.format(lookup_ip_path) + '\n' + '\n Regards,' + '\n' + 'Enterprise Team'
   
  print (lookup_exist)
  if lookup_exist == False:
    msg = 'Halted as discrepancies found in lookup file S3 location '
    subject = '%s - %s - %s - Country Postal Ref'%(environment, instance, msg)
    status = SendEmail(body_text, sender, email_result, subject)               
    workflow_step = "Handle when input lookup location has discrepancies"
    dataos_splunk.send_to_splunk(metadata, splunk_integration(msg,now,environment, instance))
    killCurrentProcess_with_metrics(msg)
###########################################################################################################
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess_with_metrics(e)

# COMMAND ----------

workflow_step = 'data Load started'
if 'load' in switch and lookup_exist == True:
  try:
      load_start = time.time()        
      load_nb = dbutils.notebook.run(
        LoadPath,
        36000,
        arguments = {"s3_dir"                   : s3_dir,
                    "instance"                 : instance,
					          "environment"              : environment,
                    "delta_flags_write"        : delta_flags_write,
                    "delta_intermediate_write" : delta_intermediate_write,
                    "flags_archive_location"   : flags_archive_location,
                    "output_location"          : output_location,
					          "stg_dir"                  : stg_dir,
                    "sender"                   : sender,
                    "email"                    : email,
                    "email_result"             : email_result,
                    "mapanet_master_table"     : mapanet_master_table,
                    "geonames_master_table"    : geonames_master_table,
                    "envt_dbuser"              : envt_dbuser.split(",")[0],
                    "lot_id"                  : now,
                    "s3_file_retention_period" : source_key.get(instance).get('s3_file_retention_period'),
                    "vacuum_file_retention_duration" : source_key.get(instance).get('vacuum_file_retention_duration'),
                    "delta_log_retention_duration" : source_key.get(instance).get('delta_log_retention_duration')
                    })
      
      load_end  = time.time()
      load_diff = load_end - load_start
      load_runtime = round(load_diff/ 60.0, 2)
  except Exception as e:
    dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
    exception_step(workflow_step,e,instance,environment,now)
    workflow_step = 'Failed app data load'
    killCurrentProcess_with_metrics(workflow_step  + ': {0}'.format(e))
  ##########################################################################################################
  else:
    loadMetricJson = json.loads(load_nb)    
    # parsing status & lot_id
    app_load_status = loadMetricJson["STATUS"]
    lot_id = loadMetricJson["lot_id"]    
    subject = '{} - {} - Enterprise Data Load ran {}'.format(environment, instance, app_load_status)
    status = SendEmail(load_nb, sender, receivers, subject)
    
    if(app_load_status == "SUCCESS" and lot_id != 0):
      print (lot_id)
      try:
          load_status = "PASSED"
          workflow_step = 'data load success'
      except Exception as e:
        workflow_step = 'Failed app data load ' 
        print (workflow_step)
        lot_id = "SOMETHING WENT WRONG"
        dataos_splunk.send_to_splunk(metadata, splunk_integration(loadMetricJson['STEP_ERROR'],now,environment, instance))
        killCurrentProcess_with_metrics(workflow_step  + ': {}'.format(e))
    else:      
      print(app_load_status)
      print(lot_id)      
      load_status = "FAILED"
      workflow_step = 'Failed data load at app_load_status & lot_id check'
      dataos_splunk.send_to_splunk(metadata, splunk_integration(loadMetricJson['STEP_ERROR'],now,environment, instance))
      killCurrentProcess_with_metrics(workflow_step)
  ###########################################################################################################
else:
  workflow_step = "Failed to call data load"
  print('Load Skipped')
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  ###########################################################################################################

# COMMAND ----------

###########################################################################################################
###                                   File Size Calculator                                              ###
###########################################################################################################
workflow_step = 'File size calculator started'
CalculatorPath = source_key.get(instance).get("CalculatorPath")
if load_status == 'PASSED':
  try:    
    size_calculator_start = time.time() 
    size_calculator_nb = dbutils.notebook.run(
      CalculatorPath,
      36000,
          arguments = {"s3_dir"                   : s3_dir,
                     "delta_flags_write"          : delta_flags_write,
                     "delta_intermediate_write"   : delta_intermediate_write,
                     "environment"                : environment
                    })

    size_calculator_end  = time.time()
    size_calculator_runtime = round((size_calculator_end - size_calculator_start) / 60.0, 2)
    size_calculator_json_metrics = json.loads(size_calculator_nb)

    if 'SUCCESS' in size_calculator_nb: 
      size_calculator_status = "PASSED"    
      print ("Job Status: SUCCESS")
      print (size_calculator_json_metrics)
      workflow_step = 'Size calculator passed'
    else:
      size_calculator_status = "FAIL"
      print ("Job Status: {}".format(size_calculator_status))
      workflow_step = 'Size calculator failed'
      dataos_splunk.send_to_splunk(metadata, splunk_integration(size_calculator_json_metrics['STEP_ERROR'],now, environment, instance))
    subject = '{} - {} - Enrich Size Calculator  {}'.format(environment, instance, size_calculator_status)
    status = SendEmail(size_calculator_nb, sender, receivers, subject )
##########################################################################################################
  except Exception as e:
    dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
    exception_step(workflow_step,e,instance,environment,now)
    killCurrentProcess_with_metrics('Failed File size calculator : {0}'.format(e))
##########################################################################################################

# COMMAND ----------

###########################################################################################################
###                             Table Creation Started                       ###
###########################################################################################################

workflow_step = "Table Creation Started"
RedshiftCreateTablePath = source_key.get(instance).get("RedshiftCreateTablePath")
if 'RS_table_creation' in switch and load_status=="PASSED":
  try:
    table_creation_start_time = time.time()
    table_creation_nb = dbutils.notebook.run(
      RedshiftCreateTablePath,
      36000,
      arguments = {"envt_dbuser"        : envt_dbuser,
                   "RedshiftFinalTable" : dw_source_name
                  })

    table_creation_end_time = time.time()
    table_creation_runtime = round((table_creation_end_time - table_creation_start_time) / 60.0, 2)
    subject = '%s - %s - Redshift Table Creation Script ran successfully'%(environment, instance)
    status = SendEmail(table_creation_nb, sender, receivers, subject)
    table_creation_json_metrics = json.loads(str(json.dumps(table_creation_nb)))
    if "SUCCESS" in table_creation_nb:
      print ("Job Status: SUCCESS")
      table_creation_status = "PASSED"
      workflow_step = 'Table Creation Completed'
    else:
      table_creation_status = "FAILED"
      workflow_step = 'Table creation failed'
      dataos_splunk.send_to_splunk(metadata, splunk_integration(table_creation_json_metrics,now, environment, instance))
#########################################################################################################
  except Exception as e:
    dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
    exception_step(workflow_step,e,instance,environment,now)
    killCurrentProcess_with_metrics('Failed to connect to Redshift '+e)

else:
  workflow_step = "unable to call create table"
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  killCurrentProcess_with_metrics('Failed to call redshift create table')
##########################################################################################################
#########################################################################################################
# dw_source_name

# COMMAND ----------

###########################################################################################################
### This step creates widget for DW load in case of multiple tables to be loaded                        ###
###########################################################################################################
try:
  workflow_step = 'Handle multi files to load to DW tables'
  alias_list = ['']
  tbl_list = dw_source_name.split(",")
  
  delta_inter_list = [delta_intermediate_write]
  delta_final_list = [delta_final_write]

  DeltaIntermediatePath = handle_multi_file(alias_list,delta_inter_list)
  DeltaFinalPath =  handle_multi_file(alias_list,delta_final_list)
  RedshiftFinalTable = handle_multi_file(alias_list,tbl_list)
 
  print (RedshiftFinalTable)
  print(DeltaIntermediatePath)
  print (DeltaFinalPath)
###########################################################################################################
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess_with_metrics('Failed handle multi file')
###########################################################################################################
 

# COMMAND ----------

if 'BAT' in switch and load_status == 'PASSED':
  try:
    workflow_step = 'BAT started'
    BAT_start = time.time()
    BAT_nb = dbutils.notebook.run(
      BATPath,
      36000,
      arguments = {"environment"              : environment,
                   "email"                    : email,
                   "delta_flags_write"        : delta_flags_write,
                   "delta_intermediate_write" : delta_intermediate_write,
                   "stg_dir"                  : stg_dir,
                   "instance"                 : instance,
                   "source_data_name"         : dw_source_name.split(".")[1],
                   "output_location"          : output_location,
                   "required_dq_checks"       : source_key.get(instance).get('required_dq_checks')
                  })
    BAT_end = time.time()

    BAT_runtime = round((BAT_end - BAT_start) / 60.0, 2)
    subject = '%s - %s - BAT ran successfully'%(environment, instance)
    status = SendEmail(BAT_nb, sender, receivers, subject )
    workflow_step = 'BAT completed'
    BAT_nb_metrics=json.loads(str(json.dumps(BAT_nb)))
  ###########################################################################################################
  except Exception as e:    
    dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance)) 
    exception_step(workflow_step,e,instance,environment,now)
    killCurrentProcess_with_metrics('Failed BAT Test : {0}'.format(e))
  ###########################################################################################################
  else:
    if BAT_nb:
      print (BAT_nb)
      if 'Failed Test Count : 0' in BAT_nb:
        BAT_status = "PASSED"
        workflow_step = 'BAT success' 
      else:
        BAT_status = "FAILED"
        workflow_step = 'BAT failed'
        dataos_splunk.send_to_splunk(metadata, splunk_integration(BAT_nb_metrics,now,environment, instance))
  ###########################################################################################################
else:
  workflow_step = "Failed to call BAT script"
  print('BAT Test Skipped!')
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  ###########################################################################################################    

# COMMAND ----------

try:
  if 'dq_test' in switch and load_status == 'PASSED':
    workflow_step = 'DQ test started'
    dq_test_start = time.time()
    dq_test_nb = dbutils.notebook.run(
      DQTestPath,
      36000,
      arguments = {"email"                    : email,
                   "environment"              : environment,
                   "delta_flags_write"        : delta_flags_write,
                   "dw_source_name"           : dw_source_name.split(".")[1]
                  })
    dq_test_end = time.time()
    DQ_test_runtime = round((dq_test_end - dq_test_start) / 60.0, 2)
    loadMetricJson= json.loads(dq_test_nb)
    dq_test_msg = loadMetricJson['FINALRESULT']
    subject = '%s - %s - DQ Test ran successfully'%(environment, instance)
    status = SendEmail(dq_test_msg, sender, receivers, subject)
    workflow_step = 'DQ test completed'
  else:
    print('Skipped!')
###########################################################################################################
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess('Failed DQ Test')
###########################################################################################################


# COMMAND ----------

if dq_test_nb:
  if 'Failed Test Count : 0' in dq_test_nb:
    workflow_step = 'DQ test passed'
    DQ_test_status = "PASSED"
  else:
    DQ_test_status = "FAILED"
    workflow_step = 'DQ test failed'
    dataos_splunk.send_to_splunk(metadata, splunk_integration(loadMetricJson,now,environment, instance))
print(dq_test_msg)

# COMMAND ----------

if 'dq_summary' in switch and load_status == 'PASSED':
   # dq test fail check
  workflow_step = 'DQ Summary started'
  dq_summary_start = time.time()
  dq_summary_nb = dbutils.notebook.run(
  DQSummaryPath,
  36000,
  arguments = {"environment":environment,
               "email":email,
               "delta_flags_write":delta_flags_write,
               "required_dq_checks" :source_key.get(instance).get('required_dq_checks'),
               "file_list" : '',
               "app" : source_key.get(instance).get('app')
               })

# COMMAND ----------

try:
  if 'dq_summary' in switch and load_status == 'PASSED': # dq test fail check
    workflow_step = 'DQ Summary started'
    dq_summary_start = time.time()
    dq_summary_nb = dbutils.notebook.run(
      DQSummaryPath,
      36000,
      arguments = {"environment":environment,
                   "email":email,
                   "delta_flags_write":delta_flags_write,
                   "required_dq_checks" :source_key.get(instance).get('required_dq_checks'),
                   "file_list" : '',
                   "app" : source_key.get(instance).get('app')
                  })
    dq_summary_end = time.time()

    DQ_summary_runtime = round((dq_summary_end - dq_summary_start) / 60.0, 2)
    loadMetricJson= json.loads(dq_summary_nb)
    load_dq_status = loadMetricJson["STATUS"]
    subject = '%s - %s - %s - DQ Summary Run Status'%(environment, instance, load_dq_status)
    dq_msg = loadMetricJson['FINAL_OUTPUT']
    status = SendEmail(dq_msg, sender, receivers, subject )
    workflow_step = 'DQ Summary completed'
###########################################################################################################    
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess_with_metrics('Failed DQ Summary')
###########################################################################################################

# COMMAND ----------

if 'dq_summary' in switch:
  if 'PASSED' in dq_summary_nb: 
    DQ_summary_status = "PASSED"
    #change app field to using -
    dq_summary_json_metrics = json.loads(dq_summary_nb.split('\035')[0])
    dq_summary_json_metrics["job_name"] = dq_summary_json_metrics.get("job_name").replace("_","-")
    
    for k,v in dq_summary_json_metrics.items():
      if "percent" in k:
        dq_summary_json_metrics[k] = v/100
    workflow_step = 'DQ Summary passed'
  else:
    DQ_summary_status = "FAIL"
    workflow_step = 'DQ Summary failed'
    dataos_splunk.send_to_splunk(metadata, splunk_integration(loadMetricJson,now,environment, instance))
print (json.loads(dq_summary_nb)['FINAL_OUTPUT'])

# COMMAND ----------

prod_load_runtime = "N/A"
RS_load_flag = "true"
try:
  workflow_step = 'PROD load started'  
  if  load_status != "PASSED":
    killCurrentProcess_with_metrics('Failed test(s), exiting before load to prod')
  prod_load_start = time.time()
  prod_load_nb = dbutils.notebook.run(
    ProdLoadPath,
    36000,
    arguments = {"DeltaIntermediatePath": DeltaIntermediatePath,
                "DeltaFinalPath"       : DeltaFinalPath,
                "RedshiftFinalTable"   : RedshiftFinalTable,
                "prod_switch"          : prod_switch,
                "envt_dbuser"          : envt_dbuser,
                "RS_load_flag"         : RS_load_flag,
	              "folder_name"          : "country_postal_ref",
	              "unity_catalog_info"   : f"team_enterprise_{environment.lower()}.ref_enrich"
                })
  prod_load_end = time.time()

  prod_load_runtime = round((prod_load_end - prod_load_start) / 60.0, 2)

  prod_load_MetricJson = json.loads(prod_load_nb)
  prod_load_status = prod_load_MetricJson["STATUS"]
  print (prod_load_status)
  print (prod_load_nb)
  prod_load_mesg = prod_load_MetricJson["MESSAGE"]
  if "PARTIAL_SUCCESS" in prod_load_status:
    print ("Job Status: PARTIAL_SUCCESS")
  elif "SUCCESS" in prod_load_status:
    print ("Job Status: SUCCESS")
  else:
    print ("Job Status: FAILED")
    dataos_splunk.send_to_splunk(metadata, splunk_integration(prod_load_MetricJson['STEP_ERROR'],now,environment, instance))
    killCurrentProcess_with_metrics('Failed DW load')

  subject = '%s - %s - Prod Load ran successfully'%(environment, instance)
  ProdloadMetricJson = json.loads(prod_load_nb)
  prod_load_email_msg = ProdloadMetricJson['MESSAGE']
  status = SendEmail(prod_load_email_msg, sender, receivers, subject)

  prod_load_status = "PASSED"
  workflow_step = 'PROD load completed'
    
   ##########################################################################################################
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess_with_metrics('Failed PROD RS Load')
  prod_load_status = "FAILED"
###########################################################################################################

# COMMAND ----------

if 'RS_test' in switch and (environment.lower() == "prod" or environment.lower() == "itg" or environment.lower() == "dev") and prod_load_status == 'PASSED':
  try:
    workflow_step = 'RS test started'
    RS_test_start = time.time()
    RS_test_nb = dbutils.notebook.run(
      "../test/ValidateRedshiftCount_03",
      36000,
      arguments = {  "environment"        : environment,
                     "DeltaFinalPath"     : DeltaFinalPath,
                     "RedshiftFinalTable" : RedshiftFinalTable,
                     "lot_id"             : lot_id,
                     "envt_dbuser"        : envt_dbuser
                  })
    RS_test_end = time.time()
    rs_check_metrics = json.loads(RS_test_nb)
    if "SUCCESS" in RS_test_nb:
      redshift_validation_status = "PASSED"
      workflow_step = 'RS test passed'
      print ("Job Status: SUCCESS")
    else:
      redshift_validation_status = "FAILED"
      workflow_step = 'RS test failed'
      print ("Job Status: FAILED")
      dataos_splunk.send_to_splunk(metadata, splunk_integration(rs_check_metrics['STEP_ERROR'],now,environment, instance))
      killCurrentProcess_with_metrics('Failed Redshift Validation')

    RS_test_runtime = round((RS_test_end - RS_test_start) / 60.0, 2)
    subject = '%s - %s - Redshift Validation ran successfully'%(environment, instance)
    rs_check_metrics = json.loads(RS_test_nb)
    rs_check_email_msg = rs_check_metrics['MESSAGE']
    status = SendEmail(rs_check_email_msg, sender, receivers, subject )
    workflow_step = 'RS test completed' 
  ###########################################################################################################
  except Exception as e:
    dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now, environment, instance))
    exception_step(workflow_step,e,instance,environment,now)
    killCurrentProcess_with_metrics('Failed RS Validation')
  ###########################################################################################################
else:
  step = 'rs validation skipped'
  print(step)


# COMMAND ----------

try:
  workflow_step = "Generating Final Email"
  heading = "Final Country Postal Data & Lookup Sheet Data Generated"
  subject = '%s - %s - %s'%(environment, instance, heading)
  Errormsg="Output files not generated properly, final email failure"
  body = "Hi Country Postal Ref Data Users, \n" + "\n The country_postal_ref data & lookup sheet data has been generated and is available for your validation. \n" + "\n Output location : s3://hp-bigdata-prod-ref-automation/GEO/country_postal_ref_output/country_postal_ref \n" +"Output location for lookup : s3://hp-bigdata-prod-ref-automation/GEO/country_postal_ref_output/country_postal_ref_lookup \n" + "\n" + "Thanks & Regards," + "\n" + "Enterprise Team"

  if load_status == 'PASSED' and BAT_status == "PASSED" and DQ_test_status == "PASSED" and  DQ_summary_status == "PASSED" :
    status = SendEmail(body, sender, email_result, subject)    
  else:
    dataos_splunk.send_to_splunk(metadata, splunk_integration(Errormsg,now,environment, instance))
    killCurrentProcess_with_metrics('Output files not generated properly, final email failure')
except Exception as e: 
  workflow_step = 'Error in generating files: Lookup or Country Postal Data for final output location.'
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess_with_metrics('Failed Output Generation')

# COMMAND ----------

#Move files to archive
workflow_step = 'archive started'
datestamp = '{:%Y%m%d}'.format(datetime.datetime.now())
# Get the directories to process from source_key_constants
dirs_to_process = dirs_included.get(instance, '/')  # default is current directory

# Remove the trailing / if present because it is the default and it must not be empty/blank/None (for loop)
if s3_dir.endswith('/'):
  s3_dir = s3_dir[:-1]

if  load_status == 'PASSED' and  BAT_status == "PASSED" and DQ_test_status == "PASSED" and DQ_summary_status == "PASSED":
  for directory in dirs_to_process:  # will call them directories because that is how we think of them
    processing_dir = s3_dir + directory
    # print ('Processing directory:', processing_dir)
    print (processing_dir)

    try:
      files_to_move = [i.name for i in dbutils.fs.ls(processing_dir) if i.name.endswith('.csv')]
      print(files_to_move)
      if files_to_move:
        to_dir = processing_dir 
        # + 'archive/'
        print (' archiving to:', to_dir)
        for a_file in files_to_move:
          from_file = processing_dir + a_file
          to_file = to_dir + datestamp + '_' + a_file
          print ('  moving', datestamp + '_' + a_file)
          print (datestamp + '_' + a_file)

          dbutils.fs.mv(from_file, to_file)
    except Exception as e:  # continue the notebook so we get metrics data
      workflow_step = 'archive failed'
      dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
      print (str(e))
  workflow_step = 'archive completed'


# COMMAND ----------

###             Creating Metrics of Run Stats for Dashboards                                            ###
###########################################################################################################
try:
  workflow_step = 'Metrics creation start'
  workflow_end_time = time.time() 
  end_time = '{:%Y-%m-%d %H:%M:%S.%f}'.format(datetime.datetime.now())[:-4]
  workflow_runtime = round((workflow_end_time - workflow_start_time) / 60.0, 2)


  source_type = prod_switch
  if source_type == 'inklaser':
    source_type = 'MIXED'
  else :
    source_type = source_type.upper()

  metricsJson = {}
  metricsJson["pre_check_rs_runtime"] = 0
  metricsJson["app_runtime"] = load_runtime
  metricsJson["bat_runtime"] = BAT_runtime
  metricsJson["dq_test_runtime"] = DQ_test_runtime
  metricsJson["dq_summary_runtime"] = DQ_summary_runtime 
  metricsJson["dw_load_runtime"] = prod_load_runtime
  metricsJson["dw_test_runtime"] = RS_test_runtime
  metricsJson["workflow_runtime"] = workflow_runtime # this is in minutes
  # follows {:%Y-%m-%d %H:%M:%S.%f} format
  metricsJson["start_time"] = start_time
  metricsJson["end_time"] = end_time
  # whether data is relevant ink or laser or mixed
  metricsJson["source_type"] = source_type


  metricsJson["environment"] = environment
  metricsJson["final_output"] = ''
  metricsJson["job_name"] = source_key.get(instance).get('app').replace('_','-')
  metricsJson["processing_date"] = '{:%Y-%m-%d %H:%M:%S.%f}'.format(datetime.datetime.now())[:-4]
  #metricsJson["total_error_count_percent"] =0
  metricsJson["total_mod_count"] = 0
  metricsJson["total_mod_count_percent"] = 0
  metricsJson['total_inc_hist_count'] = ''

  #combines metrics
  metricsDict = dict(list(metricsJson.items()) + list(json.loads(load_nb).items()) + list(size_calculator_json_metrics.items()) + list(dq_summary_json_metrics.items()))
  metricsJson["total_error_count"] = metricsDict["total_error_count"]

  #adds throughput
  metricsDict["velocity_in_records_per_second"] = round((metricsDict.get("total_final_count")/(load_runtime*60.0)),2)

  metricsDict["good_percentage"] = round(((float(metricsDict["total_good_count"])/metricsDict["total_final_count"])*100),4)

  metricsDict["total_error_count_percent"] = round(((float(metricsDict["total_error_count"])/metricsDict["total_final_count"])*100),4)

  # job status will be equalivalent to success if job executes till now,
  # else it is marked as failed anyways
  if metricsDict["STATUS"] in ['PROCESSING_SUCCEEDED', 'SUCCESS', 'PASSED']:
     metricsDict["job_status"] = "SUCCESS"
  else:
     metricsDict["job_status"] = "FAILED"
  # hash '#' acts as delimiter for split later in any metrics processing 
  metricsDict["step_error"] =  metricsDict["STEP_ERROR"]
  # marks till which step execution occured, ideally value should be 'archive completed' for a end-to-end run
  metricsDict["workflow_step"] = workflow_step


  del metricsDict["STATUS"]
  del metricsDict["STEP_ERROR"]
  # del metricsDict["STEP"]

  temp_metricsDict = []
  for i in metricsDict.items():
    temp = list(i)

    #rounds percentages
    if 'percentage' in temp[0]:
      temp[1] = round(temp[1], 3)

    #adds prefix to all entries

    temp[0] = ('enrich' + '_' + temp[0]).lower()

    temp_metricsDict.append(temp)

  metricsDict = dict(temp_metricsDict)

  metricsJson = json.dumps(metricsDict, sort_keys = True)
  workflow_step = 'Metrics creation end'
#######################################################################################################################################
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess_with_metrics('Failed Metrics creation '+e)


# COMMAND ----------

###########################################################################################################
###             Uploading Metrics json to splunk                                                       ###
###########################################################################################################
try:
  metricsJsonDist = json.loads(metricsJson)
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance,'end',metricsJsonDist))
###########################################################################################################
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance)) 
  exception_step(workflow_step,e,instance,environment,now)  
  killCurrentProcess_with_metrics('Failed Metrics upload '+e)
###########################################################################################################

# COMMAND ----------

import datetime
metrics_upload_status = False

timestamp = '{:%Y%m%d-%H%M%S}'.format(datetime.datetime.now()) # go from "yyyy-MM-dd HH:mm:ss" to "yyyyMMdd-HHmmss"
upload_file_name = source_key.get(instance).get("run_type") + '_' + timestamp + ".json"
workflowMetricsPath = s3_for_json_upload + upload_file_name
metrics_upload_status = dbutils.fs.put(workflowMetricsPath, metricsJson, overwrite = True)
print(workflowMetricsPath)

# COMMAND ----------

MetricsValidatorPath = './metrics_validation'
try:
  workflow_step = 'Metrics Validation started'
  if metrics_upload_status:
    metrics_validation_start = time.time()
    metrics_validation_nb = dbutils.notebook.run(
        MetricsValidatorPath,
        36000,
          arguments = {"metrics_file_location"    : workflowMetricsPath,
                      })
    metrics_validation_end = time.time()
    metrics_validation_runtime = round((metrics_validation_end - metrics_validation_start) / 60.0, 2)
    loadMetricJson = json.loads(metrics_validation_nb)
    metrics_validation_status = loadMetricJson["STATUS"]
    subject = '%s - %s - %s - Metrics validation Run Status'%(environment, instance, metrics_validation_status)
    validation_mesg = loadMetricJson["FINALRESULT"]
    status = SendEmail(validation_mesg, sender, receivers, subject )
    if 'Failed Test Count : 0' in metrics_validation_nb:
      metrics_test_status = "PASSED"
    else:
      metrics_test_status = "FAILED"
      workflow_step = 'Metrics Validation completed'
      dataos_splunk.send_to_splunk(metadata, splunk_integration(loadMetricJson['STEP_ERROR'],now,environment, instance))
###########################################################################################################
except Exception as e:
  dataos_splunk.send_to_splunk(metadata, splunk_integration(workflow_step,now,environment, instance))
  exception_step(workflow_step,e,instance,environment,now)
  killCurrentProcess_with_metrics('Failed metrics validation')
###########################################################################################################


# COMMAND ----------

if metrics_test_status:
  msg = '{"STATUS":"SUCCESS"}'
else:
  msg = '{"STATUS":"FAILED"}'

# COMMAND ----------

dbutils.notebook.exit(msg)

# COMMAND ----------

