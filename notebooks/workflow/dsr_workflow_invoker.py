# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("environment", "")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("instance", "")
instance = dbutils.widgets.get("instance")

# COMMAND ----------

# MAGIC %run ./source_key_constants

# COMMAND ----------

#timeout is in seconds, 600 == 10 minutes
x = dbutils.notebook.run("./workflow_params", 600, {"environment":environment})

# COMMAND ----------

dbutils.widgets.text("inputWidgetJson", x)
inputWidgetJson = dbutils.widgets.get("inputWidgetJson")

# COMMAND ----------

#create individual widgets
pairs = parse_input_widgets(inputWidgetJson)
for x in pairs:
  exec("%s = '%s'" % (x[0],x[1]))

#assigns instance to whatever source is running
print "\n\nInstance".ljust(27) + ": " + instance

#set more workflow constants
set_constants()

# COMMAND ----------

workflow_start_time = time.time() 
now = str(datetime.datetime.now().isoformat()).replace(':', '-')
print now 

# COMMAND ----------

if environment.lower() == 'prod' or environment.lower() == 'itg':
  try:
    pre_check_RS_start = time.time() 
    pre_check_RS_nb = dbutils.notebook.run(
      RedshiftConnectionTestPath,
      36000,
      arguments = {"AwsIAMRole"       : AwsIAMRole,
                   "Environment"      : environment,
                   "RedshiftDBName"   : RedshiftDBName,
                   "RedshiftInstance" : RedshiftInstance,
                   "RedshiftPort"     : RedshiftPort,
                   "RedshiftUser"     : RedshiftUser,
                   "RedshiftUserPass" : RedshiftUserPass,
                   "TempS3Bucket"     : TempS3Bucket,
                   "rs_final_read"    : rs_final_read,
                   "prod_switch"      : prod_switch
                  })

    pre_check_RS_end = time.time()
    pre_check_RS_runtime = round((pre_check_RS_end - pre_check_RS_start) / 60.0, 2)
    subject = '%s - %s - Pre-Check Redshift ran successfully'%(environment, instance)
    status = SendEmail(pre_check_RS_nb, sender, receivers, subject )
    print "Job Status: SUCCESS"
  except Exception as e:
    print str(e) + '\n\n'
    tb = traceback.format_exc()
    exception = "Exception on running Pre-Check Redshift. Stack Trace is: " + '\n' + str(tb)
    subject = 'FAILED: %s - Pre-Check Redshift Exception - %s '%(instance, environment + now)
    status = SendEmail(exception, sender, receivers, subject)
    print "Job Status: FAILED"
    print "Email Status: " + status
    killCurrentProcess('Failed to connect to Redshift')

# COMMAND ----------

print pre_check_RS_nb
if 'Test Status is           : PASSED' in pre_check_RS_nb:
  pre_check_RS_status = 'PASSED'
print "\n\nStatus: " + pre_check_RS_status

# COMMAND ----------

try:
    load_start = time.time()        
    load_nb = dbutils.notebook.run(
      DSRLoadPath,
      36000,
      arguments = {"s3_dir"                   : s3_dir,
                   "environment"              : environment,
                   "parquet_flags_read"       : parquet_flags_read,
                   "parquet_flags_write"      : parquet_flags_write,
                   "stg_dir"                  : stg_dir
                  })

    load_end  = time.time()
    load_runtime = round((load_end - load_start) / 60.0, 2)
    subject = '%s - %s - DSR Enrich Load ran successfully'%(environment, instance)
    status = SendEmail(load_nb, sender, receivers, subject )
    print "Job Status: SUCCESS"
except Exception as e:
  print str(e) + '\n\n'
  tb = traceback.format_exc()
  exception = "Exception on running DSR Enrich Load. Stack Trace is: " + '\n' + str(tb)
  subject = 'FAILED: %s - Enrich DSR Load Exception - %s '%(instance, environment + now)
  status = SendEmail(exception, sender, receivers, subject)
  print "Job Status: FAILED"
  print "Email Status: " + status
  killCurrentProcess('Failed data load')

# COMMAND ----------

print load_nb

# COMMAND ----------

if 'update_ts' in load_nb:
  load_status = "PASSED"
  try:
    loadMetricJson = json.loads(load_nb)
    update_ts = loadMetricJson["update_ts"]
  except Exception as e:
    print str(e)
    update_ts = "SOMETHING WENT WRONG"
    killCurrentProcess('Failed data load')
else:
  load_status = "FAILED"
  killCurrentProcess('Failed data load')

print lot_id

# COMMAND ----------

prod_load_runtime = "N/A"

try:
  if environment.lower() == "prod" or environment.lower() == "itg":    
    prod_load_start = time.time()
    prod_load_nb = dbutils.notebook.run(
      DSRProdLoadPath,
      36000,
      arguments = {"environment"        : environment,
                   "ParquetFinalPath"   : parquet_final_write,
                   "RedshiftUser"       : RedshiftUser,
                   "RedshiftUserPass"   : RedshiftUserPass,
                   "RedshiftDBName"     : RedshiftDBName,
                   "RedshiftFinalTable" : rs_final_read,
                   "RedshiftPort"       : RedshiftPort,
                   "RunTrackingTable"   : RunTrackingTable,
                   "email"              : email,
                   "lot_id"             : update_ts,
                   "run_type"           : source_key.get(instance).get("run_type"),
                   "run_loop"           : "Mid-month DSR final table for " + instance,
                   "sw_ver"             : sw_ver,
                   "prod_switch"        : prod_switch,
                   "RedshiftInstance"   : RedshiftInstance,
                   "jdbc_url"           : jdbc_url
                  })
    prod_load_end = time.time()

    prod_load_runtime = round((prod_load_end - prod_load_start) / 60.0, 2)
    subject = '%s - %s - Prod Load ran successfully'%(environment, instance)
    status = SendEmail(prod_load_nb, sender, receivers, subject )
    prod_load_status = "PASSED"
except Exception as e:
  print str(e) + '\n\n'
  tb = traceback.format_exc()
  exception = "Exception on running Prod Load. Stack Trace is: " + '\n' + str(tb)
  subject = 'FAILED: %s - Prod Load Exception - %s '%(instance, environment + now)
  status = SendEmail(exception, sender, receivers, subject)
  print exception
  prod_load_status = "FAILED"

# COMMAND ----------

if environment.lower() == "prod" or environment.lower() == "itg":
  print prod_load_status + '\n'
  print prod_load_nb
else: 
  print prod_load_status