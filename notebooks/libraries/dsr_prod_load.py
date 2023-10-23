# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("ParquetFinalPath","", "ParquetFinalPath")
ParquetFinalPath = dbutils.widgets.get("ParquetFinalPath")

dbutils.widgets.text("environment","", "environment")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("RedshiftFinalTable","", "RedshiftFinalTable")
RedshiftFinalTable = dbutils.widgets.get("RedshiftFinalTable")

dbutils.widgets.text("run_type","", "run_type")
run_type = dbutils.widgets.get("run_type")

dbutils.widgets.text("run_loop","", "run_loop")
run_loop = dbutils.widgets.get("run_loop")

dbutils.widgets.text("lot_id", "", "lot_id")
lot_id = dbutils.widgets.get("lot_id")
run_type = run_type + ": lot_id=" + str(lot_id)

dbutils.widgets.text("sw_ver", "1.0")
SW_VER = str(dbutils.widgets.get("sw_ver"))    

dbutils.widgets.text("RunTrackingTable", "")  #default table name for audit trail
RUN_TRACKING_TABLE_NAME = str(dbutils.widgets.get("RunTrackingTable"))

dbutils.widgets.text("RedshiftDBName", "", "RedshiftDBName")
RedshiftDBName = dbutils.widgets.get("RedshiftDBName")

dbutils.widgets.text("RedshiftPort", "", "RedshiftPort")
port = dbutils.widgets.get("RedshiftPort")

dbutils.widgets.text("RedshiftUser", "", "RedshiftUser")
RedshiftUser = dbutils.widgets.get("RedshiftUser")

dbutils.widgets.text("RedshiftUserPass", "", "RedshiftUserPass")
RedshiftUserPass = dbutils.widgets.get("RedshiftUserPass")

dbutils.widgets.text("prod_switch", "", "prod_switch")
prod_switch = dbutils.widgets.get("prod_switch")

dbutils.widgets.text("RedshiftInstance", "", "RedshiftInstance")
RedshiftInstance = dbutils.widgets.get("RedshiftInstance")

dbutils.widgets.text("jdbc_url", "", "jdbc_url")
jdbc_url = dbutils.widgets.get("jdbc_url")

# COMMAND ----------

# MAGIC %run ./credentials_for_enrichment

# COMMAND ----------

# MAGIC %run ./enrich_dq_module

# COMMAND ----------

# #RS connection vars from credentials_for_enrichment

# pro02_rs_url
# pro02_rs_dbname
# pro02_rs_user
# pro02_rs_pw
# pro02_jdbc_url

# pro03_rs_url
# pro03_rs_dbname
# pro03_rs_user
# pro03_rs_pw
# pro03_jdbc_url


# # todo: eventually prod02 run tracking should be moved to data_quality.run_tracking
# #       hardcoding prod03 for now to avoid multiple widgets
# RUN_TRACKING_TABLE_NAME
# prod03_RUN_TRACKING_TABLE_NAME = "data_quality.run_tracking"

# COMMAND ----------

# load results strings default - assign to passed if successful

load_result_itg = "Load to itg-01: N/A"
run_track_result_itg = "Load to run-tracking (itg-01): N/A"
dsr_result_itg = "Remove DSR records from _bck table: N/A"

load_result_prod02 ="Load to prod-02: N/A"
run_track_result_prod02 = "Load to run-tracking (prod-02): N/A"
dsr_result_prod02 = "Remove DSR records from _bck table: N/A"

load_result_prod03 = "Load to prod-03: N/A"
run_track_result_prod03 = "Load to run-tracking (prod-03): N/A"
dsr_result_prod03 = "Remove DSR records from _bck table: N/A"

passed = []
failed = []

# COMMAND ----------

df = spark.read.parquet(ParquetFinalPath)
final_count = df.count()

# COMMAND ----------

#prepare _bck and _tmp variables
final_tmp = RedshiftFinalTable + "_tmp"
final_bck = RedshiftFinalTable + "_bck"

print (final_tmp, final_bck)

# COMMAND ----------

#do we need to delete SN/PN, CID or both?
del_sn = False
del_cid = False

#set a default for dsr_counts
dsr_counts = ['','']

#delete SN/PN
if any(x in RedshiftFinalTable for x in ["ref_enrich.ink_printer_id_xref", "ref_enrich.laser_printer_id_xref_caspian"]):
  del_sn = True
  
#delete CID  
if any(x in RedshiftFinalTable for x in ["ref_enrich.ww_customer_ckm_enrich", "ref_enrich.us_customer_demographic_enrich"]):
  del_cid = True
  
#delete both
if any(x in RedshiftFinalTable for x in ["ref_enrich.ww_product_registration_ckm_enrich"]):
  del_sn = True
  del_cid = True

# COMMAND ----------

def create_tmp():
  query = """
  create table {} as select * from {};
  truncate {};
  """.format(
    final_tmp, RedshiftFinalTable, RedshiftFinalTable
  )
  cur.execute(query)
  conn.commit()
  
def write_df(df, table, jdbc_url, iam):
  df.write \
        .format("com.databricks.spark.redshift") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("tempdir", itg_tmp) \
        .option("aws_iam_role", iam) \
        .option("extracopyoptions", "maxerror as 100000 blanksasnull") \
        .mode("append") \
        .save()

def tmp_drop():
  query = """
  drop table if exists {};
  """.format(
    final_tmp
  )
  cur.execute(query)
  conn.commit()
  
def run_track(myvalues):
#   query = "insert into %s (user_name, sw_ver, run_type, run_loop, out_table_name, backup_table_name, start_timestamp, end_timestamp, run_duration_seconds, run_duration_minutes, run_duration_hours, out_table_row_count) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d) " % myvalues

  query = "insert into %s (run_id, redshift_query_id, redshift_user_name,sw_ver,run_type,run_argument,\
              out_schema_name,out_table_name,out_table_full_name,start_ts,end_ts,\
              run_duration_seconds, run_duration_minutes, run_duration_hours,\
              out_table_row_count,run_success_flag,run_return_message) \
              VALUES ('%s', %d, '%s', '%s', '%s', '%s', '%s', '%s','%s','%s', '%s', %d, %.2f, %.2f,%d,'%s','%s') " % myvalues

  cur.execute(query)
  conn.commit()
  
def filter_bck_records(table, del_sn, del_cid):
  count_query = "select count(*) from {}".format(RedshiftFinalTable)
  cur_dsr.execute(count_query)
  prev_count = cur_dsr.fetchone()[0]
  
  query = ""
  if del_sn == True:
    query += """
    delete 
    from {}
    where (product_number || '-' || serial_number) IN
    (select distinct (dsr.product_number || '-' || dsr.serial_number)
    from data_privacy.dsr_sn_blacklist_ref dsr
    where serial_number is not NULL
      and product_number is not NULL)
    ;
    """.format(
      table
    )
  if del_cid == True:
    query += """
    delete
    from {}
    where cid IN
    (select distinct dsr.cid 
    from data_privacy.dsr_cid_blacklist_ref dsr
    where cid is not NULL)
    ;
    """.format(
      table
    )
  if query != "":
    cur_dsr.execute(query)
    conn_dsr.commit()
    
    cleanup = "commit;vacuum {};commit;analyze {};".format(table, table)
    cur.execute(cleanup)
    conn.commit()
    
  cur_dsr.execute(count_query)
  post_count = cur_dsr.fetchone()[0]
  return prev_count, post_count

# COMMAND ----------

#write to RS, write to run tracking
def rs_writes():
  
  ## Initializing the parameters for Run Tracking table load
  run_argument = 'maxerror as 100000 blanksasnull'
  query_id = 0
  sw_ver=SW_VER
  run_success_flag = 'FALSE'
  run_return_message = ''
  rs_table = RedshiftFinalTable
  run_id = run_type  + rs_table
  out_schema_name = rs_table.split('.')[0]
  out_table_name = rs_table.split('.')[1]
  select_stmt_qry_id = 'select pg_last_query_id()'
  start_ts = datetime.now()

  create_tmp()
  print "Created _tmp tables"
  try:
    rs_table = RedshiftFinalTable
    start_ts = datetime.now()
    write_df(df, rs_table, jdbc_url, iam)
    end_ts = datetime.now()
    elapsed_time = round((end_ts - start_ts).total_seconds())

  except Exception as e:
    print e
    exception = "Error writing " + rs_table + " to Redshift"
    raise Exception(exception)
  tmp_drop()
  cur = conn.cursor()
  print "Wrote to new table, dropped _tmp"
  
  #dsr from _bck tables
  global dsr_counts
  dsr_counts = filter_bck_records(final_bck, del_sn, del_cid)
  print "Removed DSR records from _bck table\n\tBefore delete: {}\n\tAfter delete: {}".format(dsr_counts[0], dsr_counts[1])
  
      ## Setting paramters for Run Tracking table load
  end_ts = datetime.now()
  run_duration_seconds = round((end_ts - start_ts).total_seconds())
  run_duration_minutes = round(run_duration_seconds/60,2)
  run_duration_hours = round(run_duration_minutes/60,2)
  run_success_flag = "TRUE"
  run_return_message = "Completed : Redshift table {} loaded successfully".format(rs_table)

#   values = (RUN_TRACKING_TABLE_NAME, RedshiftUser, SW_VER, run_type, run_loop, rs_table, final_bck, end_ts, start_ts, elapsed_time, 0, 0, final_count)

  values = (RUN_TRACKING_TABLE_NAME,run_id,query_id,RedshiftUser,sw_ver,run_type,run_argument,out_schema_name,\
              out_table_name,rs_table,start_ts, end_ts, run_duration_seconds, run_duration_minutes,\
              run_duration_hours, final_count,run_success_flag,run_return_message)

  run_track(values)
  print "Wrote to run tracking table"

  conn.close()
  conn_dsr.close()

# COMMAND ----------

#If ITG load, write final tables to ITG-02
if environment.lower() == "itg":
  #default to failed
  load_result_itg = "Load to itg-02: FAILED"
  dsr_result_itg = "Remove DSR records from _bck table: FAILED"
  run_track_result_itg = "Load to run-tracking (itg-02): FAILED"
  
  #connection for swaps
  conn = psycopg2.connect(dbname=RedshiftDBName, user=RedshiftUser, password=RedshiftUserPass, host=RedshiftInstance_itg, port=port, sslmode ='require')
  cur=conn.cursor()
  
  #connection for dsr
  conn_dsr = psycopg2.connect(dbname=RedshiftDBName, user=RedshiftUser, password=RedshiftUserPass, host=RedshiftInstance_itg, port=port, sslmode ='require')
  cur_dsr = conn_dsr.cursor()
  
  #vars for run tracking
  user_name = RedshiftUser
  jdbc_url = jdbc_url
  iam = itg_iam
  
  rs_writes()
  load_result_itg = "Load to itg-02: PASSED"
  passed.append(load_result_itg)
  run_track_result_itg = "Load to run-tracking (itg-02): PASSED"
  passed.append(run_track_result_itg)
  dsr_result_itg = "Remove DSR records from _bck table: PASSED\n\tBefore delete: {}\n\tAfter delete: {}".format(dsr_counts[0], dsr_counts[1])
  passed.append(dsr_result_itg)

#If PROD ink load, write final tables to PROD-02
if environment.lower() == "prod" and "ink" in prod_switch:
  load_result_prod02 ="Load to prod-02: FAILED"
  dsr_result_prod02 = "Remove DSR records from _bck table: FAILED"
  run_track_result_prod02 = "Load to run-tracking (prod-02): FAILED"
  
  #connection for swaps
  conn = psycopg2.connect(dbname=RedshiftDBName, user=RedshiftUser, password=RedshiftUserPass, host=RedshiftInstance_prod02, port=port, sslmode ='require')
  cur = conn.cursor()
  
  #connection for dsr
  conn_dsr = psycopg2.connect(dbname=RedshiftDBName, user=RedshiftUser, password=RedshiftUserPass, host=RedshiftInstance_prod02, port=port, sslmode ='require')
  cur_dsr = conn_dsr.cursor()
  
  #vars for run tracking
  user_name = RedshiftUser
  jdbc_url = jdbc_url
  iam = prod_iam
  
  rs_writes()
  load_result_prod02 ="Load to prod-02: PASSED"
  passed.append(load_result_prod02)
  run_track_result_prod02 = "Load to run-tracking (prod-02): PASSED"
  passed.append(run_track_result_prod02)
  dsr_result_prod02 = "Remove DSR records from _bck table: PASSED\n\tBefore delete: {}\n\tAfter delete: {}".format(dsr_counts[0], dsr_counts[1])
  passed.append(dsr_result_prod02)
  
#If PROD laser load, write final tables to PROD-03
if environment.lower() == "prod" and "laser" in prod_switch:
  #default to failed
  load_result_prod03 = "Load to prod-03: FAILED"
  dsr_result_prod03 = "Remove DSR records from _bck table: FAILED"
  run_track_result_prod03 = "Load to run-tracking (prod-03): FAILED"
  
  #connection for swaps
  conn = psycopg2.connect(dbname=RedshiftDBName, user=RedshiftUser, password=RedshiftUserPass, host=RedshiftInstance_prod03, port=port, sslmode ='require')
  cur = conn.cursor()
  
  #connection for dsr
  conn_dsr = psycopg2.connect(dbname=RedshiftDBName, user=RedshiftUser, password=RedshiftUserPass, host=RedshiftInstance_prod03, port=port, sslmode ='require')
  cur_dsr = conn_dsr.cursor()
  
  #vars for run tracking
  user_name = RedshiftUser
#   RUN_TRACKING_TABLE_NAME = prod03_RUN_TRACKING_TABLE_NAME
  jdbc_url = jdbc_url
  iam = prod_iam
  
  rs_writes()
  load_result_prod03 = "Load to prod-03: PASSED"
  passed.append(load_result_prod03)
  run_track_result_prod03 = "Load to run-tracking (prod-03): PASSED"
  passed.append(run_track_result_prod03)
  dsr_result_prod03 = "Remove DSR records from _bck table: PASSED\n\tBefore delete: {}\n\tAfter delete: {}".format(dsr_counts[0], dsr_counts[1])
  passed.append(dsr_result_prod03)

# COMMAND ----------

for result in [load_result_itg, run_track_result_itg, dsr_result_itg, load_result_prod02, run_track_result_prod02, dsr_result_prod02, load_result_prod03, run_track_result_prod03, dsr_result_prod03]:
  if 'FAILED' in result:
    failed.append(result)

# COMMAND ----------

Underline = "---------------------------------------------------------------------------------------"
passedResults_str = ''
failedResults_str = ''

OutputNotebook = "Enrich Prod Load Results"

PassedResults_header = "Passed Tests:"
for test in passed:
  passedResults_str += "\n" + test
FailedResults_header = "Failed Tests:"
for test in failed:
  failedResults_str += "\n" + test

# COMMAND ----------

fullOutput = "\n".join([OutputNotebook,Underline, PassedResults_header, passedResults_str, Underline, FailedResults_header, failedResults_str])

# COMMAND ----------

dbutils.notebook.exit(fullOutput)