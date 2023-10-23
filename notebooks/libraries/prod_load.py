# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

###########################################################################################################
###               Get the Run time parameters from the Databricks widgets                               ###
###########################################################################################################
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

###########################################################################################################
###            Read the Final parquet Location                                                          ###
###########################################################################################################
df = spark.read.parquet(ParquetFinalPath)
final_count = df.count()

# COMMAND ----------

###########################################################################################################
###          Get the Final Table names Along with Backup and Temp table                                 ###
###########################################################################################################
final_bck = RedshiftFinalTable + "_bck"
rename_to_final_bck = final_bck[11:]
rename_to_final_xref = RedshiftFinalTable[11:]
final_tmp = RedshiftFinalTable + "_tmp"

print (final_bck,rename_to_final_bck,rename_to_final_xref,final_tmp)

# COMMAND ----------

###########################################################################################################
###          Setting variables for removing DSR records from backup table                               ###
###########################################################################################################

#do we need to delete SN/PN, CID or both?
del_sn = False
del_cid = False

#set a default for dsr_counts
dsr_counts = ['','']

#delete SN/PN
if any(x in RedshiftFinalTable for x in ["ref_enrich.wpp_participation_rate","ref_enrich.ww_instant_ink_enrollment_enrich","ref_enrich.ink_printer_id_xref",\
                                         "ref_enrich.laser_printer_id_xref_caspian"]):
  del_sn = True
  
#delete CID  
if any(x in RedshiftFinalTable for x in ["ref_enrich.ww_customer_ckm_enrich", "ref_enrich.us_customer_demographic_enrich"]):
  del_cid = True
  
#delete both
if any(x in RedshiftFinalTable for x in ["ref_enrich.ww_product_registration_ckm_enrich"]):
  del_sn = True
  del_cid = True

# COMMAND ----------

###########################################################################################################
###          Method to write the run details to the Redshift run tracking table                         ###
###########################################################################################################
def run_track(myvalues):
  
  try:
    
    query = "insert into %s (run_id, redshift_query_id, redshift_user_name,sw_ver,run_type,run_argument,\
              out_schema_name,out_table_name,out_table_full_name,start_ts,end_ts,\
              run_duration_seconds, run_duration_minutes, run_duration_hours,\
              out_table_row_count,run_success_flag,run_return_message) \
              VALUES ('%s', %d, '%s', '%s', '%s', '%s', '%s', '%s','%s','%s', '%s', %d, %.2f, %.2f,%d,'%s','%s') " % myvalues
  
    cur.execute(query)
    conn.commit()

    print("Wrote to run tracking table")
    steps_status[2]="PASSED"

  except Exception as e:
    print("Error while writing to run tracking table : {}".format(str(e)[:200]))
    conn.rollback()

###########################################################################################################
###          Method to remove DSR records from backup table                                             ###
###########################################################################################################
def filter_bck_records(bck_table, del_sn, del_cid):
  try:
    count_query = "select count(*) from {}".format(bck_table)
    cur_dsr.execute(count_query)
    prev_count = cur_dsr.fetchone()[0]
  
    query = ""
    if del_sn == True:
      query += """
      delete from {}
      where (product_number || '-' || serial_number) IN
      (select distinct (dsr.product_number || '-' || dsr.serial_number)
      from data_privacy.dsr_sn_blacklist_ref dsr
      where serial_number is not NULL
      and product_number is not NULL);
      """.format(bck_table)
      
    if del_cid == True:
      query += """
      delete from {}
      where cid IN
      (select distinct dsr.cid 
      from data_privacy.dsr_cid_blacklist_ref dsr
      where cid is not NULL);
      """.format(bck_table)
      
    if query != "":    
      cur_dsr.execute(query)
      conn_dsr.commit()
      
      cleanup = "commit;vacuum {};commit;analyze {};".format(bck_table, bck_table)
      cur.execute(cleanup)
      conn.commit()
    
    cur_dsr.execute(count_query)
    post_count = cur_dsr.fetchone()[0]
    
    global dsr_counts
    dsr_counts= prev_count, post_count
    
    print ("Removed DSR records from _bck table\n\tBefore delete: {}\n\tAfter delete: {}".format(dsr_counts[0], dsr_counts[1]))
    steps_status[1]="PASSED"

  except Exception as e:
    raise Exception(e)

# COMMAND ----------

###########################################################################################################
###                   Load the Data Into Redshift Table                                                 ###
###########################################################################################################
#do swap, write to RS, tmp swap, dsr from bck tables, write to run tracking
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

    try:
      run_msg = "creating _tmp table & truncating {} table".format(RedshiftFinalTable)
      query1 = """
        drop table if exists {};
        create table {} as select * from {};
        truncate {};
        """.format(final_tmp,final_tmp, RedshiftFinalTable, RedshiftFinalTable)
      
      cur.execute(query1)
      print("Created _tmp table & truncated {} table".format(RedshiftFinalTable))

      ## Writing Data to Redshift table
      run_msg = "writing data to Redshift table({})".format(RedshiftFinalTable)
  
      df.write \
      .format("com.databricks.spark.redshift") \
      .option("url", jdbc_url) \
      .option("dbtable", rs_table) \
      .option("tempdir", itg_tmp) \
      .option("aws_iam_role", iam) \
      .option("extracopyoptions", "maxerror as 100000 blanksasnull") \
      .mode("append") \
      .save() 
  
      print("Wrote data to Redshift table({})".format(RedshiftFinalTable))

      ## Renaming the table _tmp to _bck
      run_msg = "renaming _tmp table to _bck"
      query2 = """
        drop table if exists {};
        alter table {} rename to {};
        """.format(final_bck, final_tmp, rename_to_final_bck)
      
      cur.execute(query2)
      print("Renamed _tmp table to _bck")
      steps_status[0]="PASSED"

      ## Removing DRS records from _bck tables
      run_msg = "removing DRS records from _bck tables"
  
      filter_bck_records(final_bck, del_sn, del_cid)


    except Exception as e:
        print("Error while {} : {}".format(run_msg,e))
        err_msg = str(e)[:200]
        run_return_message = "Error while {} : {}".format(run_msg,err_msg)
        conn.rollback()

    ## Getting Query Id of Redshift table load for Run Tracking
    cur.execute(select_stmt_qry_id)
    for qry_id in cur:
      query_id = qry_id[0]
    
    ## Setting paramters for Run Tracking table load
    end_ts = datetime.now()
    run_duration_seconds = round((end_ts - start_ts).total_seconds())
    run_duration_minutes = round(run_duration_seconds/60,2)
    run_duration_hours = round(run_duration_minutes/60,2)
    
    if (steps_status[0]=="PASSED" and steps_status[1]=="PASSED") :
      run_success_flag = "TRUE"
      run_return_message = "Completed : Redshift table {} loaded successfully".format(rs_table)
    
    values = (RUN_TRACKING_TABLE_NAME,run_id,query_id,RedshiftUser,sw_ver,run_type,run_argument,out_schema_name,\
              out_table_name,rs_table,start_ts, end_ts, run_duration_seconds, run_duration_minutes,\
              run_duration_hours, final_count,run_success_flag,run_return_message)

    run_track(values)
    
    conn.close()
    conn_dsr.close()
    

# COMMAND ----------

###########################################################################################################
###                   Setting the Run Environment for the Redshift table load                           ###
###########################################################################################################
check_env = environment.lower()
load_env_list = []
if check_env == 'itg':
    load_env_list = ['ITG-02']
elif check_env == 'prod':
    if "inklaser" in prod_switch.lower():
      load_env_list = ['PROD-02', 'PROD-03']
    elif "laser" in prod_switch.lower():
      load_env_list = ['PROD-03']
    elif "ink" in prod_switch.lower():
      load_env_list = ['PROD-02']

# As in Prod we have 2 Database, we need to iterate over them for ITG iteration will be once.
###########################################################################################################
###                   Setting Parameters for data load into Redshift table in ITG/PROD02/PROD03         ###
###########################################################################################################
for db in load_env_list:
  #If ITG load, write final tables to ITG-02
  if db == "ITG-02":
    rs_env = "itg-02"
    ## default to failed
    load_result_itg = "Load to itg-02: FAILED"
    dsr_result_itg = "Remove DSR records from _bck table (itg-02): FAILED"
    run_track_result_itg = "Load to run-tracking (itg-02): FAILED"
    steps_status = ['FAILED','FAILED','FAILED']
    ## Connection Parameters
    rs_dbname=RedshiftDBName
    rs_user=RedshiftUser
    rs_password=RedshiftUserPass
    rs_host=RedshiftInstance
    rs_port=port
    rs_sslmode ='require'
    user_name =RedshiftUser
    jdbc_url =jdbc_url
    iam =itg_iam
  
  #If PROD ink load, write final tables to PROD-02
  if db == "PROD-02":
    rs_env = "prod-02"
    ## default to failed
    load_result_prod02 ="Load to prod-02: FAILED"
    dsr_result_prod02 = "Remove DSR records from _bck table (prod-02): FAILED"
    run_track_result_prod02 = "Load to run-tracking (prod-02): FAILED"
    steps_status = ['FAILED','FAILED','FAILED']
    ## Connection Parameters
    rs_dbname=RedshiftDBName
    rs_user=RedshiftUser
    rs_password=RedshiftUserPass
    rs_host=RedshiftInstance.replace('prod-04','prod-02')
    rs_port=port
    rs_sslmode ='require'
    user_name =RedshiftUser
    jdbc_url =jdbc_url.replace('prod-04','prod-02')
    iam = prod_iam
  
  #If PROD laser load, write final tables to PROD-03
  if db == "PROD-03":
    rs_env = "prod-03"
    ## default to failed
    load_result_prod03 = "Load to prod-03: FAILED"
    dsr_result_prod03 = "Remove DSR records from _bck table (prod-03): FAILED"
    run_track_result_prod03 = "Load to run-tracking (prod-03): FAILED"
    steps_status = ['FAILED','FAILED','FAILED']
    ## Connection Parameters
    rs_dbname=RedshiftDBName
    rs_user=RedshiftUser
    rs_password=RedshiftUserPass
    rs_host=RedshiftInstance.replace('prod-02','prod-04')
    rs_port=port
    rs_sslmode ='require'
    user_name =RedshiftUser
    jdbc_url =jdbc_url.replace('prod-02','prod-04')
    iam = prod_iam
    
    #vars for run tracking
  #   RUN_TRACKING_TABLE_NAME = prod03_RUN_TRACKING_TABLE_NAME
  
###########################################################################################################
###                   Displaying all the parameters                                                     ###
########################################################################################################### 
  print("============================================================================")
  print("RS Instance : {} \nRS User : {} \nRS Port : {} \nRS DBName : {}".format(rs_host,rs_user,rs_port,rs_dbname))
  print("============================================================================")
  
  try :
        #connection for swaps
        conn = psycopg2.connect(dbname=rs_dbname, user=rs_user, password=rs_password, host=rs_host, port=rs_port, sslmode =rs_sslmode)
        cur=conn.cursor()
    
        #connection for dsr
        conn_dsr = psycopg2.connect(dbname=rs_dbname, user=rs_user, password=rs_password, host=rs_host, port=port, sslmode ='require')
        cur_dsr = conn_dsr.cursor()
        
  except Exception as e:
        print("Error while connecting to Redshift : "+str(e))
        raise Exception(e)
  
  rs_writes()
###########################################################################################################
###                   Updating Passed/Failed metrices based on steps result                             ###
###########################################################################################################
  if rs_env=="itg-02":
    if steps_status[0]=="PASSED":
      load_result_itg = "Load to itg-02: PASSED"
      passed.append(load_result_itg)
    if steps_status[1]=="PASSED":
      dsr_result_itg = "Remove DSR records from _bck table (itg-02): PASSED"
      passed.append("Remove DSR records from _bck table (itg-02): PASSED\n\tBefore delete: {}\n\tAfter delete: {}".format(dsr_counts[0], dsr_counts[1])) 
    if steps_status[2]=="PASSED":
      run_track_result_itg = "Load to run-tracking (itg-02): PASSED"
      passed.append(run_track_result_itg)
          
  if rs_env=="prod-02":
    if steps_status[0]=="PASSED":
      load_result_prod02 ="Load to prod-02: PASSED"
      passed.append(load_result_prod02)
    if steps_status[1]=="PASSED":
      dsr_result_prod02 = "Remove DSR records from _bck table (prod-02): PASSED"
      passed.append("Remove DSR records from _bck table (prod-02): PASSED\n\tBefore delete: {}\n\tAfter delete: {}".format(dsr_counts[0], dsr_counts[1])) 
    if steps_status[2]=="PASSED":
      run_track_result_prod02 = "Load to run-tracking (prod-02): PASSED"
      passed.append(run_track_result_prod02) 
    
  if rs_env=="prod-03":
    if steps_status[0]=="PASSED":
      load_result_prod03 ="Load to prod-03: PASSED"
      passed.append(load_result_prod03)
    if steps_status[1]=="PASSED":
      dsr_result_prod03 = "Remove DSR records from _bck table (prod-03): PASSED"
      passed.append("Remove DSR records from _bck table (prod-03): PASSED\n\tBefore delete: {}\n\tAfter delete: {}".format(dsr_counts[0], dsr_counts[1])) 
    if steps_status[2]=="PASSED":
      run_track_result_prod03 = "Load to run-tracking (prod-03): PASSED"
      passed.append(run_track_result_prod03) 

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