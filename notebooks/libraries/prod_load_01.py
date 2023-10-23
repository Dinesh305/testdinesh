# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# Set the default values 
###########################################################################################################
###               Get the Run time parameters from the Databricks widgets                               ###
###########################################################################################################
dbutils.widgets.text("ParquetFinalPath","", "ParquetFinalPath")
ParquetFinalPath = dbutils.widgets.get("ParquetFinalPath")

dbutils.widgets.text("environment","", "environment")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text("RedshiftFinalTable","", "RedshiftFinalTable")
RedshiftFinalTable = dbutils.widgets.get("RedshiftFinalTable")

dbutils.widgets.text("dsr_input","", "dsr_input")
dsr_input = dbutils.widgets.get("dsr_input")

dbutils.widgets.text("run_type","", "run_type")
run_type = dbutils.widgets.get("run_type")

dbutils.widgets.text("lot_id", "", "lot_id")
lot_id = dbutils.widgets.get("lot_id")
run_type = run_type + ": lot_id=" + str(lot_id)

dbutils.widgets.text("sw_ver", "", "sw_ver")
sw_ver = str(dbutils.widgets.get("sw_ver"))    

dbutils.widgets.text("RunTrackingTable", "", "RunTrackingTable")
RunTrackingTable = dbutils.widgets.get("RunTrackingTable")

dbutils.widgets.text("RedshiftDBName", "", "RedshiftDBName")
RedshiftDBName = dbutils.widgets.get("RedshiftDBName")

dbutils.widgets.text("RedshiftPort", "", "RedshiftPort")
RedshiftPort = dbutils.widgets.get("RedshiftPort")

dbutils.widgets.text("RedshiftUser", "", "RedshiftUser")
RedshiftUser = dbutils.widgets.get("RedshiftUser")

dbutils.widgets.text("RedshiftUserPass", "", "RedshiftUserPass")
RedshiftUserPass = dbutils.widgets.get("RedshiftUserPass")

dbutils.widgets.text("prod_switch", "", "prod_switch")
prod_switch = dbutils.widgets.get("prod_switch")

dbutils.widgets.text("jdbc_url","", "jdbc_url")
jdbc_url = dbutils.widgets.get("jdbc_url")

dbutils.widgets.text("RedshiftInstance","", "RedshiftInstance")
RedshiftInstance = dbutils.widgets.get("RedshiftInstance")

dbutils.widgets.text("TempS3Bucket","", "TempS3Bucket")
TempS3Bucket = dbutils.widgets.get("TempS3Bucket")

# unused widget
# dbutils.widgets.text("run_loop","", "run_loop")
# run_loop = dbutils.widgets.get("run_loop")

# COMMAND ----------

RedshiftFinalTable = dict(x.split('=') for x in RedshiftFinalTable.split(','))
print (RedshiftFinalTable)
print ('-------------------------------------------------------------------------------')

# the 1 argu value is for maxsplit in split(demiliter,maxsplit) func, this ensures that first occurrenct of delimiter is the one, that causes the split in the string x
ParquetFinalPath = dict(x.split('=',1) for x in ParquetFinalPath.split(','))
print (ParquetFinalPath)
print ('-------------------------------------------------------------------------------')

if len(dsr_input) > 0:
  dsr_input= dict(x.split('=') for x in dsr_input.split(','))
  print (dsr_input)

# COMMAND ----------

dsr_tbl_list = []
if len(dsr_input) > 0:
  for k,v in dsr_input.items():
    dsr_tbl_list.append(v)
  print ('DSR Table list: ',dsr_tbl_list)

# COMMAND ----------

# MAGIC %run ../libraries/credentials_for_enrichment

# COMMAND ----------

# MAGIC %run ../libraries/enrich_dq_module

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

###########################################################################################################
###                Create the list of Dictionary Variables to be send on notebook exit                  ###
###########################################################################################################
metric_dict = OrderedDict()

metric_dict["STATUS"] = 'FAILURE'

metric_dict["STEP_ERROR"] = 'DW_LOAD_START#NO_ERROR'

# COMMAND ----------

###########################################################################################################
###              load results strings default - assign to passed if successful                          ###
###########################################################################################################
# load results strings default - assign to passed if successful
passed = []
failed = []

# COMMAND ----------

###########################################################################################################
###          Method to remove DSR records from backup table                                             ###
###########################################################################################################
def filter_bck_records(bck_table, del_sn, del_cid):
  try:
    print ("===========================================================================================")
    print ("**** DSR process started for table {} ****".format(tbl_bck))
    count_query = "select count(*) from {}".format(bck_table)
    cur_dsr.execute(count_query)
    prev_count = cur_dsr.fetchone()[0]
  
    query = ""
    if del_sn == True:
      query += """
      delete from {}
      where (product_number || '-' || serial_number) IN
      (select distinct (dsr.product_number || '-' || dsr.serial_number)
      from data_privacy.dsr_sn_blacklist_enrich dsr
      where serial_number is not NULL
      and product_number is not NULL);
      """.format(bck_table)
      
    if del_cid == True:
      query += """
      delete from {}
      where cid IN
      (select distinct dsr.cid 
      from data_privacy.dsr_cid_blacklist_enrich dsr
      where cid is not NULL);
      """.format(bck_table)
      
    if query != "": 
      print(query)
      cur_dsr.execute(query)
      conn_dsr.commit()
      
      cleanup = "commit;vacuum {};commit;analyze {};".format(bck_table, bck_table)
      cur.execute(cleanup)
      conn.commit()
    
    cur_dsr.execute(count_query)
    post_count = cur_dsr.fetchone()[0]
    
    global dsr_counts
    dsr_counts= prev_count, post_count
    
    print ("Removed DSR records from {} table\n\tBefore delete count: {}\n\tAfter delete count: {}".format(tbl_bck, dsr_counts[0], dsr_counts[1]))
  except Exception as e:
    exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###            Create DFs and get counts                                                                ###
###########################################################################################################
try:
  STEP = 'Create DFs and get counts'
  final_dfs = {}
  count_dfs = {}
  for k,v in ParquetFinalPath.items():
    final_dfs[str(k)] = spark.read.parquet(v)
    count_dfs[str(k)] = final_dfs[str(k)].count()
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###                   Setting the Run Environment for the Redshift table load                           ###
###########################################################################################################
try:
  STEP = "Setting the Run Environment for the Redshift table load"
  check_env = environment.lower()
  load_db_list = []
  if check_env == 'dev' or check_env == 'dev_dsr':
      load_db_list = ['DEV']
  elif check_env == 'itg' or check_env == 'itg_dsr':
      load_db_list = ['ITG-02']
  elif check_env == 'prod' or check_env == 'prod_dsr':
      if "inklaser" in prod_switch.lower():
        load_db_list = ['PROD-02', 'PROD-04']
      elif "laser" in prod_switch.lower():
        load_db_list = ['PROD-04']
      elif "ink" in prod_switch.lower():
        load_db_list = ['PROD-02']

  # As in Prod we have 2 Database, we need to iterate over them for ITG iteration will be once.
  ###########################################################################################################
  ###                   Setting Parameters for data load into Redshift table in ITG/PROD02/PROD04         ###
  ###########################################################################################################
  for db in load_db_list:
    print (db)
    if db == "DEV":
      rs_env = "dev"
      ## Connection Parameters
      rs_dbname=RedshiftDBName
      rs_user=RedshiftUser
      rs_password= get_rs_env_password(rs_env,rs_user)
      rs_host=RedshiftInstance_dev
      rs_port=port
      rs_sslmode ='require'
      user_name =RedshiftUser
      jdbc_url =get_rs_env_jdbc_url(rs_env,rs_user)
      iam =dev_iam
      
    #For ITG Ink and Laser ITG-02
    if db == "ITG-02":
      rs_env = "itg-02"
      ## Connection Parameters
      rs_dbname=RedshiftDBName
      rs_user=RedshiftUser
      rs_password= get_rs_env_password(rs_env,rs_user)
      rs_host=RedshiftInstance_itg
      rs_port=port
      rs_sslmode ='require'
      user_name =RedshiftUser
      jdbc_url =get_rs_env_jdbc_url(rs_env,rs_user)
      iam =itg_iam

    #For Prod Ink PROD-02
    if db == "PROD-02":
      rs_env = "prod-02"
      ## Connection Parameters
      rs_dbname=RedshiftDBName
      rs_user=RedshiftUser
      rs_password= get_rs_env_password(rs_env,rs_user)
      rs_host=RedshiftInstance_prod02
      rs_port=port
      rs_sslmode ='require'
      user_name =RedshiftUser
      jdbc_url =get_rs_env_jdbc_url(rs_env,rs_user)
      iam = prod_iam

    #For Prod Laser PROD-04
    if db == "PROD-04":
      rs_env = "prod-04"
      ## Connection Parameters
      rs_dbname=RedshiftDBName
      rs_user=RedshiftUser
      rs_password= get_rs_env_password(rs_env,rs_user)
      rs_host=RedshiftInstance_prod03
      rs_port=port
      rs_sslmode ='require'
      user_name =RedshiftUser
      jdbc_url =get_rs_env_jdbc_url(rs_env,rs_user)
      iam = prod_iam  
    ###########################################################################################################
    ###                   Displaying all the parameters                                                     ###
    ########################################################################################################### 
    print("============================================================================")
    print("RS Instance : {} \nRS User : {} \nRS Port : {} \nRS DBName : {}".format(rs_host,rs_user,rs_port,rs_dbname+' in '+ db))
    print("============================================================================")
    ###########################################################################################################
    ###########################################################################################################
    ### Loop through individual table per region for data load                                              ###
    ###########################################################################################################
    for k,v in RedshiftFinalTable.items():
      STEP = 'Loop through individual table per region for data load'
      out_table_row_count = 0
      # connection is inside this for-loop as we are closing the connection after write to run track
      # so for the next iteration there is no connection.
      conn = create_db_connection(rs_host,rs_port,user_name,rs_password,rs_dbname,rs_sslmode)
      cur = conn.cursor()
      
      if len(dsr_input) > 0:
        #connection for dsr
        conn_dsr = create_db_connection(rs_host,rs_port,user_name,rs_password,rs_dbname,rs_sslmode)
        cur_dsr = conn_dsr.cursor()
        
      # Variables Section
      print ("Load started for :" + v + " table to db "+ rs_dbname + " in " + db)
      #default to failed
      load_result = "Load " + v +" to "+ rs_dbname +" in " + db +": FAILED"
      run_track_result = "Load to run-tracking "+ rs_dbname + " in " + db +": FAILED"
      
      STEP = 'Variable Set up.'
      tbl_name = v.split(".")[1].lower()
      run_id_tbl = run_type + "#" +tbl_name
      run_argument = 'maxerror as 100000 blanksasnull'
      rs_query_id = 0
      run_success_flag = 'FALSE'
      run_return_message = ''
      out_schema_name = v.split(".")[0].lower()
      select_stmt_qry_id = 'select pg_last_query_id()'

      tbl_bck = v + "_bck"
      rename_to_tbl_bck = tbl_bck.split(".")[1]
      rename_to_tbl = tbl_name
      tmp_tbl = v + "_tmp"
      TempS3Bucket = TempS3Bucket + lot_id + "/" + v + "/"
      rename_to_tmp = tmp_tbl.split(".")[1]
      ###########################################################################################################
      ###              Actual Loading process to Redshift, Here DB Write is used to load to Redshift          ###
      ###########################################################################################################
      try:
        start_ts = datetime.now()
        out_table_row_count = count_dfs[k]
        STEP = 'Creating Temp Table.'
        prod_update = """
          drop table if exists %s;
          alter table %s rename to %s;
          """
        print(prod_update % (tmp_tbl,v,rename_to_tmp))
        cur.execute(prod_update % (tmp_tbl,v,rename_to_tmp))
        conn.commit()
        #to create table as temp table schema without data
        create_table = """
        create table %s as(select * from %s where 1=2);
        """
        print(create_table % (v,tmp_tbl))
        cur.execute(create_table % (v,tmp_tbl))
        conn.commit()
        STEP = 'Databricks Write to Redshift.'
        final_dfs[str(k)].write \
            .format("com.databricks.spark.redshift") \
            .option("url", jdbc_url) \
            .option("dbtable", v) \
            .option("tempdir", TempS3Bucket) \
            .option("aws_iam_role", iam) \
            .option("extracopyoptions", "maxerror as 100000 blanksasnull") \
            .mode("append") \
            .save()

        print ("Loaded table {}".format(v))
        STEP = 'Renaming temp table to actual table.'
        prod_update = """
        drop table if exists %s;
        alter table %s rename to %s;
        """
        print(prod_update % (tbl_bck, tmp_tbl, rename_to_tbl_bck))
        cur.execute(prod_update % (tbl_bck, tmp_tbl, rename_to_tbl_bck))
        conn.commit()
        run_success_flag = 'TRUE'
        run_return_message = v + ' Table Loaded Successfully.'
        load_result = "Load " + v + " to db " + rs_dbname + " " + db + ": SUCCESS \n"
        passed.append(load_result)
      ###########################################################################################################
      except Exception as e:
        conn.rollback()
        print (e)
        run_success_flag = 'FALSE'
        err_msg = str(e)[:200]
        exception = "Error writing to Redshift ({})".format(v)
        run_return_message = exception+ "#" + STEP + "#" + err_msg
        load_result = "Load " + v +" to " + rs_dbname + " " + db + ": FAILED \n"
        failed.append(load_result)             
      ###########################################################################################################
      ###     Removing DRS records from _bck tables                                                          ###
      ###########################################################################################################
      try:      
        STEP = 'Removing DRS records from _bck tables'
        del_sn = False
        del_cid = False

        if v in dsr_tbl_list:
          for action,dsr_tbl in dsr_input.items():
            if v == dsr_tbl:
              if action.lower() == 'sn':
                del_sn = True 
              if action.lower() == 'cid':
                del_sn = True

              filter_bck_records(tbl_bck, del_sn, del_cid)
              load_result = "DSR Removal for " + tbl_bck + " from db " + rs_dbname + " " + db + ": SUCCESS \n"
              passed.append(load_result)
      ###########################################################################################################
      except Exception as e:
        conn.rollback()
        print (e)
        run_success_flag = 'FALSE'
        err_msg = str(e)[:200]
        exception = "Error removing DSR records from Redshift tble ({})".format(tbl_bck)
        run_return_message = exception+ "#" + STEP + "#" + err_msg
        load_result = "DSR Removal for " + tbl_bck + " from db " + rs_dbname + " " + db + ": FAILED \n"
        failed.append(load_result)
      ##########################################################################################################
      ##              Below section deals with the Insert of Audit Record to Run Track Table                 ###
      ##########################################################################################################
      try:
        ########################################################################################################
        ###              Evaluating the Runtime statistics for the table load to redshift                    ###
        ########################################################################################################
        STEP = 'Evaluating the Runtime statistics'
        end_ts = datetime.now()
        elapsed_time_seconds = round(((end_ts - start_ts).total_seconds()))
        cur.execute(select_stmt_qry_id)
        for qry_id in cur:
          query_id = qry_id[0]
          print ("query_id for : ",k, " is ",query_id)
        ########################################################################################################
        STEP = 'Insert of Audit Record to Run Track Table'
        cur = conn.cursor()
        out_table_name = v
        backup_table_name = rename_to_tbl_bck

        print ("Run tracking for :" + v + " table in db "+ rs_dbname + " " + db)
        print ("===========================================================================================")

        run_duration_minutes = round(elapsed_time_seconds/60,2)
        run_duration_hours = round(run_duration_minutes/60,2)

        myvalues_audit = (RunTrackingTable, run_id_tbl, query_id,user_name,sw_ver,run_type,run_argument,\
                          out_schema_name,rename_to_tbl,v,start_ts, end_ts,\
                          elapsed_time_seconds,run_duration_minutes,run_duration_hours,\
                          out_table_row_count,run_success_flag,run_return_message)

        query1 = "insert into %s (run_id, redshift_query_id, redshift_user_name,sw_ver,run_type,run_argument,\
                  out_schema_name,out_table_name,out_table_full_name,start_ts,end_ts,run_duration_seconds, \
                  run_duration_minutes, run_duration_hours,out_table_row_count,run_success_flag,run_return_message) \
                  VALUES ('%s', %d, '%s', '%s', '%s', '%s', '%s', '%s','%s','%s', '%s', %d, %d, %.2f,%d,'%s','%s') " % myvalues_audit
        print (query1)
        cur.execute(query1)
        conn.commit()
        conn.close()

        run_track_result = "Load " + v +" to run-tracking " + rs_dbname + " " + db + ": SUCCESS \n"
        passed.append(run_track_result)
      ###########################################################################################################
      except Exception as e:
        print(str(e))
        run_track_result = "Load " + v +" to run-tracking " + rs_dbname + " " + db + ": FAILED \n"
        failed.append(run_track_result)
        conn.rollback()
        conn.close()
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

  def check_status(check_env,cntr,validate_count):
    if check_env in ("itg","dev","dev_dsr","itg_dsr") and cntr == validate_count:
      final_mesg = 'SUCCESS'
    elif check_env in ("itg","dev","dev_dsr","itg_dsr") and cntr < validate_count and cntr > 0:
      final_mesg = 'PARTIAL_SUCCESS'
    elif check_env in ("itg","dev","dev_dsr","itg_dsr") and cntr == 0:
      final_mesg = 'FAILED'
    # check for Prod
    if check_env == "prod"  or check_env == "prod_dsr":
      if prod_switch.lower() == "inklaser":
        validate_count = 2 * validate_count
        if cntr == validate_count:
          final_mesg = 'SUCCESS'
        elif cntr < validate_count and cntr > 0:
          final_mesg = 'PARTIAL_SUCCESS'
        else:
          final_mesg = 'FAILED'
      if prod_switch.lower() == "laser" or prod_switch.lower() == "ink":
        if cntr == validate_count:
          final_mesg = 'SUCCESS'
        elif cntr < validate_count and cntr > 0:
          final_mesg = 'PARTIAL_SUCCESS'
        else:
          final_mesg = 'FAILED'
    return(final_mesg)


# COMMAND ----------

###########################################################################################################
###                     Create final DW load report                                                     ###
###########################################################################################################
try: 
  STEP = 'Create final DW load report'
  Underline = "---------------------------------------------------------------------------------------"
  passedResults_str = ''
  failedResults_str = ''

  OutputNotebook = "Ref-Enrich Prod Load Results"

  PassedResults_header = "Successful Loads:"
  for test in passed:
    passedResults_str += " \n " + test
  FailedResults_header = "Failed Loads:"
  for test in failed:
    failedResults_str += " \n " + test
    
  # create the final message
  fullOutput = "\n".join([OutputNotebook,Underline, PassedResults_header, passedResults_str, Underline,\
                          FailedResults_header, failedResults_str])

  fullOutput = fullOutput + ". \n \n \n Please check run track table for more information."

  # for ITG we are loading 2 tables inventory and Sales
  # For prod we are loading 4 tables i.e. inventory and sales to both prod02 and prod04 
  validate_count = len(RedshiftFinalTable)
  validate_cnt_dsr = len(dsr_input)
  cntr_load = 0
  cntr_run_tracking = 0
  cntr_dsr = 0
  check_env = environment.lower()
  final_mesg = ''
  for test in passed:
    if ('run-tracking' in test):
      cntr_run_tracking += 1
    elif ('DSR Removal' in test):
      cntr_dsr += 1
    else:
      cntr_load += 1
  run_tracking_status = check_status(check_env,cntr_run_tracking,validate_count)
  rs_load_status      = check_status(check_env,cntr_load,validate_count)
  if validate_cnt_dsr > 0:
     dsr_status          = check_status(check_env,cntr_dsr,validate_cnt_dsr)
  else:
     dsr_status = 'N/A'
  print ("run_tracking_status is : ", run_tracking_status)
  print ("rs_load_status      is : ", rs_load_status)
  print ("dsr_status          is : ", run_tracking_status)

  if run_tracking_status == 'SUCCESS' and rs_load_status == 'SUCCESS' and (dsr_status == 'SUCCESS' or dsr_status == 'N/A') :
     final_mesg = 'SUCCESS'
  elif run_tracking_status == 'PARTIAL_SUCCESS' or rs_load_status == 'PARTIAL_SUCCESS' or dsr_status == 'PARTIAL_SUCCESS':
     final_mesg = 'PARTIAL_SUCCESS'
  else:
     final_mesg = 'FAILED'      
  
  print (final_mesg)
  print (fullOutput)
###########################################################################################################
except Exception as e:
#   print (e)
  exit_notebook(STEP,e)

# COMMAND ----------

###########################################################################################################
###  Metrics for Logging the Stats and creating the Dictionary to be send as output of run              ###
###########################################################################################################
try:
  metric_dict["STATUS"] = final_mesg
  metric_dict["STEP_ERROR"] = 'DW_LOAD_END#NO_ERROR'
  metric_dict["MESSAGE"] = fullOutput
  metric_json = json.dumps(metric_dict)
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

dbutils.notebook.exit(metric_json)