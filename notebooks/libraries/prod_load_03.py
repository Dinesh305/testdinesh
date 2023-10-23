# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# Set the default values 
###########################################################################################################
###               Get the Run time parameters from the Databricks widgets                               ###
###########################################################################################################
dbutils.widgets.text("DeltaFinalPath","", "DeltaFinalPath")
DeltaFinalPath = dbutils.widgets.get("DeltaFinalPath")

dbutils.widgets.text("RedshiftFinalTable","", "RedshiftFinalTable")
RedshiftFinalTable = dbutils.widgets.get("RedshiftFinalTable")

dbutils.widgets.text("dsr_input","", "dsr_input")
dsr_input = dbutils.widgets.get("dsr_input")

dbutils.widgets.text("prod_switch", "", "prod_switch")
prod_switch = dbutils.widgets.get("prod_switch")

dbutils.widgets.text("envt_dbuser", "")
envt_dbuser = dbutils.widgets.get("envt_dbuser")

# COMMAND ----------

import ast
import json
import re
from collections import OrderedDict

# COMMAND ----------

RedshiftFinalTable = dict(x.split('=') for x in RedshiftFinalTable.split(','))
print (RedshiftFinalTable)
print ('-------------------------------------------------------------------------------')

# the 1 argu value is for maxsplit in split(demiliter,maxsplit) func, this ensures that first occurrenct of delimiter is the one, that causes the split in the string x
DeltaFinalPath = dict(x.split('=',1) for x in DeltaFinalPath.split(','))
print (DeltaFinalPath)
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
  for k,v in DeltaFinalPath.items():
    final_dfs[str(k)] = spark.read.format('delta').load(v)
    count_dfs[str(k)] = final_dfs[str(k)].count()
###########################################################################################################
except Exception as e:
  exit_notebook(STEP,e)

# COMMAND ----------

try:
  envt_dbuser_list = envt_dbuser.split(',')
  for evnt_db in envt_dbuser_list:
    creds_str = dbutils.notebook.run("../libraries/credentials_for_enterprise", 600, {"envt_dbuser":evnt_db})
    creds = ast.literal_eval(creds_str)

    rs_env             = creds['rs_env']
    rs_dbname          = creds['rs_dbname']
    rs_user            = creds['rs_user']
    rs_password        = creds['rs_password']
    rs_host            = creds['rs_host']
    rs_port            = creds['rs_port']
    rs_sslmode         = creds['rs_sslmode']
    user_name          = creds['rs_user']
    jdbc_url           = creds['rs_jdbc_url']
    iam                = creds['aws_iam']
    tempS3Dir          = creds['tmp_dir']

    ###########################################################################################################
    ###                   Displaying all the parameters                                                     ###
    ########################################################################################################### 
    print("============================================================================")
    print("RS Instance : {} \nRS User : {} \nRS Port : {} \nRS DBName : {}".format(rs_host,rs_user,rs_port,rs_dbname+' in '+ rs_env))
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
      conn = create_db_connection(rs_host,rs_port,rs_user,rs_password,rs_dbname,rs_sslmode)
      cur = conn.cursor()

      if len(dsr_input) > 0:
        #connection for dsr
        conn_dsr = create_db_connection(rs_host,rs_port,rs_user,rs_password,rs_dbname,rs_sslmode)
        cur_dsr = conn_dsr.cursor()

      # Variables Section
      print ("Load started for :" + v + " table to db "+ rs_dbname + " in " + rs_env)
      #default to failed
      load_result = "Load " + v +" to "+ rs_dbname +" in " + rs_env +": FAILED"

      STEP = 'Variable Set up.'
      tbl_name = v.split(".")[1].lower()
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
      tempS3Dir = tempS3Dir + v + "/"
      rename_to_tmp = tmp_tbl.split(".")[1]
      ###########################################################################################################
      ###              Actual Loading process to Redshift, Here DB Write is used to load to Redshift          ###
      ###########################################################################################################
      try:
        start_ts = datetime.now()
        out_table_row_count = count_dfs[k]
        print(out_table_row_count)
        STEP = 'Creating Temp Table.'
        tbl_name_update = """
        drop table if exists %s;
        alter table %s rename to %s;
        """
        print(tbl_name_update % (tmp_tbl,v,rename_to_tmp))
        cur.execute(tbl_name_update % (tmp_tbl,v, rename_to_tmp))
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
            .option("tempdir", tempS3Dir) \
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
        load_result = "Load " + v + " to db " + rs_dbname + " " + rs_env + ": SUCCESS \n"
        passed.append(load_result)
      ###########################################################################################################
      except Exception as e:
        conn.rollback()
        print (e)
        run_success_flag = 'FALSE'
        err_msg = str(e)[:200]
        exception = "Error writing to Redshift ({})".format(v)
        run_return_message = exception+ "#" + STEP + "#" + err_msg
        load_result = "Load " + v +" to " + rs_dbname + " " + rs_env + ": FAILED \n"
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
              load_result = "DSR Removal for " + tbl_bck + " from db " + rs_dbname + " " + rs_env + ": SUCCESS \n"
              passed.append(load_result)
      ###########################################################################################################
      except Exception as e:
        conn.rollback()
        print (e)
        run_success_flag = 'FALSE'
        err_msg = str(e)[:200]
        exception = "Error removing DSR records from Redshift tble ({})".format(tbl_bck)
        run_return_message = exception+ "#" + STEP + "#" + err_msg
        load_result = "DSR Removal for " + tbl_bck + " from db " + rs_dbname + " " + rs_env + ": FAILED \n"
        failed.append(load_result)
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

  # for ITG we are loading 2 tables inventory and Sales
  # For prod we are loading 4 tables i.e. inventory and sales to both prod02 and prod04 
  validate_count = len(RedshiftFinalTable)
  validate_cnt_dsr = len(dsr_input)
  cntr_load = 0
  cntr_dsr = 0
  check_env = re.sub('[^A-Za-z]+', '', rs_env).lower()
  final_mesg = ''
  for test in passed:
    if ('DSR Removal' in test):
      cntr_dsr += 1
    else:
      cntr_load += 1
  rs_load_status      = check_status(check_env,cntr_load,validate_count)
  if validate_cnt_dsr > 0:
     dsr_status          = check_status(check_env,cntr_dsr,validate_cnt_dsr)
  else:
     dsr_status = 'N/A'
  print ("rs_load_status      is : ", rs_load_status)

  if rs_load_status == 'SUCCESS' and (dsr_status == 'SUCCESS' or dsr_status == 'N/A') :
     final_mesg = 'SUCCESS'
  elif rs_load_status == 'PARTIAL_SUCCESS' or dsr_status == 'PARTIAL_SUCCESS':
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