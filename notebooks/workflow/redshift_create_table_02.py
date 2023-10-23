# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

###########################################################################################################
###               Get the Run time parameters from the Databricks widgets                               ###
###########################################################################################################

dbutils.widgets.text("RedshiftFinalTable","", "RedshiftFinalTable")
RedshiftFinalTable = dbutils.widgets.get("RedshiftFinalTable")

dbutils.widgets.text("envt_dbuser", "")
envt_dbuser = dbutils.widgets.get("envt_dbuser")

# COMMAND ----------

RedshiftFinalTableList = []

if '=' in RedshiftFinalTable:
  lst = RedshiftFinalTable.split(",")
  for i in lst:
    RedshiftFinalTableList.append(i.split("=")[1])
else:
  RedshiftFinalTableList = RedshiftFinalTable.split(",")
RedshiftFinalTableList

# COMMAND ----------

# MAGIC %run ../workflow/ddl_schema

# COMMAND ----------

import psycopg2
import ast

# COMMAND ----------

try :
  envt_dbuser_list = envt_dbuser.split(',')
  for evnt_db in envt_dbuser_list:
    creds_str = dbutils.notebook.run("../libraries/credentials_for_enterprise", 600, {"envt_dbuser":evnt_db})
    creds = ast.literal_eval(creds_str)

    rs_dbname       = creds['rs_dbname']
    rs_user         = creds['rs_user']
    rs_password     = creds['rs_password']
    rs_host         = creds['rs_host']
    rs_port         = creds['rs_port']
    rs_sslmode      = creds['rs_sslmode']

    ###########################################################################################################
    ###                   Displaying all the parameters                                                     ###
    ########################################################################################################### 
    print("============================================================================")
    print("RS Instance : {} \nRS User : {} \nRS Port : {} \nRS DBName : {}".format(rs_host,rs_user,rs_port,rs_dbname))
    print("============================================================================")


    conn = psycopg2.connect(dbname=rs_dbname, user=rs_user, password=rs_password, host=rs_host, port=rs_port, sslmode =rs_sslmode)
    cur=conn.cursor()
    #connection for swaps
    for table_name,query in dit.items():
        run_msg = "creating table {} if doesnot exist".format(table_name)
        cur.execute(query)

        print("Created table {} if doesnot exist".format(table_name))

        conn.commit()
    conn.close()
    Output = "SUCCESS"
except Exception as e:
  print("Error while connecting to Redshift : "+str(e))
  Output = "FAILED"
  raise Exception(e)

# COMMAND ----------

dbutils.notebook.exit(Output)

# COMMAND ----------

