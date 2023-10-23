# Databricks notebook source
dbutils.widgets.text("environment", "")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

import datetime
import dateutil.relativedelta
import json,ast

# COMMAND ----------

# Generate read and write months
today = datetime.date.today()

previous_day  = today - dateutil.relativedelta.relativedelta(days=1)
write_month = previous_day.strftime('%m')
write_day = previous_day.strftime('%d')

#Get write month (one month prior to current month)
prev_month = today - dateutil.relativedelta.relativedelta(months=1)
write_month = prev_month.strftime('%m')
write_year = prev_month.strftime('%Y')

write_year = str(today.year)
if today.month == 1:
  write_year = str(today.year -1)

print (today)
print(write_day)
print (write_month)
print (write_year)

# COMMAND ----------

if environment.lower() == 'prod':
  x = str(json.dumps({
    "s3_dir"                : "s3://ref-data-prod/country_postal_ref_input/look_up_sheet/",
    "stg_dir"               : "s3://ref-data-prod/stg/country_postal_ref/",
    "switch"                : "RS_table_creation,load,BAT,dq_test,dq_summary,RS_test",
    "environment"           : "PROD",
    "delta_final_write"     : "s3://ref-data-prod/country_postal_ref/final/",
    "delta_intermediate_write" : "s3://ref-data-prod/country_postal_ref/intermediate/",
    "delta_flags_write"     : "s3://ref-data-prod/country_postal_ref/flags/",
    "flags_archive_location": "s3a://ref-data-prod/country_postal_ref/flags_archive/",
    "output_location"       : "s3://hp-bigdata-prod-ref-automation/GEO/country_postal_ref_output/",
    "email"                 : "rcbbigdata@external.groups.hp.com,bigdata-enrichment-support@external.groups.hp.com",
    "email_result"          : "austin.saechao@hp.com, floyd.moore@hp.com, rcbbigdata@external.groups.hp.com,bigdata-enrichment-support@external.groups.hp.com",
    "dw_source_name"        : "ref_enrich.country_postal_ref",
    "sw_ver"                : "v3.0.0",
    "prod_switch"           : "ink_laser",
    "envt_dbuser"           : "2p2,4p2",
    "sender"                : "bigdata-enrichment-prod@hp8.us",
    "mapanet_master_table"  : "ref_enrich_adhoc.proto_country_postal_mapanet_master_ref",
    "geonames_master_table" : "ref_enrich_adhoc.proto_country_postal_geonames_master_ref"
}))

# COMMAND ----------

  if environment.lower() == 'itg':
    x = str(json.dumps({
      "s3_dir"                : "s3://ref-data-itg/country_postal_ref_input/look_up_sheet/archive/",
      "stg_dir"               : "s3://ref-data-itg/stg/country_postal_ref/",
      "switch"                : "RS_table_creation,load,BAT,dq_test,dq_summary,RS_test",
      "environment"           : "ITG",
      "delta_intermediate_write" : "s3://ref-data-itg/country_postal_ref/intermediate/",
      "delta_final_write"     : "s3://ref-data-itg/country_postal_ref/final/",
      "delta_flags_write"     : "s3://ref-data-itg/country_postal_ref/flags/",
      "flags_archive_location": "s3a://ref-data-itg/country_postal_ref/flags_archive/",
      "output_location"       : "s3://hp-bigdata-itg-ref-automation/GEO/country_postal_ref_output/",
      "email"                 : "rcbbigdata@external.groups.hp.com,bigdata-enrichment-support@external.groups.hp.com",
      "email_result"          : "rcbbigdata@external.groups.hp.com,bigdata-enrichment-support@external.groups.hp.com",
      "dw_source_name"        : "ref_enrich.country_postal_ref",
      "sw_ver"                : "v3.1.0-a",
      "sender"                : "bigdata-enrichment-itg@hp8.us",
      "prod_switch"           : "ink_laser",
      "envt_dbuser"           : "i2",
      "mapanet_master_table"  : "ref_enrich_adhoc.proto_country_postal_mapanet_master_ref",
      "geonames_master_table" : "ref_enrich_adhoc.proto_country_postal_geonames_master_ref"
})) 

# COMMAND ----------

if environment.lower() == 'dev':
  x = str(json.dumps({
    "s3_dir"                  : "s3://ref-data-dev/country_postal_ref_input/look_up_sheet/",
    "stg_dir"                 : "s3://ref-data-dev/workflow/stg/country_postal_ref/",
    "delta_flags_write"       : "s3://ref-data-dev/country_postal_ref/flags/",
    "delta_final_write"       : "s3://ref-data-dev/country_postal_ref/final/",
    "delta_intermediate_write": "s3://ref-data-dev/country_postal_ref/intermediate/",
    "flags_archive_location"   : "s3a://ref-data-dev/country_postal_ref/flags_archive/",
    "switch"                  : "RS_table_creation,load,BAT,dq_test,dq_summary,RS_test",
    "environment"             : "dev",
    "delta_flags_write"       : "s3://ref-data-dev/country_postal_ref/flags/",
    # "email"                 : "poulomi.tarania@hp.com",
    # "email_result"          : "poulomi.tarania@hp.com",
    "dw_source_name"          :"ref_enrich.country_postal_ref",
    "output_location"         : "s3://hp-bigdata-dev-ref-automation/GEO/country_postal_ref_output/",
    "sw_ver"                  : "v3.1.0-a",
    "sender"                  : "bigdata-enrichment-dev@hp8.us",
    "prod_switch"             : "ink_laser",
    "envt_dbuser"             : "d2",
    "mapanet_master_table"    : "ref_enrich_proto.proto_country_postal_mapanet_master_ref",
    "geonames_master_table"   :"ref_enrich_proto.proto_country_postal_geonames_master_ref"
}))

# COMMAND ----------

creds_str = dbutils.notebook.run("../libraries/credentials_for_enterprise", 600, {"envt_dbuser":ast.literal_eval(x)['envt_dbuser'].split(",")[0]})
creds = ast.literal_eval(creds_str)

x = ast.literal_eval(x)
x["s3_for_json_upload"]          = creds['metrics_location']
x["s3_for_json_upload_elk"]      = creds['elk_metrics_location']

# COMMAND ----------

dbutils.notebook.exit(str(json.dumps(x)))

# COMMAND ----------

dbutils.notebook.exit(str(json.dumps(x)))