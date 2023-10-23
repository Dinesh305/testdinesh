# Databricks notebook source
# This notebook holds constant variables for each enrichment source

# COMMAND ----------

# Directories to process then move to the /archive folder after successful completion
dirs_included = {
    'country_postal_ref':['/']
  }

# COMMAND ----------

# expectedTypes_parquetFinal## key to tell what source is running

## note: some of these may need additional/special test notebooks for specific DQ's
source_key = {
    "country_postal_ref": {
    "LoadPath"               :"../app/country_postal_ref",
    # "extraction_notebook": "./data_ingestion",
    "s3_file_retention_period":90,
    "vacuum_file_retention_duration":"interval 1 seconds",
    "delta_log_retention_duration":"interval 1 seconds",
    "CalculatorPath": './size_calculator_03',
    "RedshiftCreateTablePath": "./redshift_create_table_02",
    "ProdLoadPath":"../libraries/prod_load_04",
    "DQSummaryPath":"../test/dq_summary_03",
    "vacuum_file_retention_duration":"interval 1 seconds",
    "delta_log_retention_duration":"interval 1 seconds",
	  "DSRLoadPath"            :"",
    "DQTestPath"             :"../test/Reference_DQ_validation",
    "BATPath"                :"../test/BAT_phase",
    "CalculatorPath": './size_calculator_03',
    "ProdLoadPath":"../libraries/prod_load_04",
    "workflow_params_path"   : "./workflow_params",
    "DQSummaryPath":"../test/dq_summary_03",
    "required_dq_checks"     :"dq9,dq70",
    "RedshiftCreateTablePath": "./redshift_create_table_02",
    "run_type"               :"REF_COUNTRY_POSTAL",
    "app"                    :"RF_COUNTRY_POSTAL_REF"
    }
}

# COMMAND ----------

# this is used by basic validations for expected types/columns

# Purpose - returns a list of column names
# Inputs - list of types. List of tuple pairs, [0] is the column name, [1] is the type of the column
# Outputs - list of column names
def get_columns(types):
  cols = []
  for i in types:
    cols.append(i[0])
  return cols

# COMMAND ----------

