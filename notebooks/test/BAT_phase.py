# Databricks notebook source
 dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("environment","","environment")
environment = str(dbutils.widgets.get("environment"))

dbutils.widgets.text("instance","","instance")
instance = str(dbutils.widgets.get("instance"))

dbutils.widgets.text("stg_dir", "", "stg_dir")
stg_dir = dbutils.widgets.get("stg_dir")

dbutils.widgets.text("delta_flags_write","", "delta_flags_write")
delta_flags_write = dbutils.widgets.get("delta_flags_write")

dbutils.widgets.text("delta_intermediate_write","", "delta_intermediate_write")
delta_intermediate_write = dbutils.widgets.get("delta_intermediate_write")

dbutils.widgets.text("email","","email")
email = str(dbutils.widgets.get("email"))

dbutils.widgets.text("source_data_name","","source_data_name")
source_data_name = str(dbutils.widgets.get("source_data_name"))

dbutils.widgets.text("output_location","","output_location")
output_dir = str(dbutils.widgets.get("output_location"))

# COMMAND ----------

print (environment)
print (instance)
print (stg_dir)
print(delta_flags_write)
print (delta_intermediate_write)
print (email)

# COMMAND ----------

# MAGIC %run ../libraries/generic_utils

# COMMAND ----------

# MAGIC %run ../workflow/source_key_constants

# COMMAND ----------

# MAGIC %run ../workflow/bat_data

# COMMAND ----------

source_name  = source_data_name

# COMMAND ----------

try:
  geo_names_stg_data_dir = stg_dir + 'geonames/'
  df_geo_names_stg = spark.read.format('delta').load(geo_names_stg_data_dir)
  df_geo_names_stg_count = df_geo_names_stg.count()
except Exception as e:
  print(e)
  df_geo_names_stg = 'N/A'

# COMMAND ----------

try:
  mapanet_stg_data_dir = stg_dir + 'mapanet/'
  df_mapanet_stg = spark.read.format('delta').load(mapanet_stg_data_dir)
  df_mapanet_stg_count = df_mapanet_stg.count()
except Exception as e:
  print(e)
  df_mapanet_stg = 'N/A'

# COMMAND ----------

try:
  look_up_stg_data_dir = stg_dir + 'look_up/'
  df_look_up_stg = spark.read.format('delta').load(look_up_stg_data_dir)
  df_look_up_stg_count = df_look_up_stg.count()
except Exception as e:
  print(e)
  df_look_up_stg = 'N/A'

# COMMAND ----------

try:
  flags_cols = spark.read.format('delta').load(delta_flags_write)
  flags_cols_count = flags_cols.count()
except Exception as e:
  print(e)
  flags_cols = 'N/A'

# COMMAND ----------

try:
  final_cols = spark.read.format('delta').load(delta_intermediate_write)
  final_cols_count = final_cols.count()
except Exception as e:
  print(e)
  final_cols = 'N/A'

# COMMAND ----------

# final_cols.printSchema()

# spark.read.format('delta').load('s3://ref-data-itg/country_postal_ref/intermediate/')

# spark.read.format('delta').load('s3://ref-data-itg/country_postal_ref/intermediate/')

# COMMAND ----------

try:
  output_data_write = output_dir + 'country_postal_ref/'
  df_output = spark.read.format('com.databricks.spark.csv').option('header','True').load(output_data_write)
  df_output_count = df_output.count()
except Exception as e:
  print(e)
  df_output = 'N/A'

# COMMAND ----------

try:
  output_data_write = output_dir + 'country_postal_ref_lookup/'
  df_output_lookup = spark.read.format('com.databricks.spark.csv').option('header','True').load(output_data_write)
  df_output_lookup_count = df_output_lookup.count()
except Exception as e:
  print(e)
  df_output_lookup = 'N/A'

# COMMAND ----------

passedTests = []
failedTests = []
tuple_type = ''
tupleTypeResult = ''
Underline = "----------------------------------------------------------------------------------------------------------------"

# COMMAND ----------

file_kind = 'Output Country Postal Ref Data'
source_data_name = 'country_postal_ref'
try:
  actual_schema = df_output.schema
except Exception as e:
  print (e)
  actual_schema = 'N/A'

try :               
  expected_schema = expected_schema_output_location.get(source_data_name)
except Exception as e:
  print(e)
  expected_schema = 'N/A'

if (expected_schema != 'N/A' and actual_schema != 'N/A'):
  validate_schema(actual_schema,expected_schema,file_kind)
else:
  failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_dtypes = df_output.dtypes
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_dtypes = expected_dtypes_output_location.get(source_data_name)
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_datatypes(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_columns = get_columns(df_output.dtypes)
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_columns = get_columns(expected_dtypes_output_location.get(source_data_name))
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_columns(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

file_kind = 'Output LookUp Data'
source_data_name = 'look_up'
try:
  actual_schema = df_output_lookup.schema
except Exception as e:
  print (e)
  actual_schema = 'N/A'

try :               
  expected_schema = expected_schema_output_location.get(source_data_name)
except Exception as e:
  print(e)
  expected_schema = 'N/A'

if (expected_schema != 'N/A' and actual_schema != 'N/A'):
  validate_schema(actual_schema,expected_schema,file_kind)
else:
  failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_dtypes = df_output_lookup.dtypes
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_dtypes = expected_dtypes_output_location.get(source_data_name)
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_datatypes(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_columns = get_columns(df_output_lookup.dtypes)
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_columns = get_columns(expected_dtypes_output_location.get(source_data_name))
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_columns(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

file_kind = 'delta Flags'
source_data_name ='country_postal_ref'
try:
  actual_schema = flags_cols.schema
except Exception as e:
  print (e)
  actual_schema = 'N/A'

try :               
  expected_schema = expected_schema_delta_flags.get(source_data_name)
except Exception as e:
  print(e)
  expected_schema = 'N/A'

if (expected_schema != 'N/A' and actual_schema != 'N/A'):
  validate_schema(actual_schema,expected_schema,file_kind)
else:
  failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_dtypes = flags_cols.dtypes
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_dtypes = expected_dtypes_delta_flags.get(source_data_name)
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_datatypes(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_columns = get_columns(flags_cols.dtypes)
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_columns = get_columns(expected_dtypes_delta_flags.get(source_data_name))
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_columns(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

file_kind = 'delta Final'
try:

  # def show_data(input_filename):
  #   df = sqlContext.read.json(input_filename)
  #   df.show()

  actual_schema = final_cols.schema
except Exception as e:
  print (e)
  actual_schema = 'N/A'

try :
  expected_schema = expected_schema_delta_final.get(source_data_name)
except Exception as e:
  print(e)
  expected_schema = 'N/A'

if (expected_schema != 'N/A' and actual_schema != 'N/A'):
  validate_schema(actual_schema,expected_schema,file_kind)
else:
  failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

file_kind = 'delta Final'
try:
  actual_dtypes = final_cols.dtypes
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_dtypes = expected_dtypes_delta_final.get(source_data_name)
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_datatypes(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

file_kind = 'delta Final'
try:
  actual_columns = get_columns(final_cols.dtypes)
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_columns = get_columns(expected_dtypes_delta_final.get(source_data_name))
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_columns(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

file_kind = 'Geonames Delta Stage'
source_data_name = 'geo_names'
try:
  actual_schema = df_geo_names_stg.schema
except Exception as e:
  print (e)
  actual_schema = 'N/A'

try :
  expected_schema = expected_schema_delta_stg.get(source_data_name)
except Exception as e:
  print(e)
  expected_schema = 'N/A'

if (expected_schema != 'N/A' and actual_schema != 'N/A'):
  validate_schema(actual_schema,expected_schema,file_kind)
else:
  failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_dtypes = df_geo_names_stg.dtypes
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_dtypes = expected_dtypes_delta_stg.get(source_data_name)
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_datatypes(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_columns = get_columns(df_geo_names_stg.dtypes)
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_columns = get_columns(expected_dtypes_delta_stg.get(source_data_name))
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_columns(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

file_kind = 'Mapanet Delta Stage'
source_data_name = 'mapanet'
try:
  actual_schema = df_mapanet_stg.schema
except Exception as e:
  print (e)
  actual_schema = 'N/A'

try :
  expected_schema = expected_schema_delta_stg.get(source_data_name)
except Exception as e:
  print(e)
  expected_schema = 'N/A'

if (expected_schema != 'N/A' and actual_schema != 'N/A'):
  validate_schema(actual_schema,expected_schema,file_kind)
else:
  failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_dtypes = df_mapanet_stg.dtypes
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_dtypes = expected_dtypes_delta_stg.get(source_data_name)
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_datatypes(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_columns = get_columns(df_mapanet_stg.dtypes)
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_columns = get_columns(expected_dtypes_delta_stg.get(source_data_name))
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_columns(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

file_kind = 'Look Up Delta Stage'
source_data_name = 'look_up'
try:
  actual_schema = df_look_up_stg.schema
except Exception as e:
  print (e)
  actual_schema = 'N/A'

try :
  expected_schema = expected_schema_delta_stg.get(source_data_name)
except Exception as e:
  print(e)
  expected_schema = 'N/A'

if (expected_schema != 'N/A' and actual_schema != 'N/A'):
  validate_schema(actual_schema,expected_schema,file_kind)
else:
  failedTests.append(Underline + "\n" + "Failed to validate schema as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_dtypes = df_look_up_stg.dtypes
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_dtypes = expected_dtypes_delta_stg.get(source_data_name)
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_datatypes(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Datatypes as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

try:
  actual_columns = get_columns(df_look_up_stg.dtypes)
except Exception as e:
  print (e)
  actual_dtypes = 'N/A'

try :
  expected_columns = get_columns(expected_dtypes_delta_stg.get(source_data_name))
except Exception as e:
  print(e)
  expected_dtypes = 'N/A'

if (expected_dtypes != 'N/A' and actual_dtypes != 'N/A'):
  validate_columns(actual_dtypes,expected_dtypes,file_kind)
else:
  failedTests.append("Failed to validate Columns as {} {} is empty/not existing.".format(source_name,file_kind))

# COMMAND ----------

TotalTestsCount = len(passedTests) + len(failedTests)
passedCount = len(passedTests)
failedCount = len(failedTests)
passPercent = (passedCount/TotalTestsCount)*100
failPercent = (failedCount/TotalTestsCount)*100
passedValidations = ''
failedvalidations = ''

for testpassed in passedTests:
  passedValidations += "\n" + testpassed
for testpassed in failedTests:
  failedvalidations += "\n" + testpassed

# COMMAND ----------

TestOutputNotebook = "Enrichment Validation Test Output for "+instance
Environment        = "Environment is    : " + environment 
Instance           = "Instance is    : " + instance 
Counts             = "Counts:"
TotalTestCount     = "Total Test Count  : " + str(TotalTestsCount)
passedTestsCount   = "Passed Test Count : " + str(len(passedTests))
Percentage         = "PASS/FAIL Percentage:"
failedTestsCount   = "Failed Test Count : " + str(len(failedTests))
passedPercentage   = "Passed Test Percentage : " + str(passPercent)+"%" 
failedPercentage   = "Failed Test Percentage : " + str(failPercent)+"%"
PassedTestsText    = "\nPassed Tests:"
passedValidations  = passedValidations
FailedTestsText    = "\nFailed Tests:" 
failedvalidations  = failedvalidations

fullTestOutput = "\n".join([TestOutputNotebook,Underline,Environment,Underline,Counts,TotalTestCount,passedTestsCount,failedTestsCount,Underline,Percentage,passedPercentage,failedPercentage,Underline,PassedTestsText,passedValidations,Underline,FailedTestsText,failedvalidations])

print (fullTestOutput)

# COMMAND ----------

dfResult = spark.createDataFrame([("Passed",passedCount),("Failed",failedCount)],["State","Result"])
dfResult.createOrReplaceTempView("resultTable")
display(spark.sql("SELECT * FROM resultTable"))

# COMMAND ----------

dbutils.notebook.exit(fullTestOutput)

# COMMAND ----------

