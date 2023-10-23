# Databricks notebook source
import pyspark,psycopg2,boto3,re,math,os,sys,inspect
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

## Explain the memoization going on here...
lookupTableObjects = {}
ruleParameterOrder = {}
ruleParameterUsage = {}

# COMMAND ----------

# MAGIC %run "./Workflow - Structures"

# COMMAND ----------

# MAGIC %run "./Workflow - Utils"

# COMMAND ----------

# Reference Quality Check 01 ::
# This check ensures that the combination of values across an arbitrary number of columns is unique.
ruleParameterOrder["RF01_ASSERT_UNIQUE"] = ["columnList","dataframe","tableObject","testNumber"]
ruleParameterUsage["RF01_ASSERT_UNIQUE"] = None
def RF01_ASSERT_UNIQUE(columnList,dataframe,tableObject,testNumber):
  '''
  RF01_ASSERT_UNIQUE(columnList::LIST[<STRING>],lookupTable::<STRING>,dataframe::<Dataframe Object>,
                     tableObject::<Table Object>,testNumber::<INT>)
  
    This is a reference quality data check intended to check that a list of columns
    does not contain duplicates across those columns. This is similar to the
    Enrichment DQ9.

    Expected usage in the RuleDefinitions notebook:

        >> "RF01_ASSERT_UNIQUE(['<column_name_1>','<column_name_2>',...])"

    Arguments:

        columnList  - The list of strings representing the columns that should be uniqueness checked.
        dataframe   - The Dataframe object that contains the table being tested.
        tableObject - The Table object that contains all of the table's metadata.
        testNumber  - The index of the current test for column naming purposes.

      Returns:

        dataframe   - The Dataframe object with the new column that reports test failures added
                      to the end.
  '''
  myAssert(type(columnList) == list,QualityTestInputException,"RF01_ASSERT_UNIQUE 'columnList' expected to be a list [{}]".format(str(type(columnList))))
  myAssert(hasattr(tableObject,"meta"),QualityTestInputException,"RF01_ASSERT_UNIQUE 'tableObject' expected to be a Table [{}]".format(str(tableObject)))
  myAssert(type(testNumber)==int,QualityTestInputException,"RF01_ASSERT_UNIQUE 'testNumber' expected to be an int [{}]".format(str(testNumber)))
  testName = "test"+str(testNumber)
  dataframe = dataframe.select('*', row_number().over(Window.partitionBy(columnList).orderBy(columnList)).alias('ranked'))
  dataframe = dataframe.withColumn(testName,when(col("ranked") == 1,"").otherwise("RF01_ASSERT_UNIQUE({})".format(str(columnList)))).drop("ranked")
  return dataframe

# COMMAND ----------

# This is a reference data quality check intended to check that the number of characters
# for strings in a given column are between an upper and lower bound inclusive.
ruleParameterOrder["RF02_ASSERT_CHARACTER_LENGTH_RANGE"] = ["columnName","lowerBound","upperBound"]
ruleParameterUsage["RF02_ASSERT_CHARACTER_LENGTH_RANGE"] = {"columnName":"column","upperBound":"literal","lowerBound":"literal"}
@udf(StringType())
def RF02_ASSERT_CHARACTER_LENGTH_RANGE(value, lowerBound, upperBound):
  '''
  RF02_ASSERT_CHARACTER_LENGTH_RANGE(value::<STRING|None>, lowerBound::<INT>, upperBound::<INT>)
  
    This is a reference data quality check intended to check that the number of characters
    for strings in a given column are between an upper and lower bound inclusive.
    
    Expected usage in the RuleDefinitions notebook:
    
      >> "RF02_ASSERT_CHARACTER_LENGTH_RANGE('<column_name>',<lower_bound>,<upper_bound>)"

    Arguments:

      value - The string that should be tested, this will be the column under test. Note that
              this function ignores all nulls; null values will pass this test.
      lowerBound - The integer lower bound for the allowable length of the string.
      upperBound - The integer upper bound for the allowable length of the string.

    Returns:

      status - If the test passes then it will return "", if it fails then it will return the
               name of the test, which in this case is "RF02_ASSERT_CHARACTER_LENGTH_RANGE" with
               the parameters that were passed in.
  '''
  thisFunctionName = sys._getframe().f_code.co_name
  # perform the rule
  if value is None:
    return ""
  if len(value) < lowerBound or upperBound < len(value):
    return thisFunctionName+"(lowerBound="+str(lowerBound)+",upperBound="+str(upperBound)+")"
  return ""

# COMMAND ----------

# Reference Quality Check 03 ::
# This check ensures that all of the values in a single column 
# satisfy a given regular expression 
ruleParameterOrder["RF03_ASSERT_REGEX"] = ["columnName","regex"]
ruleParameterUsage["RF03_ASSERT_REGEX"] = {"columnName":"column","regex":"literal"}
@udf(StringType())
def RF03_ASSERT_REGEX(value,regex):
  '''
  RF03_ASSERT_REGEX(value::<STRING>|None, regex::<STRING>)
  
    This is a reference data quality check intended to check that the string values
    of a column follow a particular regex pattern. Note that this function ignores all
    null values. The reason for this is that there is a separate check that checks
    for nulls, and is assumed to be allowable elsewhere.
    
    This function adds '^' to the beginning and '$' to the end of the regex to
    enforce that the regex fits the whole string. If you are having issues with
    the regex, this may be why.
    
    Expected usage in the RuleDefinitions notebook:
    
      >> "RF03_ASSERT_REGEX('<column_name>','<regex_pattern>')"

    Arguments:

      value - The string that should be tested, this will be the column under test. Note that
              this function ignores all nulls; null values will pass this test.
      regex - The regex pattern to be tested as a string. This will get compiled internally
              in this function.

    Returns:

      status - If the test passes then it will return "", if it fails then it will return the
               name of the test, which in this case is "RF03_ASSERT_REGEX" with the
               parameters that were passed in.
  '''
  thisFunctionName = sys._getframe().f_code.co_name
  # perform the rule
  if value is None:
    return ""
  pattern = regex
  pattern = pattern[1:] if pattern != "" and pattern[0] == "^" else pattern
  pattern = pattern[:-1] if pattern != "" and pattern[-1] == "$" else pattern
  pattern = re.compile("^"+pattern+"$")
  matches = re.findall(pattern,value)
  if len(matches) == 0:
    return thisFunctionName+"("+str(value)+" , '"+regex+"')"
  return ""


# COMMAND ----------

ruleParameterOrder["RF05_ASSERT_CONDITIONAL_VALUE"] = ["columnName","lookupTable","dataframe","tableObject","testNumber"]
ruleParameterUsage["RF05_ASSERT_CONDITIONAL_VALUE"] = None
def RF05_ASSERT_CONDITIONAL_VALUE(column,lookupTable,dataframe,tableObject,testNumber):
  '''
  RF05_ASSERT_CONDITIONAL_VALUE(column::<STRING>,lookupTable::<STRING>,dataframe::<Dataframe Object>,
                                tableObject::<Table Object>,testNumber::<INT>)
  
    This is a reference quality data check intended to check that a value in a column
    is what is expected based on a value in another column. This check uses a lookup
    table in redshift to get the expected relationships between columns.

    Expected usage in the RuleDefinitions notebook:

        >> "RF05_ASSERT_CONDITIONAL_VALUE('<column_name>','<schema>.<table>')"

    Arguments:

        column      - The string representing the column that should be checked.
        lookupTable - The string representing the "<schema>.<table>" of the lookup table.
        dataframe   - The Dataframe object that contains the table being tested.
        tableObject - The Table object that contains all of the table's metadata.
        testNumber  - The index of the current test for column naming purposes.

      Returns:

        dataframe   - The Dataframe object with the new column that reports test failures added
                      to the end.
  
  '''
  global lookupTableObjects
  
  @udf(BooleanType())
  def innerUDF(value,relatedValue,relatedFieldString,acceptableValuesString):
    if relatedFieldString is None:
      return True
    relatedFieldList = [item.strip() for item in relatedFieldString.split(",")]
    relatedValue = str(relatedValue)
    acceptableValues = [item.strip() for item in acceptableValuesString.split(",")] + [None]
    failed = relatedValue in relatedFieldList and not value in acceptableValues
    return not failed

  @udf(BooleanType())
  def joinUDF(value,listString):
    return str(value) in [item.strip() for item in listString.split(",")]
  
  if lookupTable in lookupTableObjects:
    lookupTableDataframe = lookupTableObjects[lookupTable]
  else:
    lookupTableArgs = {
      "schema" : lookupTable.split(".")[0],
      "table"  : lookupTable.split(".")[1],
      "credentials" : tableObject.meta["credentials"],
      "source" : "redshift",
      "environment" : tableObject.meta["environment"],
      "s3Directory" : tableObject.meta["s3Directory"]
    }
    lookupTableDataframe = Table(lookupTableArgs).getDataframeFromRedshift().filter(col("field_name") == column)
    lookupTableObjects[lookupTable] = lookupTableDataframe    
  collected = lookupTableDataframe.collect()
  relatedField,relatedFieldString = collected[0]["related_field"],collected[0]["related_field_value"]
  testName = "test"+str(testNumber)
  dataframe = (dataframe
               .join(lookupTableDataframe,col("related_field_value").contains(col(relatedField)),"leftouter")
               .withColumn(testName,
                           when(innerUDF(column,"related_field","related_field_value","acceptable_values"),"")
                           .otherwise("RF05_ASSERT_CONDITIONAL_NULL({})".format(column)))
               .drop("field_name")
               .drop("related_field_value")
               .drop("related_field")
               .drop("acceptable_values")
              )
  return dataframe


# COMMAND ----------

def RF06_ASSERT_MULTICONDITIONAL_VALUE(column,lookupTable,dataframe,tableObject,testName):
  '''
  Not implemented yet
  '''

# COMMAND ----------

# This is a reference data quality check intended to check that the number of characters
# for strings in a given column are between an upper and lower bound inclusive.
ruleParameterOrder["RF07_ASSERT_VALUE_IN_LIST"] = ["columnName","acceptableValueList"]
ruleParameterUsage["RF07_ASSERT_VALUE_IN_LIST"] = {"columnName":"column","acceptableValueList":"literal"}
@udf(StringType())
def RF07_ASSERT_VALUE_IN_LIST(value, acceptableValueList):
  '''
  RF07_ASSERT_VALUE_IN_LIST(value::<ANY TYPE>, acceptableValueList::LIST[<type(value)>])
  
    This is a reference data quality check intended to check that the values within a column
    are within a discrete list of acceptable values. The acceptable value list must all be of
    the same type as the type of value and None will be appended to it.

    Arguments:

      value - The string that should be tested, this will be the column under test. Note that
              this function ignores all nulls; null values will pass this test.
      acceptableValueList - The list of elements that are considered valid for the input value.

    Returns:

      status - If the test passes then it will return "", if it fails then it will return the
               name of the test, which in this case is "RF07_ASSERT_VALUE_IN_LIST" with the
               parameters that were passed in.
  '''
  thisFunctionName = sys._getframe().f_code.co_name
  acceptableValueList = eval(acceptableValueList)
  # enforce the input constraints
  acceptableValueList = list(set([str(val) for val in acceptableValueList]+[None])) # <-| removes all duplicates from the list
  # perform the rule
  if not str(value) in acceptableValueList:
    return thisFunctionName+"({},{})".format(str(value),str(acceptableValueList))
  return ""


# COMMAND ----------

# This is a reference data quality check intended to check that the number of characters
# for strings in a given column are between an upper and lower bound inclusive.
ruleParameterOrder["RF08_ASSERT_NUMERIC_RANGE"] = ["columnName","lowerBound","upperBound"]
ruleParameterUsage["RF08_ASSERT_NUMERIC_RANGE"] = {"columnName":"column","lowerBound":"literal","upperBound":"literal"}
@udf(StringType())
def RF08_ASSERT_NUMERIC_RANGE(value, lowerBound, upperBound):
  '''
  RF08_ASSERT_NUMERIC_RANGE(value::<INT|FLOAT|None>, lowerBound::<INT|FLOAT>, upperBound::<INT|FLOAT>)
  
    This is a reference data quality check intended to check that the numeric values
    within a column are between the upper and lower bounds inclusive.

    Arguments:

      value - The number that should be tested, this will be the column under test. Note that
              this function ignores all nulls; null values will pass this test.
      lowerBound - The integer lower bound for the allowable length of the string.
      upperBound - The integer upper bound for the allowable length of the string.

    Returns:

      status - If the test passes then it will return "", if it fails then it will return the
               name of the test, which in this case is "RF08_ASSERT_NUMERIC_RANGE" with the
               parameters that were passed in.
  '''
  thisFunctionName = sys._getframe().f_code.co_name
  if value is None:
    return ""
  value = float(str(value))
  upperBound = float(str(upperBound))
  lowerBound = float(str(lowerBound))
  # perform the rule
  if value is None:
    return ""
  if value < lowerBound or upperBound < value:
    return thisFunctionName+"({},lowerBound={},upperBound={})".format(str(value),str(lowerBound),str(upperBound))
  return ""

# COMMAND ----------

# This is a reference data quality check intended to check that the value is 
# conditionally valid based on a lookup table
ruleParameterOrder["RF09_ASSERT_CONDITIONAL_RANGE_LOOKUP"] = ["columnName","lookupTable","dataframe","tableObject","testNumber"]
ruleParameterUsage["RF09_ASSERT_CONDITIONAL_RANGE_LOOKUP"] = None
def RF09_ASSERT_CONDITIONAL_RANGE_LOOKUP(column,lookupTable,dataframe,tableObject,testName):
  '''
  RF09_ASSERT_CONDITIONAL_RANGE_LOOKUP(column::<STRING>,lookupTable::<STRING>,dataframe::<Dataframe Object>,
                                       tableObject::<Table Object>,testNumber::<INT>)
  
    This is a reference quality data check intended to check that a value in a column
    is within an expected range based on the value in another column. These conditions
    are defined within 

    Expected usage in the RuleDefinitions notebook:

        >> "RF09_ASSERT_CONDITIONAL_RANGE_LOOKUP('<column_name>','<schema>.<table>')"

    Arguments:

        column      - The string representing the column that should be checked.
        lookupTable - The string representing the "<schema>.<table>" of the lookup table.
        dataframe   - The Dataframe object that contains the table being tested.
        tableObject - The Table object that contains all of the table's metadata.
        testNumber  - The index of the current test for column naming purposes.

      Returns:

        dataframe   - The Dataframe object with the new column that reports test failures added
                      to the end.
  
  '''
  global lookupTableObjects
  
  if lookupTable in lookupTableObjects:
    lookupTableDataframe = lookupTableObjects[lookupTable]
  else:
    lookupTableArgs = {
      "schema" : lookupTable.split(".")[0],
      "table"  : lookupTable.split(".")[1],
      "credentials" : tableObject.meta["credentials"],
      "source" : "redshift",
      "environment" : tableObject.meta["environment"],
      "s3Directory" : tableObject.meta["s3Directory"]
    }
    lookupTableDataframe = Table(lookupTableArgs).getDataframeFromRedshift().filter(col("field_name") == column)
    lookupTableObjects[lookupTable] = lookupTableDataframe
  relatedField = lookupTableDataframe.collect()[0]["related_field"]
  testName = "test"+str(testName)
  dataframe = (dataframe
              .join(lookupTableDataframe,col(relatedField) == col("related_field_value"),"leftouter")
              .withColumn(testName,when( isnull(col(column)) | ((col("range_start_inclusive") <= col(column))  &  (col(column) <= col("range_end_inclusive"))),"").otherwise("RF09_ASSERT_CONDITIONAL_RANGE_LOOKUP({})".format(column)))
              .drop("field_name")
              .drop("related_field")
              .drop("related_field_value")
              .drop("range_start_inclusive")
              .drop("range_end_inclusive")
              )
  return dataframe
  

# COMMAND ----------

ruleParameterOrder["RF10_ASSERT_DATE_RANGE"] = ["columnName","beginDate","endDate"]
ruleParameterUsage["RF10_ASSERT_DATE_RANGE"] = {"columnName":"column","beginDate":"literal","endDate":"literal"}
@udf(StringType())
def RF10_ASSERT_DATE_RANGE(value,beginDate,endDate):
  '''
  RF10_ASSERT_DATE_RANGE(value::<datetime.date Object>,beginDate::<STRING>,endDate::<STRING>)
  
    This is a reference data quality check intended to check that all of the values
    in a single date column fall within an acceptable range of dates denoted by the
    beginDate and endDate strings. Note that the format of the strings should be
    'YYYY-MM-DD' or 'TODAY' if you want to dynamically keep one of the parameters
    as the present date.
    
    Expected usage in the RuleDefinitions notebook:

        >> "RF10_ASSERT_DATE_RANGE('<column_name>','<YYYY-MM-DD>','<YYYY-MM-DD>')"
    
    Arguments:
    
      value - The value that should be tested, this will be the column under test. Note that
              this function ignores all nulls; null values will pass this test.
      beginDate - The string lower bound for the allowable begin date.
      endDate   - The string upper bound for the allowable end date.
      
    Returns:
    
      status - If the test passes then it will return "", if it fails then it will return the
               name of the test, which in this case is "RF10_ASSERT_DATE_RANGE" with the
               parameters that were passed in.
  '''
  beginDate = datetime.date.fromtimestamp(time.time()) if beginDate == "TODAY+YEAR" else datetime.date(*[int(part) for part in beginDate.split("-")])
  endDate   = datetime.date.fromtimestamp(time.time()) if endDate   == "TODAY+YEAR" else datetime.date(*[int(part) for part in endDate.split("-")])
  value     = value if isinstance(value,datetime.date) else datetime.date(*[int(part) for part in repr(value).split(" ")[0].split("-")])
  output    = ""
  print type(value)
  print type(beginDate)
  print type(endDate)
  if not (beginDate <= value and value <= endDate):
    output  = "RF10_ASSERT_DATE_RANGE(val={},begin={},end={})".format(str(value),str(beginDate),str(endDate))
  return output

# COMMAND ----------

# Reference Quality Check 11 ::
# This is a reference data quality check intended to check that all of the values in a single
# column are not null.
ruleParameterOrder["RF11_ASSERT_NOT_NULL"] = ["columnName"]
ruleParameterUsage["RF11_ASSERT_NOT_NULL"] = {"columnName":"column"}
@udf(StringType())
def RF11_ASSERT_NOT_NULL(value):
  '''
  RF11_ASSERT_NOT_NULL(value::<ANY TYPE>)
  
    This is a reference data quality check intended to check that all of the values in a single
    column are not null.
    
    Expected usage in the RuleDefinitions notebook:
    
      >> "RF11_ASSERT_NOT_NULL('<column_name>')"

    Arguments:

      value - The string that should be tested, this will be the column under test.

    Returns:

      status - If the test passes then it will return "", if it fails then it will return the
               name of the test, which in this case is "RF11_ASSERT_NOT_NULL" with the
               parameters that were passed in.
  '''
  thisFunctionName = sys._getframe().f_code.co_name
  # There are no input constraints here so just the rule
  if value is None:
    return thisFunctionName
  return ""


# COMMAND ----------

# Reference Quality Check 12 ::
# This is a reference data quality check intended to check that all of the values in a column
# are null based on a condition. This condition is found in a redshift "lookup table".
ruleParameterOrder["RF12_ASSERT_CONDITIONAL_NULL_LOOKUP"] = ["column","lookupTable","dataframe","tableObject","testNumber"]
ruleParameterUsage["RF12_ASSERT_CONDITIONAL_NULL_LOOKUP"] = None
def RF12_ASSERT_CONDITIONAL_NULL_LOOKUP(column,lookupTable,dataframe,tableObject,testNumber):
  '''
  RF12_ASSERT_CONDITIONAL_NULL_LOOKUP(column::<STRING>,lookupTable::<STRING>,dataframe::<Dataframe Object>,
                                      tableObject::<Table Object>,testNumber::<INT>)
  
    This is a reference quality check intended to check that, based on a value in another field, a given
    value in the 'column' is null. This check uses a lookup table. Note that this check is
    not a UDF itself.
    
    Expected usage in the RuleDefinitions notebook:
    
      >> "RF12_ASSERT_CONDITIONAL_NULL_LOOKUP('<column_name>','<schema>.<table>')"
    
    Arguments:
    
      column      - The string representing the column that should be checked.
      lookupTable - The string representing the "<schema>.<table>" of the lookup table.
      dataframe   - The Dataframe object that contains the table being tested.
      tableObject - The Table object that contains all of the table's metadata.
      testNumber  - The index of the current test for column naming purposes.
      
    Returns:
    
      dataframe   - The Dataframe object with the new column that reports test failures added
                    to the end.
  
  '''
  global lookupTableObjects
  
  @udf(BooleanType())
  def innerUDF(value,relatedValue,relatedFieldString):
    if relatedFieldString is None:
      return True
    relatedFieldList = [item.strip() for item in relatedFieldString.split(",")]
    relatedValue = str(relatedValue)
    failed = relatedValue in relatedFieldList and not value is None
    return not failed

  @udf(BooleanType())
  def joinUDF(value,listString):
    return str(value) in [item.strip() for item in listString.split(",")]
  
  if lookupTable in lookupTableObjects:
    lookupTableDataframe = lookupTableObjects[lookupTable]
  else:
    lookupTableArgs = {
      "schema" : lookupTable.split(".")[0],
      "table"  : lookupTable.split(".")[1],
      "credentials" : tableObject.meta["credentials"],
      "source" : "redshift",
      "environment" : tableObject.meta["environment"],
      "s3Directory" : tableObject.meta["s3Directory"]
    }
    lookupTableDataframe = Table(lookupTableArgs).getDataframeFromRedshift().filter(col("field_name") == column)
    lookupTableObjects[lookupTable] = lookupTableDataframe    
  collected = lookupTableDataframe.collect()
  relatedField,relatedFieldString = collected[0]["related_field"],collected[0]["related_field_value"]
  testName = "test"+str(testNumber)
  dataframe = (dataframe
               .join(lookupTableDataframe,col("related_field_value").contains(col(relatedField)),"leftouter")
               .withColumn(testName,when(innerUDF(column,"related_field","related_field_value"),"").otherwise("RF12_ASSERT_CONDITIONAL_NULL"))
               .drop("field_name")
               .drop("related_field_value")
               .drop("related_field")
              )
  return dataframe
  

# COMMAND ----------

# Reference Quality Check 13 ::
# This is a reference data quality check intended to check that all of the values in a set
# of columns are aligned across another set of columns. See docstring for more info.
ruleParameterOrder["RF13_ASSERT_ALIGNMENT_ACROSS_FIELDS"] = ["primaryColumns","alignedColumns","dataframe","tableObject","testNumber"]
ruleParameterUsage["RF13_ASSERT_ALIGNMENT_ACROSS_FIELDS"] = None
def RF13_ASSERT_ALIGNMENT_ACROSS_FIELDS(primaryColumns,alignedColumns,dataframe,tableObject,testNumber):
  '''
  RF13_ASSERT_ALIGNMENT_ACROSS_FIELDS(primaryColumns::LIST[<STRING>],alignedColumns::LIST[<STRING>],
                                      dataframe::<Dataframe Object>,tableObject::<Table Object>,testNumber::<INT>)
  
    This is a reference data quality check intended to check that the values within a list of
    columns are aligned based on a list of primary grouping columns. To further explain this
    please refer to the examples below. Note that this function is not a UDF itself.
    
    This case would fail because the 'alignedColumns' are not always the same for the unique set of
    'primaryColumns'...
    +------------------+------------------+------------------+------------------+---------------+
    | primary_column_a | primary_column_b | aligned_column_a | aligned_column_b | other_columns |
    +------------------+------------------+------------------+------------------+---------------+
    | "abc"            | "hello world"    | 12               | 42               | ...           |
    | "abc"            | "hello world"    | 12               | 42               | ...           |
    | "def"            | "hello world"    | 17               | 20               | ...           | <---- These rows cause
    | "def"            | "hello world"    | 10               | 20               | ...           | <---- the failure
    +------------------+------------------+------------------+------------------+---------------+
  
    Whereas, this case would pass because all of the fields stay aligned...
    +------------------+------------------+------------------+------------------+---------------+
    | primary_column_a | primary_column_b | aligned_column_a | aligned_column_b | other_columns |
    +------------------+------------------+------------------+------------------+---------------+
    | "abc"            | "hello world"    | 12               | 42               | ...           |
    | "abc"            | "hello world"    | 12               | 42               | ...           |
    | "def"            | "hello world"    | 17               | 20               | ...           |
    | "def"            | "hello world"    | 17               | 20               | ...           |
    | "abc"            | "sup world"      | 0                | 87               | ...           |
    +------------------+------------------+------------------+------------------+---------------+
    
    Expected usage in the RuleDefinitions notebook:
    
      >> "RF13_ASSERT_ALIGNMENT_ACROSS_FIELDS(['<column1>','<column2>',...],['<column1>','<column2>',...])"
    
    Arguments:
    
      primaryColumns - A list of string column names that form the primary keys for which the
                       fields should align.
      alignedColumns - A list of string column names that are the set of columns that should
                       be aligned based on the key given.
      dataframe      - The Dataframe object that is being tested, and will eventually be returned.
      tableObject    - The Table object that is associated with this dataframe in case any of
                       the table's metadata is desired (especially the credentials).
      testNumber     - The current index of the test being performed for column naming purposes.
    
    Returns:
    
      dataframe      - The Dataframe object with the new column that reports test failures added
                       to the end.
  
  '''
  testName = "test"+str(testNumber)
  columnsInSubset = tuple(primaryColumns+alignedColumns)
  subsetDataframe = dataframe.select(*columnsInSubset).withColumn("rf_13_rank",dense_rank().over(Window.partitionBy(*primaryColumns).orderBy(*columnsInSubset)))
  dataframe = dataframe.join(subsetDataframe,list(columnsInSubset),"leftOuter").withColumn(testName,when(col("rf_13_rank") == 1,"").otherwise("RF13_ASSERT_ALIGNMENT_ACROSS_FIELDS"+str(columnsInSubset))).drop("rf_13_rank")
  return dataframe

# COMMAND ----------

# This is a reference data quality check intended to check that the values within a column
# that are strings do not have any trailing or leading whitespace in them.
ruleParameterOrder["RF14_ASSERT_TRIMMED_WHITESPACE"] = ["columnName"]
ruleParameterUsage["RF14_ASSERT_TRIMMED_WHITESPACE"] = {"columnName":"column"}
@udf(StringType())
def RF14_ASSERT_TRIMMED_WHITESPACE(value):
  '''
  RF14_ASSERT_TRIMMED_WHITESPACE(value::<STRING>)
    
    This is a reference data quality check intended to check that the values within a column
    that are strings do not have any trailing or leading whitespace in them. Note that any
    non-string inputs will be cast to a string before testing. This should not cause any
    unforseen false positives because casting to a string generally does not have any extra
    whitespace in it.
    
    Expected usage in the RuleDefinitions notebook:
    
      >> "RF14_ASSERT_TRIMMED_WHITESPACE('<column_name>')"

    Arguments:

      value - The string that should be tested, this will be the column under test. Note that
              this function ignores all nulls; null values will pass this test.

    Returns:

      status - If the test passes then it will return "", if it fails then it will return the
               name of the test, which in this case is "RF14_ASSERT_TRIMMED_WHITESPACE(value)"
               with the parameters that were passed in.
  '''
  thisFunctionName = sys._getframe().f_code.co_name
  value = str(value)
  # perform the rule
  if not value.strip() == value:
    return thisFunctionName+"('{}')".format(value)
  return ""

# COMMAND ----------

# Taken from the enrichment workflow directly
consolidateErrorColumns = udf(lambda *args: ','.join([i for i in args if i is not None and i.strip() != ""]), StringType())

# COMMAND ----------

@udf(StringType())
def getType(value):
  return str(type(value))