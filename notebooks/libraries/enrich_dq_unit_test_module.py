# Databricks notebook source
# -*- coding: UTF-8 -*-
environment = "DEV"

# COMMAND ----------

# MAGIC %run ./enrich_dq_module

# COMMAND ----------

def check_test_results(df):
  x = df.withColumn("result", 
                    F.when((F.col("expected").isNull()) & (F.col("actual").isNull()), "PASSED")
                    .when(F.col("expected") == F.col("actual"), "PASSED")
                    .otherwise("FAILED")
                   )
  failed_df = x.filter(F.col("result") == "FAILED")
  passed_df = x.filter(F.col("result") == "PASSED")
  if failed_df.count() > 0:
    print "FAILED TESTS: "
    failed_df.show(100, False)
  print "PASSED TESTS: " + str(passed_df.count()) + '\n'

# COMMAND ----------

#DQ1

print "DQ1: check all invalid cases"
data = [("","DQ1"),(None,"DQ1"),("0000000000","DQ1"),("CN42CAT10B.","DQ1"),("00*0000000","DQ1"),("0 000000 ","DQ1"),('0"000000',"DQ1")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq1_sn(F.col("col1")))
)

print "DQ1: check valid cases even some that shouldn't be valid"
data = [("","DQ1"),("MIMI-THE-CAT",None),("0000000010",None),("CN42CAT10R#ABA",None),("BADBADANDBAD",None),("00000000000",None),('SN_GOES_HERE',None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq1_sn(F.col("col1")))
)

# COMMAND ----------


#DM4 - flagging

print "DM4: checking valid and invalid cases"
data = [("asdfghjklmzx","DM4"),("qwertyuiopa","DM4"),("awsedrftgh",None),("zsxdcfvgb",None),(None,None),("",None)]
x = spark.createDataFrame(data, ["col1","expected"])

check_test_results(
  x.withColumn("actual", udf_dm4_sn(x.col1)))




# COMMAND ----------

#DM4 - modding

print "DM4: checking valid & invalid cases"
data = [("asdfghjklmzx","asdfghjklm"),("qwertyuiopa","qwertyuiop"),("awsedrftgh","awsedrftgh"),("zsxdcfvgb","zsxdcfvgb"),(None,None),("","")]
x = spark.createDataFrame(data, ["col1","expected"])


check_test_results(
  x.withColumn("actual", udf_mod_dm4_sn(x.col1)))

# COMMAND ----------

b_business_patterns = sc.broadcast(map(lambda x: x.replace('*', '').upper(), get_business_patterns(environment)))

# DM17_hash - flagging
print "DM17: checking correct finding and modifying values as individual"
data = [("NON ASCII©",0,"DM17_col1"),("BOB BUYS MANY PRINTERS",1000,None),("BOB'S ADDRESS",999,"DM17_col1"),("ISAAC'S MEGALOPOLIS",1001,None),("BUY'S NEGATIVE PRINTERS",-1,"DM17_col1")]
x = spark.createDataFrame(data, ["channel","channel_ship_cnt","expected"])
check_test_results(
  x.withColumn("actual", udf_dm17_individual(F.col("channel"), F.col("channel_ship_cnt"), lit(1000), lit("col1")))
)

print "DM17: checking for names that are in the known business patterns"
# First test case is to let databricks determine the type as string
data = [("US",800,"DM17_col1"),("PARIS CITY ISAAC",90,None),("USAF",800,None),("MIMI LLC",10,None),("IKEA OR",1,None)]
x = spark.createDataFrame(data, ["channel","channel_ship_cnt","expected"])
check_test_results(
  x.withColumn("actual", udf_dm17_individual(F.col("channel"), F.col("channel_ship_cnt"), lit(1000), lit("col1")))
)

# COMMAND ----------

b_business_patterns = sc.broadcast(map(lambda x: x.replace('*', '').upper(), get_business_patterns(environment)))

# DM17_hash - modding
print "DM17: checking correct finding and modifying values as individual"
data = [("NON ASCII©",0,"f6b28a8d8990d8c4bb1e6df32827eefdb25ffc934ca361ae92578b65b3c35493"),("BOB BUYS MANY PRINTERS",1000,"BOB BUYS MANY PRINTERS"),("BOB'S ADDRESS",999,"6bbe5e4492a5f2b8265a2b37620f31d82c5837299ed5caa133c7f25749f330d1"),("ISAAC'S MEGALOPOLIS",1001,"ISAAC'S MEGALOPOLIS"),("BUY'S NEGATIVE PRINTERS",-1,"481e7b6814ac665e006a17ae939b9154043d4c88a01a0930208f8cae4573fa9a")]
x = spark.createDataFrame(data, ["channel","channel_ship_cnt","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm17_individual_hash(F.col("channel"), F.col("channel_ship_cnt"), lit(1000), F.col("channel")))
)

print "DM17: checking for names that are in the known business patterns"
data = [("PARIS CITY ISAAC",90,"PARIS CITY ISAAC"),("USAF",800,"USAF"),("MIMI LLC",10,"MIMI LLC"),("IKEA OR",1,"IKEA OR")]
x = spark.createDataFrame(data, ["channel","channel_ship_cnt","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm17_individual_hash(F.col("channel"), F.col("channel_ship_cnt"), lit(1000), F.col("channel")))
)

# COMMAND ----------

b_business_patterns = sc.broadcast(map(lambda x: x.replace('*', '').upper(), get_business_patterns(environment)))

# DM17 hash_if_marked - modding
print "DM17: checking correct finding and modifying values as individual when using hash if marked mod"
data = [("NO CHANCE",0,"4186fe22964d9fdadd835a14b184ae092bf72834da08fe7eb5d9d8a32cb3837f",),("BOB BUYS MANY PRINTERS",1000,"BOB BUYS MANY PRINTERS",),("BOB'S ADDRESS",999,"6bbe5e4492a5f2b8265a2b37620f31d82c5837299ed5caa133c7f25749f330d1",),("ISAAC'S MEGALOPOLIS",1001,"ISAAC'S MEGALOPOLIS"),("CITY OF ISAAC",900,"CITY OF ISAAC"),("BUY'S NEGATIVE PRINTERS",-1,"481e7b6814ac665e006a17ae939b9154043d4c88a01a0930208f8cae4573fa9a",) ]
x = spark.createDataFrame(data, ["channel","channel_ship_cnt","expected"])
x = x.withColumn("check_dm17_channel", udf_dm17_individual(F.col("channel"), F.col("channel_ship_cnt"), lit(1000), F.lit("channel")))
check_test_results(
  x.withColumn("actual", udf_mod_dm17_individual_hash_if_marked(F.col("check_dm17_channel"), F.col("channel")))
)

# COMMAND ----------

#DM24 - flagging

b_invalid_chars_regex = sc.broadcast(invalid_chars_regex)
print "DM24: checking all invalid chacters and one good one"
data_all_invalid = [(u"“", "DM24"), ("%", "DM24"), (u"ˆ", "DM24"), (u"‰", "DM24"), ("", "DM24"), (u"™", "DM24"), ("", "DM24"), ("»", "DM24"), ("¡", "DM24"), ("­", "DM24"), ("®", "DM24"), (u"‚", "DM24"), (u"”", "DM24"), ("©", "DM24"), ("«", "DM24"), ("¶", "DM24"), ("±", "DM24"), ("²", "DM24"), ("³", "DM24"), ("¼", "DM24"), ("½", "DM24"), ("¾", "DM24"), ("¿", "DM24"), ("¹", "DM24"), ("|", "DM24"), (u"ª", "DM24"), (u"º", "DM24"),("KEVIN",None)]
x = spark.createDataFrame(data_all_invalid, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dm24_invalid_char(F.col('col1')))
)

print "DM24: cases where the invalid characters are mixed in with good ones - DM24_{column_name}"
data = [(u"Brªndºn™M","DM24_col1"),(u"Kevin©","DM24_col1"),(u"½Mimi","DM24_col1"),(u"¡sªª©","DM24_col1"),(u" THE» RYAN","DM24_col1")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dm24_invalid_char(F.col('col1'), lit('col1')))
)

print "DM24 valid cases and one invalid"
# First test case is to let databricks determine the type as string
data = [("Brandon%","DM24"),("Kevin",None),("Mimi",None),("ISAAC",None),("THE RYAN",None),("1",None), ("",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dm24_invalid_char(F.col('col1')))
)

# COMMAND ----------

#DM24 - modding

print "DM24: checking all invalid chacters and one god one"
data_all_invalid = [("KEVIN","KEVIN"),(u"“", None), ("%", None), (u"ˆ", None), (u"‰", None), ("", None), (u"™", None), ("", None), ("»", None), ("¡", None), ("­", None), ("®", None), (u"‚", None), (u"”", None), ("©", None), ("«", None), ("¶", None), ("±", None), ("²", None), ("³", None), ("¼", None), ("½", None), ("¾", None), ("¿", None), ("¹", None), ("|", None), (u"ª", None), (u"º", None)]
x = spark.createDataFrame(data_all_invalid, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm24_invalid_char(F.col('col1')))
)

print "DM24: cases where the invalid characters are mixed in with good ones and one valid"
# First test case is to let databricks determine the type as string
data = [("Justin","Justin"),(u"Brªndºn™M",None),(u"Kevin©",None),(u"½Mimi",None),(u"¡sªª©",None),(u" THE» RYAN",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm24_invalid_char(F.col('col1')))
)

print "DM24 valid cases"
data = [("Kevin","Kevin"),("Mimi","Mimi"),("ISAAC","ISAAC"),("THE RYAN","THE RYAN"),("1","1"), ("","")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm24_invalid_char(F.col('col1')))
)

# COMMAND ----------

#DM27 - flagging

print "DM27: checking single length acceptance"
data = [("1","DM27_col1",),("11",None,),("111","DM27_col1",)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dm27_len(F.col("col1"), 2, lit('col1')))
)

print "DM27: checking boundary length acceptance"
data = [("1","DM27_col1"),("11",None),("11111",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dm27_len(F.col("col1"), 2, lit('col1'), 5))
)

print "DM27: checking if field is not supplied"
data = [("1","DM27"),("11",None),("1111",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dm27_len(F.col("col1"), 2, max_length=4))
)

# COMMAND ----------

#DM27 - modding

print "DM27: checking single length acceptance"
data = [("1",None,),("11","11",),("111",None,)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm27_len(F.col("col1"), 2, None))
)

print "DM27: checking boundary length acceptance"
data = [("1",None),("11","11"),("11111","11111")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm27_len(F.col("col1"), 2, 5))
)

# COMMAND ----------

#DQ35 - flagging

print "DQ35: regex_check on serial_number"
data = [("CN42A1050KM",None),(" ","DQ35_col1"),(".","DQ35_col1"),("*","DQ35_col1"),("\"","DQ35_col1")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq35_regex_chk(F.col("col1"), lit('col1'), '^[0-9A-Z]+$'))
)

print "DQ35: regex_check special cases not caught that DQ1 will catch"
# first item is to give clue about data type
data = [(" ","DQ35_col1"),("0000000000",None),("XXXXXXXXXX",None),("",None),(None,None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq35_regex_chk(F.col("col1"), lit('col1'), '^[0-9A-Z]+$'))
)

# Todo: write tests for other fields that use DQ35

# COMMAND ----------

#DM43 - flagging
replacements = ['Y','N']

print "DM43: checking valid and invalid cases"
data = [("Y","DM43_col1"),("N","DM43_col1"),("a","DM43_col1"),("YN","DM43_col1"),(None,None),("",None)]
x = spark.createDataFrame(data, ["col1","expected"])

check_test_results(
  x.withColumn("actual", step_dm43_decode(x.col1, replacements, lit("col1")))
)

print "DM43: checking valid and invalid cases for pass through version"
data = [("Y","DM43_col1"),("N","DM43_col1"),("a",None),("YN",None),(None,None),("", None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm43_decode_pass_through(x.col1, replacements, lit("col1")))
)

# COMMAND ----------

#DM43 - modding
replacements = [('Y', 'VALUE'), ('N', 'VOLUME')]

print "DM43: checking valid & invalid cases"
data = [("Y","VALUE"),("N","VOLUME"),("a",None),("YN",None),(None,None)]
x = spark.createDataFrame(data, ["col1","expected"])

#need to create 'actual' from 'col1' since dm43 drops 'field'
check_test_results(
  udf_mod_dm43_decode(x, replacements, "col1").withColumn("actual", F.col("col1"))
)

print "DM43: checking containing only the 'N' to verify that the join doesn't bring in the missing item ('Y')"
data = [("N","VOLUME"),("YES",None)]
x = spark.createDataFrame(data, ["col1","expected"])

#need to create 'actual' from 'col1' since dm43 drops 'field'
check_test_results(
  udf_mod_dm43_decode(x, replacements, "col1").withColumn("actual", F.col("col1"))
)

# COMMAND ----------

# DM43 - modding (pass through version)
replacements = [('Y', 'VALUE'), ('N', 'VOLUME')]

print "DM43: checking valid & invalid cases"
data = [("Y", "VALUE"), ("N", "VOLUME"), ("y", "y"), ("YN", "YN"), ("FRED", "FRED"), ("BARNEY", "BARNEY"), (None, None)]
x = spark.createDataFrame(data, ["col1", "expected"])

# Create a copy of col1 so the DM can modify the "actual" column
x = x.withColumn('actual', x.col1)
check_test_results(
  udf_mod_dm43_decode_pass_through(x, replacements, "actual")  # .withColumn("actual", F.col("col1"))
)

# COMMAND ----------

#DM44 flagging

invalid_vals1 = ['NA', 'N/A', '#N/A', 'ALL', 'ONLINE SALES', 'CHANNEL SALES', 'CUSTOMER NOT SPECIFIED', 'END USER', 'ENDUSER', 'CUSTOMER', 'PHYSICAL SALES', 'ANONYMOUS', 'CASH CUSTOMER', 'NONAME', 'UNKNOWN STREET', 'CITY', 'DIRECT PHYSICAL', 'CONTRACT ONLINE', 'RETAIL PHYSICAL', 'ONLINE', '(CIDADE/PROVINCIA)', 'BLANK', 'DO NOT CONTACT', 'INVOICE', 'X']

print "DM44: checking some invalid values"
data = [("NA","DM44_col1"),("N/A","DM44_col1"),("#N/A","DM44_col1"),("CHANNEL SALES","DM44_col1"),("CUSTOMER NOT SPECIFIED", "DM44_col1"), ("END USER", "DM44_col1"), ("CUSTOMER", "DM44_col1"), ("PHYSICAL SALES", "DM44_col1"), ("CASH CUSTOMER", "DM44_col1"), ("X", "DM44_col1")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm44_list_null_chk(F.col("col1"), lit("col1"), invalid_vals1))
)

print "DM44: checking good values that contain some or all of an invalid value"
data = [("NA","DM44_col1"),("ALL 4 KEVIN",None),("SALES ONLINE R US",None),("WINDY CITY",None), ("(CIDADE/PROVINCIA) LLC", None), ("CASH KING", None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm44_list_null_chk(F.col("col1"), lit("col1"), invalid_vals1))
)

# COMMAND ----------

#DM44 - modding

def escape_regex(val):
  return '|'.join(['^'+re.escape(i)+'$' for i in val])

invalid_vals1 = escape_regex(["*"])
invalid_vals2 = escape_regex(['NA', 'N/A', '#N/A', 'ALL', 'ONLINE SALES', 'CHANNEL SALES', 'CUSTOMER NOT SPECIFIED', 'END USER', 'ENDUSER', 'CUSTOMER', 'PHYSICAL SALES', 'ANONYMOUS', 'CASH CUSTOMER', 'NONAME', 'UNKNOWN STREET', 'CITY', 'DIRECT PHYSICAL', 'CONTRACT ONLINE', 'RETAIL PHYSICAL', 'ONLINE', '(CIDADE/PROVINCIA)', 'BLANK', 'DO NOT CONTACT', 'INVOICE', 'X'])

print "DM44: checking some invalid values"
data = [("KEVIN","KEVIN"),("NA",None),("N/A",None),("#N/A",None),("CHANNEL SALES",None),("CUSTOMER NOT SPECIFIED", None), ("END USER", None), ("CUSTOMER", None), ("PHYSICAL SALES", None), ("CASH CUSTOMER", None), ("X", None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm44_list_null_chk(F.col("col1"),invalid_vals2))
)

print "DM44: checking good values that contain some or all of an invalid value"
data = [("MIMIX", "MIMIX"), ("ALL 4 KEVIN", "ALL 4 KEVIN"), ("SALES ONLINE R US", "SALES ONLINE R US"), ("WINDY CITY", "WINDY CITY"), ("(CIDADE/PROVINCIA) LLC", "(CIDADE/PROVINCIA) LLC"), ("CASH KING", "CASH KING"), ("ALLCITY", "ALLCITY")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm44_list_null_chk(F.col("col1"), invalid_vals2))
)

print "DM44: checking good values that contain an invalid value when invalid list is one item"
data = [("WILDCARD STUDI*S", "WILDCARD STUDI*S")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm44_list_null_chk(F.col("col1"), invalid_vals1))
)

# COMMAND ----------

#DM50 - flagging

col = ['col1']
contain_vals = ["YES", "NO", "DUM"]

print "DM50: checking replacements specific to hermes-pdb (same column multiple replacements)"
data = [("ISAAC","ISAAC"),("YES","DM50_col1"),("NO","DM50_col1"),("DUMMY","DM50_col1"),("ISAAC YES","DM50_col1")]
x = spark.createDataFrame(data, ["col1","expected"])

#x = x.withColumn("actual", F.col("col1"))
x = x.withColumn("actual", F.lit(None))
for i in col:
  for j in contain_vals:
    x = x.withColumn("actual", step_dm50_contains(F.col(i), F.lit(i), j, F.col("actual")))
#check_test_results(x)
x.show()

# COMMAND ----------

#DM50 - modding

replacements = {
  'DUMM':None,
  'BYPAS':None,
  'LUANDA':None}

print "DM50: checking valid & invalid cases"
data = [("DUMM",None),("BYPAS",None),("LUANDA",None),("DUMMY",None),(None,None),("HELLO","HELLO")]
x = spark.createDataFrame(data, ["col1","expected"])

x = x.withColumn("actual", F.col("col1"))
for i in replacements.items():
  x = x.withColumn("actual", udf_mod_dm50_contains(F.col("actual"), i[1], i[0]))
check_test_results(x)

replacements = {
  'HP':'HP',
  'COMMERCIAL DIST':'COMMERCIAL DISTRIBUTOR',
  'CONSUMER DIST':'CONSUMER DISTRIBUTOR',
  'FULL LINE DIST':'FULL LINE DISTRIBUTOR'
}
print "DM50: checking replacements specific to GCW AMS"
data = [("COMMERCIAL DIST","COMMERCIAL DISTRIBUTOR"),("CONSUMER DIST","CONSUMER DISTRIBUTOR"),("FULL LINE DIST","FULL LINE DISTRIBUTOR"),("FULL LINE DIST##############","FULL LINE DISTRIBUTOR"),("###############FULL LINE DIST########","FULL LINE DISTRIBUTOR"),("HP INC","HP"), ("FULL 123LINE DIST", "FULL 123LINE DIST")]
x = spark.createDataFrame(data, ["col1","expected"])

x = x.withColumn("actual", F.col("col1"))
for i in replacements.items():
  x = x.withColumn("actual", udf_mod_dm50_contains(F.col("actual"), i[1], i[0]))
check_test_results(x)

replacements = {
  'YES':'Y',
  'NO':'N',
  'DUM':None
}
print "DM50: checking replacements specific to hermes-pdb (same column multiple replacements)"
data = [("ISAAC","ISAAC"),("YES","Y"),("NO","N"),("DUMMY",None),("ISAAC YES","Y")]
x = spark.createDataFrame(data, ["col1","expected"])

x = x.withColumn("actual", F.col("col1"))
for i in replacements.items():
  x = x.withColumn("actual", udf_mod_dm50_contains(F.col("actual"), i[1], i[0]))
check_test_results(x)

# COMMAND ----------

#DM58 - flagging

print "DM58: checking removing quotes"
data = [('"hello','DM58_col1'),('hello"','DM58_col1'),('hello',None),('""""hello"""""','DM58_col1'),('"""""""""','DM58_col1'),('super "cool" Co.',None),(None,None),('', None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm58_quotes(F.col("col1"), lit("col1")))
)

# COMMAND ----------

#DM53_hermes - flagging

#format of hermes-pdb dates - pass in max_date as +10 years
print "DM53: checking valid & invalid cases for hermes format with max date passed in"
data = [("21-JUL-10",None),("01-JAN-06",None),("01-JAN-26",None),("01-JAN-96",None),("01-JAN-80",None),("20-JUN-29","DM53_col1"),("01-JAN-79","DM53_col1")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm53_date_hermes(F.col("col1"), F.lit("col1"), F.lit("1980-01-01"), F.lit(datetime(year=datetime.now().year+10, month=datetime.now().month, day=datetime.now().day).strftime("%Y-%m-%d"))))
)

#don't pass in max_date -although this is somewhat redundant because hermes will always use a max date
print "DM53: checking valid & invalid cases for hermes date format without max date"
data = [("21-JUL-10",None),("01-JAN-06",None),("01-JAN-26","DM53_col1"),("01-JAN-96",None),("01-JAN-80",None),("20-JUN-29","DM53_col1"),("01-JAN-79","DM53_col1")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm53_date_hermes(F.col("col1"), F.lit("col1"), F.lit("1980-01-01")))
)

# COMMAND ----------

#DM53_hermes - modding

#format of hermes-pdb dates - pass in max_date as +10 years
print "DM53: checking valid & invalid cases for hermes format with max date passed in"
data = [("21-JUL-10","21-JUL-10"),("01-JAN-70",None),("01-JAN-30",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm53_date_hermes(F.col("col1"), F.lit("1980-01-01"), F.lit(datetime(year=datetime.now().year+10, month=datetime.now().month, day=datetime.now().day).strftime("%Y-%m-%d"))))
)

# COMMAND ----------

#DM58 - modding

print "DM58: checking removing quotes"
data = [('"hello','hello'),('hello"','hello'),('hello','hello'),('""""hello"""""','hello'),('"""""""""',''),('super "cool" Co.','super "cool" Co.'),(None,None),('','')]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm58_quotes(F.col("col1")))
)

# COMMAND ----------

#DM61 - flagging

print "DM61: checking valid & invalid cases"
data = [("HELLO",None),("123",None),("-/",None),("hHello","DM61_col1"),("11/$","DM61_col1")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm61_translate(F.col("col1"), '^[A-Z0-9-/]+$', lit('col1')))
)

# COMMAND ----------

#DM61 - modding

print "DM61: checking valid & invalid cases"
data = [("HELLO","HELLO"),("123","123"),("-/","-/"),("hHello",None),("11/$",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm61_translate(F.col("col1"), '^[A-Z0-9-/]+$'))
)

# COMMAND ----------

#DM62 - flagging

print "DM62: checking valid & invalid cases for 0s"
data = [("000000000000","DM62_col1"),("0","DM62_col1"),("010",None),("0000hello0000",None),("1",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm62_complement_translate(F.col("col1"), '^[0]+$', lit('col1')))
)

print "DM62: checking valid & invalid cases for . and spaces"
data = [(" . . . . .","DM62_col1"),("\t",None),(" ","DM62_col1"),(".","DM62_col1"), ("......","DM62_col1"),("   h.",None),("h",None),("hello",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm62_complement_translate(F.col("col1"), '^[. ]+$', lit('col1')))
)

# COMMAND ----------

#DM62 - modding

print "DM62: checking valid & invalid cases for 0s"
data = [("000000000000",None),("0",None),("010","010"),("0000hello0000","0000hello0000"),("1","1")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm62_complement_translate(F.col("col1"), '^[0]+$'))
)

print "DM62: checking valid & invalid cases for . and spaces"
data = [(" . . . . .",None),("\t","\t"),(" ",None),(".",None), ("......",None),("   h.","   h."),("h","h"),("hello","hello")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm62_complement_translate(F.col("col1"), '^[. ]+$'))
)

# COMMAND ----------

#DM63 - flagging
#["“", "%", "ˆ", "‰", "", "™", "", "»", "¡", "­", "®", "‚", "”", "©", "«", "¶", "±", "²", "³", "¼", "½", "¾", "¿", "¹", "|"]
print "DM63: checking valid & invalid cases for non printable characters"
data = [("""000000000000""","DM63_col1"),("""""","DM63_col1"),("""©""","DM63_col1"),("¿¿¿","DM63_col1"),("1",None), (None,None), ("",None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", step_dm63_nonprint_chars(F.col("col1"), lit('col1')))
)

# COMMAND ----------

#DM63 - modding

print "DM63: checking valid & invalid cases for removing non printable characters"
data = [("""000000000000""","000000000000"),("""""",""),("""©""",""),("¿¿¿",""),("1","1"),("""hello©""","hello"), (None,None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm63_nonprint_chars(F.col("col1")))
)

# COMMAND ----------

#DM64 - flagging
print "DM64: checking valid & invalid cases for negative numbers"
data = [("-500","DM64_col1"),("-.01","DM64_col1"),("0",None),("12.24",None),("500",None)]
                     
x = spark.createDataFrame(data, ["col1", "expected"])
x = x.withColumn("col1", F.col("col1").cast(DoubleType()))

check_test_results(
  x.withColumn("actual", step_dm64_negatives(F.col("col1"), lit('col1')))
)

# COMMAND ----------

#DM64 - modding
print "DM64: checking valid & invalid cases for negative numbers"
data = [("-500",None),("-.01",None),("0","0"),("12.24","12.24"),("500","500")]
                     
x = spark.createDataFrame(data, ["col1", "expected"])
x = x.withColumn("col1", F.col("col1").cast(DoubleType()))

check_test_results(
  x.withColumn("actual", udf_mod_dm64_negatives(F.col("col1")))
)

# COMMAND ----------

#DM65 - flagging

print "DM65: checking valid & invalid cases for strings that start with a 'ZZ'"
data = [("ZZHELLO","DM65_col1"),("HELLOZZ",None),("ZHELLO",None),("Z ZHELLO",None)]
                     
x = spark.createDataFrame(data, ["col1", "expected"])
check_test_results(
  x.withColumn("actual", step_dm65_startswith(F.col("col1"), "ZZ", lit('col1')))
)

print "DM65: checking valid & invalid cases for strings that start with an 'INDSIDE'"
data = [("INDSIDEHELLO","DM65_col1"),("HELLOINDSIDE",None),("HELINDSIDELO",None),(" INDSIDE",None)]
x = spark.createDataFrame(data, ["col1", "expected"])
check_test_results(
  x.withColumn("actual", step_dm65_startswith(F.col("col1"), "INDSIDE", lit('col1')))
)

# COMMAND ----------

#DM65 - modding

print "DM65: checking valid & invalid cases for strings that start with a 'ZZ'"
data = [("ZZHELLO",None),("HELLOZZ","HELLOZZ"),("ZHELLO","ZHELLO"),("Z ZHELLO","Z ZHELLO")]
                     
x = spark.createDataFrame(data, ["col1", "expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm65_startswith(F.col("col1"), "ZZ"))
)

print "DM65: checking valid & invalid cases for strings that start with an 'INDSIDE'"
data = [("INDSIDEHELLO",None),("HELLOINDSIDE","HELLOINDSIDE"),("HELINDSIDELO","HELINDSIDELO"),(" INDSIDE"," INDSIDE")]
x = spark.createDataFrame(data, ["col1", "expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm65_startswith(F.col("col1"), "INDSIDE"))
)

# COMMAND ----------

# DQ998

b_sn_blacklist = sc.broadcast([("TH7244Y1C1","E4W44A"),("TH54L6V0XW","F8B04A"),("TH88R3S097","P4C85A"),("CN7B64D0Z8","T8X10B"),("","A9T80A"),(None,None)])
print "DQ998: checking valid & invalid cases"
data = [("CN7B64D0Z8","T8X10B","DQ998_serial_number"),("CN55T352N8","T8X1",None),("","A9T80A",None),(None,None,None), ("","", None)]

x = spark.createDataFrame(data, ["serial_number", "product_number", "expected"])
check_test_results(
  x.withColumn("actual", udf_step_dq998_sn(F.col("serial_number"), F.col("product_number")))
)

# COMMAND ----------

# DQ999

b_cid_blacklist = sc.broadcast(["1111003425862778","1111003509407728","NO RECORDS IN CID","1111003690626799","1111003626578256","",None])
print "DQ999: checking valid & invalid cases"
data = [("1111003690626799","DQ999_cid"),("CN55T352N8",None),("",None),(None,None)]

x = spark.createDataFrame(data, ["cid", "expected"])
check_test_results(
  x.withColumn("actual", udf_step_dq999_cid(F.col("cid")))
)

# COMMAND ----------

# DQ 70

print "DQ70: check all invalid cases"
data = [("","DQ70"),(None,"DQ70")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq70_nullcheck(F.col("col1")))
)

print "DQ70: check valid cases even some that shouldn't be valid"
data = [("","DQ70"),("MIMI-THE-CAT",None),("0000000010",None),("CN42CAT10R#ABA",None),("BADBADANDBAD",None),("00000000000",None),('SN_GOES_HERE',None)]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq70_nullcheck(F.col("col1")))
)

# COMMAND ----------

# DQ66 - returns a DQ when column is non-numeric
print "DQ66: returns a DQ when column is non-numeric"
data = [("abcd","DQ66"),("  ","DQ66")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq66_numeric(F.col("col1")))
)

print "DQ66: returns a DQ when column is non-numeric"
data = [("123456",None),("00000",None),("0000000010",None),("r56tyu","DQ66")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq66_numeric(F.col("col1")))
)
###########################################################################################################
###                        Unit Testing for the Function DM60 (Marking)                                 ###
###########################################################################################################
#b_business_patterns = sc.broadcast(map(lambda x: x.replace('*', '').upper(), get_business_patterns(environment)))
print "DM60: checking Addresses not directly associated with an individual’s name."
data = [("SGUPTA","SGUPTA","Bangalore","Bangalore",None),("SGUPTA","SGUPTA","Bangalore","Delhi",None),\
        ("8691dd14cb5e57c529007fa104d40864b8fcc140ff516ed6e2b2be3d8334a1d9","Sujith","Bangalore","Bangalore","DM60_col1"),\
        ("8691dd14cb5e57c529007fa104d40864b8fcc140ff516ed6e2b2be3d8334a1d9","Sujith","Bangalore","Delhi",None),\
        ("Sujith","Sujith","Bangalore","Bangalore",None)]
x = spark.createDataFrame(data, ["end_user_customer_name","end_user_customer_name_orig","end_user_address1_orig","bill_to_address1_orig","expected"])
check_test_results(
  x.withColumn("actual", step_dm60_hash(F.col("end_user_customer_name"), F.col("end_user_customer_name_orig"), \
                                        F.col("end_user_address1_orig"), F.col("bill_to_address1_orig"), lit("col1")))
              )

# COMMAND ----------

###########################################################################################################
###                        Unit Testing for the Function DM60(Modifying)                                 ###
###########################################################################################################
print "DM60: Modding Addresses not directly associated with an individual’s name."
data = [("5f73ff5596261fabd63b2c36495b7370f4e9fcee8a97df323ae8a36f13f8cbdf","SGUPTA","Bangalore","Bangalore","Bangalore",\
                                                         "1358fa296d21d03a6e6bf996e429f315dd2cf9879034ba93580e3243e65669fd"),\
        ("SGUPTA","SGUPTA","Bangalore","Delhi","Marathalli","Marathalli"),\
        ("8691dd14cb5e57c529007fa104d40864b8fcc140ff516ed6e2b2be3d8334a1d9","Sujith","Karnataka","Karnataka","Karnataka",\
                                   '92edad65bb8a09c96bae7aa945521bed49e3c41bcd91c6d3482fbd76d2c8e35b'),\
        ("PK","PK","Bangalore","Delhi","Whitefield","Whitefield"),\
        ("dbbfefa577e8b8ad8eee368d5e9797549601cbbad66ce5a8aa026675aba082c7","Suraj","Whitefield","Whitefield","Whitefield",\
                                   "6fb139b169c25789ecd126610891cd281f8a8c52f2e74c5425c84512dd37d6fd")]
x = spark.createDataFrame(data, ["end_user_customer_name","end_user_customer_name_orig","end_user_address1_orig","bill_to_address1_orig","bill_to_address1","expected"])
check_test_results(
  x.withColumn("actual", udf_mod_dm60_hash(F.col("end_user_customer_name"), F.col("end_user_customer_name_orig"),\
                                           F.col("end_user_address1_orig"), F.col("bill_to_address1_orig"), F.col("bill_to_address1")))

# COMMAND ----------

# DQ67 - returns a dq if value is NOT in provided list
print "DQ67: returns a dq if value is NOT in provided list"
inputlist=['Y','N']
data = [("HT","DQ67"),(" ","DQ67")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq67_list_chk(F.col("col1"),inputlist))
)

print "DQ67: returns a dq if value is in provided list"
data = [("Y",None),("N",None),("r56tyu","DQ67")]
x = spark.createDataFrame(data, ["col1","expected"])
check_test_results(
  x.withColumn("actual", udf_dq67_list_chk(F.col("col1"),inputlist))
)

# COMMAND ----------

# filling flag for dq68 

expectVal1 =  ['O','F','C','I','A']
expectVal2 =  ['OBSOLETE','FUTURE','CANCELLED','INACTIVE','Active']


data = [("","Active","DQ68"),("A","Active",None),("","CANCELLED","DQ68"),("","Leveraged",None),("A","","DQ68"),("I",None,"DQ68")]
x = spark.createDataFrame(data, ["col1","col2","expected"])
x = x.withColumn("actual",lit(''))
for incr in range(len(expectVal1)):
  val1 = expectVal1[incr]
  val2 = expectVal2[incr]
  x=x.withColumn("actual",udf_concat_list_test(col("actual"),udf_dq68_cohortpair_chk(F.col("col1"),F.col("col2"),lit(val1),lit(val2))))
  x=x.withColumn("actual",udf_litNone(F.col("actual")))
  
check_test_results(x)


# COMMAND ----------

# filling flag for dm69 -udf_dm69_fill_pair_chk

expectVal1 =  ['O','F','C','I','A']
expectVal2 =  ['OBSOLETE','FUTURE','CANCELLED','INACTIVE','Active']


data = [("","Active","DM69"),("A","Active",None),("","CANCELLED","DM69"),("","Leveraged",None),("A","","DM69"),("I",None,"DM69")]
x = spark.createDataFrame(data, ["col1","col2","expected"])
x = x.withColumn("actual",lit(''))
for incr in range(len(expectVal1)):
  val1 = expectVal1[incr]
  val2 = expectVal2[incr]
  x=x.withColumn("actual",udf_concat_list_test(col("actual"),udf_dm69_fill_pair_chk(F.col("col1"),F.col("col2"),lit(val1),lit(val2))))
  x=x.withColumn("actual",udf_litNone(F.col("actual")))
  
check_test_results(x)


# COMMAND ----------

#call this method for both the columns  
def udf_mod_dm69_update(col1, col2, exp_col1,exp_col2):
  if(col2 == exp_col2 and ((col1=="NULL")or(col1=="") or (col1 ==None))):
     return exp_col1
         
udf_dm69_mod_test = udf(lambda a,b,c,d: udf_mod_dm69_update(a,b,c,d), StringType())

# COMMAND ----------

#step_dm69_fillpair_chk- flagging dm69
# DM69 - Flag missing code for unavailable matching cohort pair
# don't flag if the pairs are not identifiable ex - "", leveraged, and null, null etc
expectVal1 =  ['O','F','C','I','A']
expectVal2 =  ['OBSOLETE','FUTURE','CANCELLED','INACTIVE','Active']

print "DM69: Flag missing code for available matching cohort pair"


data_expect = [("","Active","A"),("A","Active",None),("","CANCELLED","C"),("","Leveraged",None),("I","","INACTIVE"),("F","","FUTURE")]
x = spark.createDataFrame(data_expect, ["col1","col2","expected"])
x = x.withColumn("actual",lit(''))
for incr in range(len(expectVal1)):
  val1 = expectVal1[incr]
  val2 = expectVal2[incr]
#    df_rdma.withColumn(col_pair[0],udf_dm69_mod_pair(F.col(col_pair[0]),F.col(col_pair[1]),lit(val1),lit(val2)))
  x=x.withColumn("col1",udf_concat_list_test(col("col1"),udf_dm69_mod_test(F.col("col1"),F.col("col2"),lit(val1),lit(val2))))
  x=x.withColumn("col1",udf_litNone(F.col("col1")))
  
  x=x.withColumn("col2",udf_concat_list_test(col("col2"),udf_dm69_mod_test(F.col("col2"),F.col("col1"),lit(val2),lit(val1))))
  x=x.withColumn("col2",udf_litNone(F.col("col2")))

check_test_results(x)
# this is working as expected i.e. the values are getting populated properly in col1 and col2, only  actual column is not getting populated, which is flagging and done as part of previous cell/method.
