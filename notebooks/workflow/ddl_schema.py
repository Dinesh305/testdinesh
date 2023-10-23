# Databricks notebook source
# DDL for MyIdentity
query0 = """
  create table if not exists {} 
  (
    country_code varchar(2) ENCODE lzo,
    postal_code varchar(20) ENCODE lzo,
    place_name varchar(180) ENCODE lzo,
    admin_name1 varchar(100) ENCODE lzo,
    admin_code1 varchar(20) ENCODE lzo,
    admin_name2 varchar(100) ENCODE lzo,
    admin_code2 varchar(20) ENCODE lzo,
    admin_name3 varchar(100) ENCODE lzo,
    admin_code3 varchar(20) ENCODE lzo,
    geo_datasource VARCHAR(45) ENCODE lzo,
    mapanet_language VARCHAR(10) ENCODE lzo,
    insert_ts TIMESTAMP ENCODE az64
  )
    DISTSTYLE AUTO
""".format(RedshiftFinalTableList[0])

# COMMAND ----------

dit = {RedshiftFinalTableList[0]:query0}