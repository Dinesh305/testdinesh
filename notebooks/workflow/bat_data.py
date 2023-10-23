# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, ByteType, IntegerType,FloatType,ShortType, DoubleType, DecimalType

# COMMAND ----------

expected_schema_output_location = {'country_postal_ref':StructType([StructField('country_code',StringType(),True),
                                                      StructField('postal_code',StringType(),True),
                                                      StructField('place_name',StringType(),True),
                                                      StructField('admin_name1',StringType(),True),
                                                      StructField('admin_code1',StringType(),True),
                                                      StructField('admin_name2',StringType(),True),
                                                      StructField('admin_code2',StringType(),True),
                                                      StructField('admin_name3',StringType(),True),
                                                      StructField('admin_code3',StringType(),True),
                                                      StructField('mapanet_language',StringType(),True),
                                                      StructField('insert_ts',StringType(),True),
                                                      StructField('geo_datasource',StringType(),True)]),
                                   
                                'look_up':  StructType([StructField('derived_country_code', StringType(),True),
                                           StructField('geo_datasource', StringType(),True),
                                           StructField('mapanet_language', StringType(),True),
                                           StructField('ent_onboard_date', StringType(),True)])
                               
                               }

# COMMAND ----------

expected_dtypes_output_location = {'country_postal_ref':[('country_code', 'string'),
                                                         ('postal_code', 'string'),
                                                         ('place_name', 'string'),
                                                         ('admin_code1', 'string'),
                                                         ('admin_name1', 'string'),
                                                         ('admin_code2', 'string'),
                                                         ('admin_name2', 'string'),
                                                         ('admin_code3', 'string'),
                                                         ('admin_name3', 'string'),
                                                         ('geo_datasource', 'string'),
                                                         ('mapanet_language', 'string'),
                                                         ('insert_ts', 'string')],
                                   
                                   'look_up':[('derived_country_code', 'string'),
                                               ('geo_datasource', 'string'),
                                               ('mapanet_language', 'string'),
                                               ('ent_onboard_date', 'string')]
                                 }

# COMMAND ----------

expected_schema_delta_final ={'country_postal_ref':StructType([StructField('country_code',StringType(),True),
                                                      StructField('postal_code',StringType(),True),
                                                      StructField('place_name',StringType(),True),
                                                      StructField('admin_name1',StringType(),True),
                                                      StructField('admin_code1',StringType(),True),
                                                      StructField('admin_name2',StringType(),True),
                                                      StructField('admin_code2',StringType(),True),
                                                      StructField('admin_name3',StringType(),True),
                                                      StructField('admin_code3',StringType(),True),
                                                      StructField('mapanet_language',StringType(),True),
                                                      StructField('insert_ts',TimestampType(),True),
                                                      StructField('geo_datasource',StringType(),True)])
                               }

# COMMAND ----------

expected_schema_delta_flags ={'country_postal_ref':StructType([StructField('country_code',StringType(),True),
                                                      StructField('postal_code',StringType(),True),
                                                      StructField('place_name',StringType(),True),
                                                      StructField('admin_name1',StringType(),True),
                                                      StructField('admin_code1',StringType(),True),
                                                      StructField('admin_name2',StringType(),True),
                                                      StructField('admin_code2',StringType(),True),
                                                      StructField('admin_name3',StringType(),True),
                                                      StructField('admin_code3',StringType(),True),
                                                      StructField('mapanet_language',StringType(),True),
                                                      StructField('insert_ts',TimestampType(),True),
                                                      StructField('geo_datasource',StringType(),True),
                                                      StructField('error_list', StringType(),True),
                                                      StructField('mod_list', StringType(),True),
                                                      StructField('error_code', IntegerType(),True)])
                               }

# COMMAND ----------

expected_schema_delta_stg = {'mapanet':StructType([StructField('countrya2', StringType(),True),
                                 StructField('language', StringType(),True),
                                 StructField('postalcode', StringType(),True),
                                 StructField('region0code', StringType(),True),
                                 StructField('region0name', StringType(),True),
                                 StructField('region1code', StringType(),True),
                                 StructField('region1name', StringType(),True),
                                 StructField('region2code', StringType(),True),
                                 StructField('region2name', StringType(),True),
                                 StructField('region3code', StringType(),True),
                                 StructField('region3name', StringType(),True),
                                 StructField('region4code', StringType(),True),
                                 StructField('region4name', StringType(),True),
                                 StructField('localitycode', StringType(),True),
                                 StructField('localitytype', StringType(),True),
                                 StructField('locality', StringType(),True),
                                 StructField('sublocalitycode', StringType(),True),
                                 StructField('sublocalitytype', StringType(),True),
                                 StructField('sublocality', StringType(),True),
                                 StructField('areacode', StringType(),True),
                                 StructField('areatype', StringType(),True),
                                 StructField('areaname', StringType(),True),
                                 StructField('latitude', DecimalType(10,7),True),
                                 StructField('longitude', DecimalType(10,7),True),
                                 StructField('altitude', IntegerType(),True),
                                 StructField('timezone', StringType(),True),
                                 StructField('utc', StringType(),True),
                                 StructField('dst', StringType(),True)
                                 ]),
                       
                    'geo_names': StructType([StructField('country_code', StringType(),True),
                                   StructField('postal_code', StringType(),True),
                                   StructField('place_name', StringType(),True),
                                   StructField('admin_name1', StringType(),True),
                                   StructField('admin_code1', StringType(),True),
                                   StructField('admin_name2', StringType(),True),
                                   StructField('admin_code2', StringType(),True),
                                   StructField('admin_name3', StringType(),True),
                                   StructField('admin_code3', StringType(),True),
                                   StructField('latitude', DecimalType(9,6),True),
                                   StructField('longitude',DecimalType(9,6),True),
                                   StructField('accuracy',DecimalType(10,6),True),
                                   StructField('source', StringType(),True)
                                  ]),
                       
                    'look_up':  StructType([StructField('derived_country_code', StringType(),True),
                                           StructField('geo_datasource', StringType(),True),
                                           StructField('mapanet_language', StringType(),True),
                                           StructField('ent_onboard_date', StringType(),True)])
                      }

# COMMAND ----------

expected_dtypes_delta_stg = {'mapanet': [('countrya2', 'string'),
                               ('language', 'string'),
                               ('postalcode', 'string'),
                               ('region0code', 'string'),
                               ('region0name', 'string'),
                               ('region1code', 'string'),
                               ('region1name', 'string'),
                               ('region2code', 'string'),
                               ('region2name', 'string'),
                               ('region3code', 'string'),
                               ('region3name', 'string'),
                               ('region4code', 'string'),
                               ('region4name', 'string'),
                               ('localitycode', 'string'),
                               ('localitytype', 'string'),
                               ('locality', 'string'),
                               ('sublocalitycode', 'string'),
                               ('sublocalitytype', 'string'),
                               ('sublocality', 'string'),
                               ('areacode', 'string'),
                               ('areatype', 'string'),
                               ('areaname', 'string'),
                               ('latitude', 'decimal(10,7)'),
                               ('longitude', 'decimal(10,7)'),
                               ('altitude', 'int'),
                               ('timezone', 'string'),
                               ('utc', 'string'),
                               ('dst', 'string')
                               ],
                             
                 'geo_names': [('country_code', 'string'),
                               ('postal_code', 'string'),
                               ('place_name', 'string'),
                               ('admin_name1', 'string'),
                               ('admin_code1', 'string'),
                               ('admin_name2', 'string'),
                               ('admin_code2', 'string'),
                               ('admin_name3', 'string'),
                               ('admin_code3', 'string'),
                               ('latitude', 'decimal(9,6)'),
                               ('longitude', 'decimal(9,6)'),
                               ('accuracy', 'decimal(10,6)'),
                               ('source', 'string')
                               ],
                   
                 'look_up':[('derived_country_code', 'string'),
                           ('geo_datasource', 'string'),
                           ('mapanet_language', 'string'),
                           ('ent_onboard_date', 'string')]

                  }

# COMMAND ----------

expected_dtypes_delta_final = {'country_postal_ref':[('country_code', 'string'),
                                                         ('postal_code', 'string'),
                                                         ('place_name', 'string'),
                                                         ('admin_code1', 'string'),
                                                         ('admin_name1', 'string'),
                                                         ('admin_code2', 'string'),
                                                         ('admin_name2', 'string'),
                                                         ('admin_code3', 'string'),
                                                         ('admin_name3', 'string'),
                                                         ('geo_datasource', 'string'),
                                                         ('mapanet_language', 'string'),
                                                         ('insert_ts', 'timestamp')]    
                                 }

# COMMAND ----------

expected_dtypes_delta_flags = { 'country_postal_ref':[('country_code', 'string'),
                                                         ('postal_code', 'string'),
                                                         ('place_name', 'string'),
                                                         ('admin_code1', 'string'),
                                                         ('admin_name1', 'string'),
                                                         ('admin_code2', 'string'),
                                                         ('admin_name2', 'string'),
                                                         ('admin_code3', 'string'),
                                                         ('admin_name3', 'string'),
                                                         ('geo_datasource', 'string'),
                                                         ('mapanet_language', 'string'),
                                                         ('insert_ts', 'timestamp'),
                                                         ('error_list', 'string'),
                                                         ('mod_list', 'string'),
                                                         ('error_code', 'int')]}