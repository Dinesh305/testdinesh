# Databricks notebook source
# MAGIC %run /Library/python/workflow_library_py3

# COMMAND ----------

#parameters for secrets manager
endpoint_url = "https://secretsmanager.us-west-2.amazonaws.com"
region_name = "us-west-2"

auto_enrich_dev = "arn:aws:secretsmanager:us-west-2:714256721545:secret:dev/redshift/auto_enrich"
auto_ref_dev = "arn:aws:secretsmanager:us-west-2:714256721545:secret:auto_ref"
auto_email_dev = "arn:aws:secretsmanager:us-west-2:714256721545:secret:dev/redshift/auto_email_campaigns"
auto_dsr_dev = "arn:aws:secretsmanager:us-west-2:714256721545:secret:dev/redshift/auto_dsr-3FwAh2"

auto_enrich_itg = "arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_enrich"
auto_ref_itg = "arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_ref"
auto_email_itg = "arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_email_campaigns"
auto_dsr_itg = "arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_dsr"

auto_dsr_prod02 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_dsr"
auto_email_prod02 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_email_campaigns"
auto_ref_prod02 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_ref"
auto_enrich_prod02 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_enrich"   
auto_customer_surveys_prod02 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_customer_surveys"
auto_customer_surveys_restricted_prod02 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_customer_surveys_restricted"

auto_ref_prod04 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_ref"
auto_enrich_prod04 =  "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_enrich"
auto_dsr_prod04 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_dsr"
auto_email_prod04= "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_email_campaigns"
auto_customer_surveys_prod04 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_customer_surveys"
auto_customer_surveys_restricted_prod04 = "arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_customer_surveys_restricted-0fLF6Z"

port = "5439"
prod_port = "5439"

# COMMAND ----------

if environment.lower() in ['dev_dsr', 'dev']:
  
  RedshiftInstance_dev = "bdbt-redshift-dev.hp8.us"
  dev_rs_url = RedshiftInstance_dev + ":" + port
  dev_rs_dbname = "bdbt"
  
  dev_rs_user_enrich = "auto_enrich"
  dev_rs_pw_enrich = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_enrich_dev, "password")
  dev_jdbc_url_enrich = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(dev_rs_url, dev_rs_dbname, dev_rs_user_enrich  , dev_rs_pw_enrich)

  dev_rs_user_email_campaigns = "auto_email_campaigns"
  dev_rs_pw_email_campaigns = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_email_dev, "password")
  dev_jdbc_url_email_campaigns = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(dev_rs_url, dev_rs_dbname, dev_rs_user_email_campaigns  , dev_rs_pw_email_campaigns)
  
  dev_rs_user_ref = "auto_ref"
  dev_rs_pw_ref = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_ref_dev, "auto_ref")
  dev_jdbc_url_ref = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(dev_rs_url, dev_rs_dbname, dev_rs_user_ref  , dev_rs_pw_ref)
  
  dev_rs_user_dsr = "auto_dsr"
  dev_rs_pw_dsr = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_dsr_dev, "password")
  dev_jdbc_url_dsr = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(dev_rs_url, dev_rs_dbname, dev_rs_user_dsr, dev_rs_pw_dsr)
  
elif environment.lower() in ["itg", "itg_dsr"]:  
  dsr_rs_user = "auto_enrich"
  dsr_rs_pw = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_dsr_itg, "password")
  
  RedshiftInstance_itg = "bdbt-redshift-itg-02.hp8.us"
  itg_rs_url = RedshiftInstance_itg + ":" + port
  itg_rs_dbname = "bdbt"
  itg_rs_user_enrich = "auto_enrich"
  itg_rs_pw_enrich = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_enrich_itg,"password")
  itg_jdbc_url_enrich = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(itg_rs_url, itg_rs_dbname, itg_rs_user_enrich, itg_rs_pw_enrich)  
  
  itg_rs_user_ref = "auto_ref"
  itg_rs_pw_ref = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_ref_itg, "password")
  itg_jdbc_url_ref = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(itg_rs_url, itg_rs_dbname, itg_rs_user_ref, itg_rs_pw_ref)
  
  itg_rs_user_email_campaigns = "auto_email_campaigns"
  itg_rs_pw_email_campaigns = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_email_itg, "password")
  itg_jdbc_url_email_campaigns = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(itg_rs_url, itg_rs_dbname, itg_rs_user_email_campaigns, itg_rs_pw_email_campaigns)
  
  itg_rs_user_dsr = "auto_dsr"
  itg_rs_pw_dsr = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_dsr_itg, "password")
  itg_jdbc_url_dsr = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(itg_rs_url, itg_rs_dbname, itg_rs_user_dsr, itg_rs_pw_dsr)
  
elif environment.lower() in ["prod", "prod_dsr"]: 
  
  dsr_rs_user = "auto_enrich" 
  dsr_rs_pw_02 = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_dsr_prod02, "password")
  dsr_rs_pw_04 = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_dsr_prod04, "password")
  
  RedshiftInstance_prod02 = "bdbt-redshift-prod-02.hp8.us"
  pro02_rs_url = RedshiftInstance_prod02 + ":" + prod_port
  pro02_rs_dbname = "bdbt"
  pro02_rs_user_enrich = "auto_enrich"
  pro02_rs_pw_enrich = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_enrich_prod02, "password")
  pro02_jdbc_url_enrich = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro02_rs_url, pro02_rs_dbname, pro02_rs_user_enrich, pro02_rs_pw_enrich)
  
  pro02_rs_user_ref = "auto_ref"
  pro02_rs_pw_ref = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_ref_prod02 , "password")
  pro02_jdbc_url_ref = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro02_rs_url, pro02_rs_dbname, pro02_rs_user_ref, pro02_rs_pw_ref)
  
  pro02_rs_user_email_campaigns = "auto_email_campaigns"
  pro02_rs_pw_email_campaigns = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_email_prod02, "password")
  pro02_jdbc_url_email_campaigns = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro02_rs_url, pro02_rs_dbname, pro02_rs_user_email_campaigns  , pro02_rs_pw_email_campaigns)

  pro02_rs_user_customer_surveys = "auto_customer_surveys"
  pro02_rs_pw_customer_surveys = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_customer_surveys_prod02, "password")
  pro02_jdbc_url_customer_surveys = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro02_rs_url, pro02_rs_dbname, pro02_rs_user_customer_surveys  , pro02_rs_pw_customer_surveys)

  pro02_rs_user_customer_surveys_restricted = "auto_customer_surveys_restricted"
  pro02_rs_pw_customer_surveys_restricted = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_customer_surveys_restricted_prod02, "password")
  pro02_jdbc_url_customer_surveys_restricted = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro02_rs_url, pro02_rs_dbname, pro02_rs_user_customer_surveys_restricted  , pro02_rs_pw_customer_surveys_restricted)
  
  pro02_rs_user_dsr = "auto_dsr"
  pro02_rs_pw_dsr = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_dsr_prod02, "password")
  pro02_jdbc_url_dsr = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro02_rs_url, pro02_rs_dbname, pro02_rs_user_dsr, pro02_rs_pw_dsr)

  RedshiftInstance_prod03 = "bdbt-redshift-prod-04.hp8.us"
  pro03_rs_url = RedshiftInstance_prod03 + ":" + prod_port
  pro03_rs_dbname = "bdbt"
  pro03_rs_user_enrich = "auto_enrich"
  pro03_rs_pw_enrich = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_enrich_prod04, "password")
  pro03_jdbc_url_enrich = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro03_rs_url, pro03_rs_dbname, pro03_rs_user_enrich, pro03_rs_pw_enrich)
  
  pro03_rs_user_ref = "auto_ref"
  pro03_rs_pw_ref = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_ref_prod04, "password")
  pro03_jdbc_url_ref = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro03_rs_url, pro03_rs_dbname, pro03_rs_user_ref, pro03_rs_pw_ref)
  
  pro03_rs_user_email_campaigns = "auto_email_campaigns"
  pro03_rs_pw_email_campaigns = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_email_prod04, "password")
  pro03_jdbc_url_email_campaigns = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro03_rs_url, pro03_rs_dbname, pro03_rs_user_email_campaigns, pro03_rs_pw_email_campaigns)
  
  pro03_rs_user_dsr = "auto_dsr"
  pro03_rs_pw_dsr = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_dsr_prod04, "password")
  pro03_jdbc_url_dsr = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro03_rs_url, pro03_rs_dbname, pro03_rs_user_dsr, pro03_rs_pw_dsr)
  
  pro03_rs_user_customer_surveys = "auto_customer_surveys"
  pro03_rs_pw_customer_surveys = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_customer_surveys_prod04, "password")
  pro03_jdbc_url_customer_surveys = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro03_rs_url, pro03_rs_dbname, pro03_rs_user_customer_surveys  , pro03_rs_pw_customer_surveys)

  pro03_rs_user_customer_surveys_restricted = "auto_customer_surveys_restricted"
  pro03_rs_pw_customer_surveys_restricted = aws_secrets_manager_get_secret(endpoint_url, region_name, auto_customer_surveys_restricted_prod04, "password")
  pro03_jdbc_url_customer_surveys_restricted = "jdbc:redshift://{}/{}?user={}&password={}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory".format(pro03_rs_url, pro03_rs_dbname, pro03_rs_user_customer_surveys_restricted  , pro03_rs_pw_customer_surveys_restricted)

dev_tmp = "s3a://hp-bigdata-dev-enrichment/logs/db_temp/"
itg_tmp = "s3a://hp-bigdata-itg-enrichment/logs/db_temp/"
prod_tmp = "s3a://hp-bigdata-prod-enrichment/logs/db_temp/"

dev_iam = "arn:aws:iam::714256721545:role/BD-RedshiftCopyUnload"
itg_iam = "arn:aws:iam::651785582400:role/BD-RedshiftCopyUnload"
prod_iam = "arn:aws:iam::651785582400:role/BD-RedshiftCopyUnload"

metrics_location_prod = "s3a://hp-bigdata-prod-enrichment/metrics/enrichment/"
metrics_location_itg = "s3a://hp-bigdata-itg-enrichment/metrics/enrichment/"
metrics_location_dev = "s3a://hp-bigdata-dev-enrichment/metrics/enrichment/"

elk_metrics_location_dev = "s3a://hp-bigdata-dev-enrichment/metrics/elk_metrics/enrichment/"
elk_metrics_location_itg = "s3a://hp-bigdata-itg-enrichment/metrics/elk_metrics/enrichment/"
elk_metrics_location_prod = "s3a://hp-bigdata-prod-enrichment/metrics/elk_metrics/enrichment/"

#Change
prev_read_location_prod_enrich = "s3a://enrich-data-lake-prod/Prev_read_Path/"
prev_read_location_itg_enrich = "s3a://enrich-data-lake-itg/Prev_read_Path/"
prev_read_location_dev_enrich = "s3a://enrich-data-lake/Prev_read_Path/"

prev_read_location_prod_ref = "s3a://ref-data-prod/Prev_read_Path/"
prev_read_location_itg_ref = "s3a://ref-data-itg/Prev_read_Path/"
prev_read_location_dev_ref = "s3a://ref-data-dev/Prev_read_Path/"

RunTrackingTable = "dataos_track.cumulus_run_tracking"

# COMMAND ----------

def get_rs_env_password(env, redshift_user):
    
  if env.lower() in ['dev', 'dev_dsr']:
    dev_password = { 'auto_enrich'              :  dev_rs_pw_enrich,
                     'auto_ref'                 :  dev_rs_pw_ref,
                     'auto_email_campaigns'     :  dev_rs_pw_email_campaigns,
                     'auto_dsr'                 :  dev_rs_pw_dsr
                    }
  
    _password = dev_password.get(redshift_user)
  
  elif env.lower() in ['itg','itg-02','itg_02','itg_dsr']:
    itg_password = { 'auto_enrich'              :  itg_rs_pw_enrich,
                   'auto_ref'                   :  itg_rs_pw_ref,
                   'auto_email_campaigns'       :  itg_rs_pw_email_campaigns,
                    'auto_dsr'                  :  itg_rs_pw_dsr
                  }
    _password = itg_password.get(redshift_user)
    
  elif env.lower() in ['prod-02','prod02','prod_02','prod02_dsr','prod02-dsr']:  
    # prod02 password dict
    prod02_password = { 'auto_enrich'            :  pro02_rs_pw_enrich,
                      'auto_ref'                 :  pro02_rs_pw_ref,
                      'auto_email_campaigns'     :  pro02_rs_pw_email_campaigns,
                      'auto_customer_surveys'    :  pro02_rs_pw_customer_surveys,
                      'auto_customer_surveys_restricted' :pro02_rs_pw_customer_surveys_restricted,
                       'auto_dsr'                        :  pro02_rs_pw_dsr
                      }
    _password = prod02_password.get(redshift_user)
    
  elif env.lower() in ['prod-04', 'prod04','prod_04','prod04_dsr','prod04-dsr']:
    # prod04 password dict
    prod04_password = { 'auto_enrich'                    :  pro03_rs_pw_enrich,
                      'auto_ref'                         :  pro03_rs_pw_ref,
                      'auto_email_campaigns'             :  pro03_rs_pw_email_campaigns,
                      'auto_customer_surveys'            : pro03_rs_pw_customer_surveys,
                      'auto_customer_surveys_restricted' :  pro03_rs_pw_customer_surveys_restricted,
                       'auto_dsr'                        :  pro03_rs_pw_dsr
                      }
    _password = prod04_password.get(redshift_user)
  
  else:
    raise Exception(f'InvalidEnvironmentError {env}')
    
  if _password == None:
    raise Exception(f'InvalidPasswordException : {_password} is not a valid password for user {redshift_user}')

  return _password

# COMMAND ----------

def get_rs_env_jdbc_url(env, redshift_user):
    
  if env.lower() in ['dev','dev_dsr']:
    dev_jdbc_url = { 'auto_enrich'              : dev_jdbc_url_enrich,
                   'auto_ref'                 :  dev_jdbc_url_ref,
                   'auto_email_campaigns'     :  dev_jdbc_url_email_campaigns,
                    'auto_dsr' :  dev_jdbc_url_dsr
                  }
  
    _jdbc_url = dev_jdbc_url.get(redshift_user)
  
  elif env.lower() in ['itg','itg-02','itg_02','itg_dsr']:
    itg_jdbc_url = { 'auto_enrich'            :  itg_jdbc_url_enrich,
                   'auto_ref'                 :  itg_jdbc_url_ref,
                   'auto_email_campaigns'     :  itg_jdbc_url_email_campaigns,
                     'auto_dsr' :  itg_jdbc_url_dsr
                  }
    _jdbc_url = itg_jdbc_url.get(redshift_user)
    
  elif env.lower() in ['prod-02','prod02','prod_02','prod02_dsr','prod02-dsr']:  
    # prod02 password dict
    prod02_jdbc_url = { 'auto_enrich'            :  pro02_jdbc_url_enrich,
                      'auto_ref'                 :  pro02_jdbc_url_ref,
                      'auto_email_campaigns'     :  pro02_jdbc_url_email_campaigns,
                      'auto_customer_surveys'    :  pro02_jdbc_url_customer_surveys,
                      'auto_customer_surveys_restricted' :pro02_jdbc_url_customer_surveys_restricted,
                       'auto_dsr' :  pro02_jdbc_url_dsr
                      }
    _jdbc_url = prod02_jdbc_url.get(redshift_user)
    
  elif env.lower() in ['prod-04', 'prod04','prod_04','prod04_dsr','prod04-dsr']:
    # prod04 password dict
    prod04_jdbc_url = { 'auto_enrich'            :  pro03_jdbc_url_enrich,
                      'auto_ref'                 :  pro03_jdbc_url_ref,
                      'auto_email_campaigns'     :  pro03_jdbc_url_email_campaigns,
                      'auto_customer_surveys'    : pro03_jdbc_url_customer_surveys,
                      'auto_customer_surveys_restricted' :pro03_jdbc_url_customer_surveys_restricted,
                       'auto_dsr' :  pro03_jdbc_url_dsr
                      }
    _jdbc_url = prod04_jdbc_url.get(redshift_user)
  
  else:
    raise Exception(f'InvalidEnvironmentError {env}')
    
  if _jdbc_url == None:
    raise Exception(f'InvalidJDBCUrlException : {_jdbc_url} is not a valid jdbc url for user {redshift_user}')

 

  return _jdbc_url

# COMMAND ----------

'''
Get BIID/SKEY
'''
import hmac
import hashlib
import base64

# def strip_non_printable(value):
#   if value != None:
#     stripped_value = (c for c in value if 0 < ord(c) < 127)
#     return ''.join(stripped_value)

#If uuid has a non-acii char, return N/A
def get_biid(uuid):
  for char in uuid:
    if not 0 < ord(char) < 128:
      return "N/A"
  message = bytes(uuid).encode('utf-8')
  secret = bytes("12a244f3c74c4134716944ab5caf73722c564ddc36d54a66c071434911a2f321293beeb41c271501fb059d4d77c55407408ba89228670bfa5cc6410b886363e3").encode('utf-8')
  signature = base64.b16encode(hmac.new(secret, message, digestmod=hashlib.sha256).digest()).lower()
  return signature

#If biid has a non-ascii char, return N/A
def get_skey(biid):
  if biid == "N/A":
    return "N/A"
  message = bytes(biid).encode('utf-8')
  secret = bytes("089F81F753F54A6B94931DD3DCD1A0976184D4DB7F481437A5EA5F6258D70958").encode('utf-8')
  signature = base64.b16encode(hmac.new(secret, message, digestmod=hashlib.sha256).digest()).lower()
  return signature

# COMMAND ----------

#Get dates for previous quarter (used by channel shipments)

def previous_quarter(current):
  q1_start = datetime(current.year - 1, 11, 1)
  q1_end = datetime(current.year, 1, 31)
  q2_start = datetime(current.year, 2, 1)
  q2_end = datetime(current.year, 4, 30)
  q3_start = datetime(current.year, 5, 1)
  q3_end = datetime(current.year, 7, 31)
  q4_start = datetime(current.year - 1, 8, 1)
  q4_end = datetime(current.year - 1, 10, 31)

  if 2 <= current.month <= 4:
    return datetime.strftime(q1_start, "%Y-%m-%d"), datetime.strftime(q1_end, "%Y-%m-%d")
  elif 5 <= current.month <= 7:
    return datetime.strftime(q2_start, "%Y-%m-%d"), datetime.strftime(q2_end, "%Y-%m-%d")
  elif 8 <= current.month <= 10:
    return datetime.strftime(q3_start, "%Y-%m-%d"), datetime.strftime(q3_end, "%Y-%m-%d")
  return datetime.strftime(q4_start, "%Y-%m-%d"), datetime.strftime(q4_end, "%Y-%m-%d")