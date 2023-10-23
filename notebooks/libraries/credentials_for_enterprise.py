# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("envt_dbuser", "")
envt_dbuser = dbutils.widgets.get("envt_dbuser")

# COMMAND ----------

# MAGIC %run ../libraries/aws_secrets_manager

# COMMAND ----------

# MAGIC %run ../libraries/datawarehouse

# COMMAND ----------

class Redshift_Configuration(object):
  
  supported_envs = ['dev','dev_dsr','itg','itg_dsr','prod02','prod02_dsr','prod04','prod04_dsr']
  
  supported_landscape = {'development':['dev','dev_dsr']
                        ,'integration': ['itg','itg_dsr']
                        ,'production': ['prod02','prod02_dsr','prod04','prod04_dsr']
                        }
  
  alias_envt_dbuser_dict = {'d0':['dev','rsautomation']
                           ,'d1':['dev','auto_enrich']
                           ,'d2':['dev','auto_ref']
                           ,'d3':['dev_dsr','auto_dsr']
                           ,'d4':['dev','auto_email_campaigns']
                           ,'d5':['dev','auto_subscription_enrollments']
                           ,'i1':['itg','auto_enrich']
                           ,'i2':['itg','auto_ref']
                           ,'i3':['itg_dsr','auto_dsr']
                           ,'i4':['itg','auto_email_campaigns']
                           ,'i5' :['itg', 'auto_registration_info']
                           ,'i6' :['itg', 'auto_subscription_enrollments']
                           ,'2p0':['prod02','rsautomation']
                           ,'2p1':['prod02','auto_enrich']
                           ,'2p2':['prod02','auto_ref']
                           ,'2p3':['prod02_dsr','auto_dsr']
                           ,'2p4':['prod02','auto_email_campaigns']
                           ,'2p5':['prod02','auto_customer_surveys']
                           ,'2p6':['prod02','auto_registration_info']
                           ,'2p7':['prod02','auto_subscription_enrollments']
                           ,'2p8':['prod02','auto_customer_surveys_restricted']
                           ,'4p0':['prod04','rsautomation']
                           ,'4p1':['prod04','auto_enrich']
                           ,'4p2':['prod04','auto_ref']
                           ,'4p3':['prod04_dsr','auto_dsr']
                           ,'4p4':['prod04','auto_email_campaigns']
                           ,'4p5':['prod04','auto_customer_surveys']
                           ,'4p6':['prod04','auto_registration_info']
                           ,'4p7':['prod04','auto_customer_surveys_restricted']
                                                                   }
  
  alias_phass_phrases = {'d0':['arn:aws:secretsmanager:us-west-2:714256721545:secret:dev/redshift/rsautomation','password']
                        ,'d1':['arn:aws:secretsmanager:us-west-2:714256721545:secret:dev/redshift/auto_enrich','password']
                       ,'d2':['arn:aws:secretsmanager:us-west-2:714256721545:secret:auto_ref','auto_ref']
                       ,'d3':['arn:aws:secretsmanager:us-west-2:714256721545:secret:dev/redshift/auto_dsr','password']
                       ,'d4':['arn:aws:secretsmanager:us-west-2:714256721545:secret:dev/redshift/auto_email_campaigns','password']
                       ,'d5':['arn:aws:secretsmanager:us-west-2:714256721545:secret:dev/redshift/auto_subscription_enrollments','password']
                       ,'i0':['rsautomation-itg','rsautomation']
                       ,'i1':['arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_enrich','password']
                       ,'i2':['arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_ref','password']
                       ,'i3':['arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_dsr','password']
                       ,'i4':['arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_email_campaigns','password']
                       ,'i5':['arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_registration_info','password']
                       ,'i6':['arn:aws:secretsmanager:us-west-2:651785582400:secret:itg02/redshift/auto_subscription_enrollments','password']
                       ,'2p0':['rsautomation-itg','rsautomation']
                       ,'2p1':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_enrich','password']
                       ,'2p2':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_ref','password']
                       ,'2p3':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_dsr','password']
                       ,'2p4':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_email_campaigns','password']
                       ,'2p5':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_customer_surveys','password']
                       ,'2p6':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_registration_info','password']
                       ,'2p7':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_subscription_enrollments','password']
                       ,'2p8':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod02/redshift/auto_customer_surveys_restricted','password']
                       ,'4p0':['prod04','rsautomation'] # rsautomation doesn't exisit for prod04
                       ,'4p1':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_enrich','password']
                       ,'4p2':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_ref','password']
                       ,'4p3':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_dsr','password']
                       ,'4p4':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_email_campaigns','password']
                       ,'4p5':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_customer_surveys','password']
                       ,'4p6':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_registration_info','password']
                       ,'4p7':['arn:aws:secretsmanager:us-west-2:651785582400:secret:prod04/redshift/auto_customer_surveys_restricted','password']}
  
                          
  dev_tmp = "s3a://hp-bigdata-dev-enrichment/logs/db_temp/"
  itg_tmp = "s3a://hp-bigdata-itg-enrichment/logs/db_temp/"
  prod_tmp = "s3a://hp-bigdata-prod-enrichment/logs/db_temp/"

  dev_iam = "arn:aws:iam::714256721545:role/BD-RedshiftCopyUnload"
  itg_iam = "arn:aws:iam::651785582400:role/BD-RedshiftCopyUnload"
  prod_iam = "arn:aws:iam::651785582400:role/BD-RedshiftCopyUnload"
  
  # Change
  metrics_location_prod = "s3a://hp-bigdata-prod-enrichment/metrics/enrichment/"
  metrics_location_itg = "s3a://hp-bigdata-itg-enrichment/metrics/enrichment/"
  metrics_location_dev = "s3a://hp-bigdata-dev-enrichment/metrics/enrichment/"
  
  # Change
  elk_metrics_location_dev = "s3a://hp-bigdata-dev-enrichment/metrics/elk_metrics/enrichment/"
  elk_metrics_location_itg = "s3a://hp-bigdata-itg-enrichment/metrics/elk_metrics/enrichment/"
  elk_metrics_location_prod = "s3a://hp-bigdata-prod-enrichment/metrics/elk_metrics/enrichment/"

  
  def __init__(self, alias_key,secrets_manager_instance = None, dbname = 'bdbt'):
    
    if alias_key in Redshift_Configuration.alias_envt_dbuser_dict.keys():
      self._alias_key = alias_key
    else:
      raise Exception('NotAValidAliasKey : {} is not a alias key'.format(alias_key)) 
    self._dbname = dbname
    self._set_redshift_instance(secrets_manager_instance)
    
   

  def get_rs_cred(key):
    for k,v in Redshift_Configuration.alias_envt_dbuser_dict.items():
      if key == k:
        environment =  v[0]
        dbuser      =  v[1]
    return environment,dbuser  

  def _set_redshift_instance(self,secrets_manager_instance):
    environment,dbuser = Redshift_Configuration.get_rs_cred(self._alias_key)
    dbname = self.get_dbname()
    redshift_instance = RedshiftDatawarehouse(dbuser,environment,dbname)
    redshift_instance.set_secrets_manager_instance(secrets_manager_instance)
    self._redshift_instance = redshift_instance
    return 
  
  def get_dbname(self):
    return self._dbname
  
  def get_username(self):
    env,username = Redshift_Configuration.alias_envt_dbuser_dict.get(self._alias_key)
    return username
  
  def get_environment(self):
    env,username = Redshift_Configuration.alias_envt_dbuser_dict.get(self._alias_key)
    return env
  
  def get_password(self):
    [secret_name,secret_name_key] = Redshift_Configuration.alias_phass_phrases.get(self._alias_key)
    secrets_manager_service = self._redshift_instance.get_secrets_manager_instance()
    rs_passwrd = secrets_manager_service.get_secret(secret_name, secret_name_key)
    return rs_passwrd
  
  def get_redshift_instance(self):
    "Returns: a redshift datawarehouse instance"
    return self._redshift_instance
  
  def get_host_url(self):
    "Returns : a redshift endpoint to connect to"
    host_url = self._redshift_instance.get_host_url()
    return host_url
  
  def get_port(self):
    "Returns: a redshift port to connect to"
    port = self._redshift_instance.get_port()
    return port
  
  def get_ssl_status(self):
    """Returns : a status value, as par"""
    ssl_status = self._redshift_instance.get_ssl_status()
    return ssl_status
  
  def get_temp_bucket_path(self):
    """Returns : a s3 path, parent path for users to use as intermediate location """
    # fetch the type of environment
    environment,_ = Redshift_Configuration.get_rs_cred(self._alias_key)
    
    if environment in Redshift_Configuration.supported_landscape.get('development'):
      tmp_path = Redshift_Configuration.dev_tmp
    elif environment in Redshift_Configuration.supported_landscape.get('integration'):
      tmp_path = Redshift_Configuration.itg_tmp
    elif environment in Redshift_Configuration.supported_landscape.get('production'):
      tmp_path = Redshift_Configuration.prod_tmp
    else:
      raise Exception('NotASupportedEnvironment')
      
    return tmp_path
  
  # Change - get metrics location
  def get_metrics_location_path(self):
    """Returns : a s3 path, metrics write location """
    # fetch the type of environment
    environment,_ = Redshift_Configuration.get_rs_cred(self._alias_key)
    
    if environment in Redshift_Configuration.supported_landscape.get('development'):
      metrics_path = Redshift_Configuration.metrics_location_dev
    elif environment in Redshift_Configuration.supported_landscape.get('integration'):
      metrics_path = Redshift_Configuration.metrics_location_itg
    elif environment in Redshift_Configuration.supported_landscape.get('production'):
      metrics_path = Redshift_Configuration.metrics_location_prod
    else:
      raise Exception('NotASupportedEnvironment')
      
    return metrics_path
  
  # Change - get elk metrics location
  def get_elk_metrics_location_path(self):
    """Returns : a s3 path, elk metrics write location """
    # fetch the type of environment
    environment,_ = Redshift_Configuration.get_rs_cred(self._alias_key)
    
    if environment in Redshift_Configuration.supported_landscape.get('development'):
      elk_metrics_path = Redshift_Configuration.elk_metrics_location_dev
    elif environment in Redshift_Configuration.supported_landscape.get('integration'):
      elk_metrics_path = Redshift_Configuration.elk_metrics_location_itg
    elif environment in Redshift_Configuration.supported_landscape.get('production'):
      elk_metrics_path = Redshift_Configuration.elk_metrics_location_prod
    else:
      raise Exception('NotASupportedEnvironment')
      
    return elk_metrics_path
  
  def get_iam_role(self):
    """Retuns : IAM role"""
    
    # fetch the type of environment
    environment,_ = Redshift_Configuration.get_rs_cred(self._alias_key)
    
    if environment in Redshift_Configuration.supported_landscape.get('development'):
      iam_role = Redshift_Configuration.dev_iam
    elif environment in Redshift_Configuration.supported_landscape.get('integration'):
      iam_role = Redshift_Configuration.itg_iam
    elif environment in Redshift_Configuration.supported_landscape.get('production'):
      iam_role = Redshift_Configuration.prod_iam
    else:
      raise Exception('NotASupportedEnvironment')
      
    return iam_role
  
  
  def get_jdbc_url(self):
    """ Returns : JDBC URL to connect """
    [secret_name,secret_name_key] = Redshift_Configuration.alias_phass_phrases.get(self._alias_key)
    jdbc_url = self._redshift_instance.get_jdbc_url(secret_name,secret_name_key)
    
    return jdbc_url
  
  def get_rs_configuration(self):
    """
    Purppose : A wrapper function to get general configuration of the instance
    
    Returns :
      rs_env:  environment
      rs_dbname: dbname
      rs_user: dbuser name
      rs_password : dbuser password
      rs_host :  endpoint url
      rs_port : endpoint port
      rs_sslmode : security mode
      rs_jdbc_url : jdbc url to connect
      iam : IAM role
    """
    try:
      
      rs_env       = self.get_environment()
      rs_dbname    = self.get_dbname()
      rs_user      = self.get_username()
      rs_password  = self.get_password()
      rs_host      = self.get_host_url()
      rs_port      = self.get_port()
      rs_sslmode   = self.get_ssl_status()
      rs_jdbc_url  = self.get_jdbc_url()
      iam      = self.get_iam_role()
      
    except Exception as e:
      raise e
    else:
      return rs_env, rs_dbname,rs_user,rs_password,rs_host,rs_port,rs_sslmode,rs_jdbc_url,aws_iam

# COMMAND ----------

secrets_manager_us_west_2_endpoint_url = "https://secretsmanager.us-west-2.amazonaws.com"
region_name = "us-west-2"
secrets_manager_us_west_2 = Secrets_Manager_Service(secrets_manager_us_west_2_endpoint_url,region_name)
RunTrackingTable = "dataos_track.cumulus_run_tracking"

# COMMAND ----------

redshift_configuration = Redshift_Configuration(envt_dbuser,secrets_manager_us_west_2)

# COMMAND ----------

creds = {
    "rs_env"                           : redshift_configuration.get_environment(),
    "rs_dbname"                        : redshift_configuration.get_dbname(),
    "rs_user"                          : redshift_configuration.get_username(),
    "rs_password"                      : redshift_configuration.get_password(),
    "rs_host"                          : redshift_configuration.get_host_url(),
    "rs_port"                          : redshift_configuration.get_port(),
    "rs_sslmode"                       : redshift_configuration.get_ssl_status(),
    "rs_jdbc_url"                      : redshift_configuration.get_jdbc_url(),
    "aws_iam"                          : redshift_configuration.get_iam_role(),
    "tmp_dir"                          : redshift_configuration.get_temp_bucket_path(),
    "metrics_location"                 : redshift_configuration.get_metrics_location_path(),
    "elk_metrics_location"             : redshift_configuration.get_elk_metrics_location_path(),
    "run_tracking_table"               : RunTrackingTable
}

# COMMAND ----------

dbutils.notebook.exit(creds)

# COMMAND ----------

