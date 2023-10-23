# Databricks notebook source
class Datawarehouse(object):
  
  def __init__(self,user, environment):
    self._set_username(user)
    self.environment = environment
  
  def _set_username(self,username):
    self.user_name = username
  
  def _get_passwrd(self): return Exception('Not Implemented')
    
  def populate_jdbc_url(self):
    return Exception('Not Implemented')
  
  def get_jdbc_url(self): return Exception('Not Implemented')

# COMMAND ----------

class RedshiftDatawarehouse(Datawarehouse):
  
  supported_envs = ['dev','dev_dsr','itg','itg_dsr','prod02','prod02_dsr','prod04','prod04_dsr']

  database_metadata = {
   "dev" : {
    "host" : "bdbt-redshift-dev.hp8.us",
    "port" : "5439",
    "sslmode" : "require"
  },
  "itg" : {
    "host" : "bdbt-redshift-itg-02.hp8.us",
    "port" : "5439",
    "sslmode" : "require"
  },
  "prod02" : {
    "host" : "bdbt-redshift-prod-02.hp8.us",
    "port" : "5439",
    "sslmode" : "require"
  },
  "prod04" : {
    "host" : "bdbt-redshift-prod-04.hp8.us",
    "port" : "5439",
    "sslmode" : "require"
  }
}  
  database_metadata['dev_dsr'] = database_metadata['dev']
  database_metadata['itg_dsr'] = database_metadata['itg']
  database_metadata['prod02_dsr'] = database_metadata['prod02']
  database_metadata['prod04_dsr'] = database_metadata['prod04']
  
  url_template_string = "jdbc:redshift://{host}:{port}/{dbname}?user={username}&password={password}&ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory"
  
  def __init__(self, user, environment, dbname):
    super().__init__(user, environment)
    self._set_dbname(dbname)
    
    
  def _set_dbname(self, dbname):
    self._rs_dbname  = dbname
  
  def set_secrets_manager_instance(self,secrets_manager_service):
    # to check if it is type of secrets manager service instance or not.
    if isinstance(secrets_manager_service,Secrets_Manager_Service):
      self._secrets_manager_service = secrets_manager_service
    else:
      raise Exception('NotASecretsManagerServiceInstance')
      
  def get_secrets_manager_instance(self):
    
    if hasattr(self,'_secrets_manager_service'):
      return self._secrets_manager_service
    else:
      raise Exception('SecretsManagerInstanceNotInitialized')
  
  def secrets_manager_service(self):
    
    return self.get_secrets_manager_instance()
  
  def _populate_jdbc_url(self,secret_name:str, secret_phrase:str):
    
    secrets_manager_service = self.get_secrets_manager_instance()
    rs_passwrd = secrets_manager_service.get_secret(secret_name, secret_phrase)
    
    redshift_parameters = RedshiftDatawarehouse.database_metadata.get(self.environment)
    
    if redshift_parameters == None:
      raise Exception('EnvironmentNotFoundException')
    else:
      jdbc_url = RedshiftDatawarehouse.url_template_string.format(username=self.user_name,
                                                                  password=rs_passwrd,
                                                                  dbname=self._rs_dbname,
                                                                  host = redshift_parameters.get('host'),
                                                                  port = redshift_parameters.get('port') )
      self._jdbc_url = jdbc_url
  
  def get_host_url(self):
    redshift_parameters = RedshiftDatawarehouse.database_metadata.get(self.environment)
    host_url = redshift_parameters.get('host')
    return host_url
  
  def get_port(self):
    redshift_parameters = RedshiftDatawarehouse.database_metadata.get(self.environment)
    port = redshift_parameters.get('port')
    return port
  
  def get_ssl_status(self):
    redshift_parameters = RedshiftDatawarehouse.database_metadata.get(self.environment)
    sslmode_status = redshift_parameters.get('sslmode')
    return sslmode_status
  
  def get_jdbc_url(self,secret_name,secret_phrase):
    
    if hasattr(self,"_jdbc_url"):
      return self._jdbc_url
    
    else:
      self._populate_jdbc_url(secret_name,secret_phrase)
    return self._jdbc_url