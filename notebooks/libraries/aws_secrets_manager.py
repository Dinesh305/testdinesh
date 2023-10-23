# Databricks notebook source
import boto3
import json
#added for AWS secrets manager
from botocore.exceptions import ClientError

## refer https://docs.aws.amazon.com/general/latest/gr/asm.html for more information and how to validate
class Secrets_Manager_Service(object):
  
  accepted_region_names = ["us-east-1",
                          "us-east-2",
                          "us-west-2",
                          "us-west-1"]
  
  def __init__(self,secrets_endpoint, region):
    self._secrets_endpoint = secrets_endpoint
    self._set_region(region)  
    
  def _set_region(self,region):
    ### validate before adding region and value 
    try:
      if Secrets_Manager_Service.validate_regions(region):
        self._region_name = region
    except Exception as e:
      raise e
    else:
      pass
    
  def validate_regions(region):
    if region in Secrets_Manager_Service.accepted_region_names:
      return True
    else:
      raise Exception('RegionNotWithInAcceptedList')
    
  def get_secret(self,secret_name,secrets_name_key):
    
        region_name = self._region_name

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            # returns a dict
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
              
            secret_string = get_secret_value_response.get('SecretString')
            fetched_secret =  json.loads(secret_string).get(secrets_name_key)
          
        except ClientError as e:
          
          if e.response['Error']['Code'] == 'DecryptionFailureException':
              # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
              # Deal with the exception here, and/or rethrow at your discretion.
            raise e
          elif e.response['Error']['Code'] == 'InternalServiceErrorException':
              # An error occurred on the server side.
              # Deal with the exception here, and/or rethrow at your discretion.
            raise e
          elif e.response['Error']['Code'] == 'InvalidParameterException':
              # You provided an invalid value for a parameter.
              # Deal with the exception here, and/or rethrow at your discretion.
            raise e
          elif e.response['Error']['Code'] == 'InvalidRequestException':
              # You provided a parameter value that is not valid for the current state of the resource.
              # Deal with the exception here, and/or rethrow at your discretion.
            raise e
          elif e.response['Error']['Code'] == 'ResourceNotFoundException':
              # We can't find the resource that you asked for.
              # Deal with the exception here, and/or rethrow at your discretion.
            raise e
#           else:
#             # Decrypts secret using the associated KMS CMK.
#             # Depending on whether the secret is a string or binary, one of these fields will be populated.
#             if 'SecretString' in get_secret_value_response:
#                 secret = get_secret_value_response['SecretString']
#                 fetched_secret = secret
#             else:
#                 decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
#                 fetched_secret = decoded_binary_secret
                
        return fetched_secret