# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("inputWidgetJson","")
inputWidgetJson = dbutils.widgets.get("inputWidgetJson")

# COMMAND ----------

######################################################################################
#  Do Widgets
######################################################################################

import json

widgets = {
  'tag' : '',                                                    # this is either the version number (manually entered) for IGT
                                                                 # or if DEV, it uses latest-package.txt in SoftwareBucketDir.  It is used to get correct directory in s3 and creating workspace directory names
  'SourceId': 'PELaser',
  'CurrentLandscapeLocation': 'DEV',                             # DEV, ITG 
  'noteBookSet' : 'raw_only_laser',                          # set list noteBookSets for valid set      
  'WorkSpacePath':  '/automated_runs/automated_runs_laser_dev'   # where the workspace directory will be created
}

workSpaceSuffix = ""

# create the widgets with default values
for key, value in json.loads(str(inputWidgetJson)).items():  
  dbutils.widgets.text(key,value)

# get values from widget   
for key, value in json.loads(str(inputWidgetJson)).items():
  temp = dbutils.widgets.get(key)
  exec("%s = '%s'" % (key,temp))   

print SourceId

if SourceId == "PEInk":
  if CurrentLandscapeLocation == "DEV":
    SoftwareBucketDir = "hp-bigdata-software-packages/dailyCI"
  elif CurrentLandscapeLocation == "ITG":
    SoftwareBucketDir = "hp-bigdata-software-packages-itg/releases" 

elif SourceId == "PELaser":
  if CurrentLandscapeLocation == "DEV":
    SoftwareBucketDir = "hp-bigdata-software-packages/dailyCI_laser_pe/dev"
  elif CurrentLandscapeLocation == "ITG":
    SoftwareBucketDir = "hp-bigdata-software-packages-itg/releases_laser_pe"  
elif SourceId == "Enrichment":
  if CurrentLandscapeLocation == "DEV":
    SoftwareBucketDir = "hp-bigdata-software-packages/dailyCI_enrichment/dev"
  elif CurrentLandscapeLocation == "ITG":
    SoftwareBucketDir = "hp-bigdata-software-packages-itg/releases_enrichment"
  elif CurrentLandscapeLocation == "PROD":
    SoftwareBucketDir = "hp-bigdata-software-packages-prod/releases_enrichment"  
  
else:
  raise ValueError('Please choose a valid landscape location! CurrentLandscapeLocation: ', CurrentLandscapeLocation)

noteBookSets = [
"s3_to_enrichment_product_registration",
"s3_to_enrichment_shipment_scout",
"s3_to_enrichment_ink_printer_id_xref",
"s3_to_enrichment_market_demo_acxiom",
"s3_to_enrichment_customer_demo_acxiom",
"s3_to_enrichment_customer_ckm",
"s3_to_enrichment_orca_hp",
"s3_to_enrichment_orca_walmart",
"s3_to_enrichment_trac",
"s3_to_enrichment_ink_fw_request_log",
"s3_to_enrichment_laser_printer_id_xref"

]  

if noteBookSet not in noteBookSets:
  raise ValueError('Please choose a valid notebook set from noteBookSets above! noteBookSet: ', noteBookSet)

print SoftwareBucketDir

CommonDirectory = "/Library/enrichment_libraries"

# COMMAND ----------

######################################################################################
#  Get Notebook Locations
#  list notebooks dir in package directory that you want to import 
#  i.e. notebooks/databricks-workflow-pipeline/workflow-pipeline-to-git   
#       this is where notebooks exist in s3 dir that contains package.
######################################################################################
notebooks_s3_to_enrichment_product_registration = [
  "notebooks/product_registration_ckm"
]

notebooks_s3_to_enrichment_shipment_scout = [
  "notebooks/channel_shipments_scout"
]

notebooks_s3_to_enrichment_ink_printer_id_xref = [
  "notebooks/ink_printer_id_xref"
]

notebooks_s3_to_enrichment_market_demo_acxiom = [
  "notebooks/market_demo_acxiom"
]

notebooks_s3_to_enrichment_customer_demo_acxiom = [
  "notebooks/customer_demo_acxiom"
]

notebooks_s3_to_enrichment_customer_ckm = [
  "notebooks/customer_ckm"
]

notebooks_s3_to_enrichment_orca_hp = [
  "notebooks/orca_hp"
]

notebooks_s3_to_enrichment_orca_walmart = [
  "notebooks/orca_walmart"
]

notebooks_s3_to_enrichment_trac = [
  "notebooks/trac"
]

notebooks_s3_to_enrichment_ink_fw_request_log = [
  "notebooks/ink_fw_request_log"
]

notebooks_s3_to_enrichment_laser_printer_id_xref = [
  "notebooks/laser_printer_id_xref"
]

if CurrentLandscapeLocation != "DEV":  
  notebooks_s3_to_enrichment_product_registration = ["notebooks"]
  notebooks_s3_to_enrichment_shipment_scout = ["notebooks"]
  notebooks_s3_to_enrichment_ink_printer_id_xref = ["notebooks"] 
  notebooks_s3_to_enrichment_market_demo_acxiom = ["notebooks"]
  notebooks_s3_to_enrichment_customer_demo_acxiom = ["notebooks"]
  notebooks_s3_to_enrichment_customer_ckm = ["notebooks"]
  notebooks_s3_to_enrichment_orca_hp = ["notebooks"]
  notebooks_s3_to_enrichment_orca_walmart = ["notebooks"]
  notebooks_s3_to_enrichment_trac = ["notebooks"]
  notebooks_s3_to_enrichment_ink_fw_request_log = ["notebooks"]
  notebooks_s3_to_enrichment_laser_printer_id_xref = ["notebooks"]

# COMMAND ----------

##################################################
# manifest files (expected notebooks to assemble from s3)
# code checks manifest and deletes extra notebooks and report missing notebooks.
# if this manifest is empty, then all notebooks will be imported
# example
# manifest = [
#  "notebook1",  
#  "notebook2"
# ]
##################################################
manifest_enrichment_product_registration = [
"DQDictionaryMap",
"BAT",
"Enrichment_ProductRegistration_DQ_Validation",
"dq_summary",
"load-product-registration-ckm-inc",
"pre_check_RS",
"ValidateRedshiftCount",
"workflow_invoker3",
"DQValidationLibrary",
"source_key_constants",
"prod_load",
"workflow_params"
]

manifest_enrichment_shipment_scout = [
  "DQDictionaryMap",
  "Enrichment_ShipmentScout_DQ_Validation",
  "dq_summary",
  "channel-shipments-scout",
  "pre_check_RS",
  "ValidateRedshiftCount",
  "DQValidationLibrary",
  "shipments_scout_BAT",
  "workflow_invoker3",
  "source_key_constants",
  "prod_load",
  "workflow_params"
]

manifest_enrichment_ink_printer_id_xref = [
"DQDictionaryMap",
"ink_printer_xref_BAT", #BAT
"dq_summary",
"ink_printer_xref_DQ_test", #rename from EnrichmentTest
"ink-printer-xref", #rename from automated-refactored-ink-printer-xref-load-inc
"pre_check_RS",
"ValidateRedshiftCount",
"DQValidationLibrary",
"workflow_invoker3", 
"source_key_constants",
"prod_load",
"workflow_params"
]

manifest_enrichment_market_demo_acxiom = [
"DQDictionaryMap",
"Enrichment_market_demo_acxiom_DQ_Validation",
"dq_summary",
"market-demo-acxiom",
"pre_check_RS",
"ValidateRedshiftCount",
"DQValidationLibrary", 
"BAT",
"workflow_invoker3",
"source_key_constants",
"prod_load",
"workflow_params"
]

manifest_enrichment_customer_demo_acxiom = [
"DQDictionaryMap",
"Enrichment_customer_demo_acxiom_DQ_Validation",
"dq_summary",
"customer-demo-acxiom",
"pre_check_RS",
"ValidateRedshiftCount",
"DQValidationLibrary", 
"BAT",
"workflow_invoker3",
"source_key_constants",
"prod_load",
"workflow_params"
]

manifest_enrichment_customer_ckm = [
"DQDictionaryMap",
"Enrichment_customer_ckm_DQ_Validation",
"dq_summary",
"customer-ckm",
"pre_check_RS",
"ValidateRedshiftCount",
"DQValidationLibrary", 
"BAT",
"workflow_invoker3",
"source_key_constants",
"prod_load",
"workflow_params"
]

manifest_enrichment_orca_hp = [
"DQDictionaryMap",
"Enrichment_orca_hp_DQ_Validation",
"dq_summary",
"channel-sellout-orca-hp",
"pre_check_RS",
"ValidateRedshiftCount",
"DQValidationLibrary", 
"Enrichment_BasicValidation",
"workflow_invoker2",
"source_key_constants",
"prod_load"
]

manifest_enrichment_orca_walmart = [
"DQDictionaryMap",
"Enrichment_orca_walmart_DQ_Validation",
"dq_summary",
"channel-sellout-orca-walmart",
"pre_check_RS",
"ValidateRedshiftCount",
"DQValidationLibrary", 
"BAT",
"workflow_invoker3",
"source_key_constants",
"prod_load",
"workflow_params"
]

manifest_enrichment_trac = [
"trac",
"pre_check_RS",
"ValidateRedshiftCount_trac",
"BasicValidation_trac",
"workflow_invoker2_trac",
"source_key_constants",
"enrich_prod_load_trac"
]

manifest_enrichment_ink_fw_request_log = [
"ink_fw_request_log",
"Enrichment_BasicValidation_ink_fw_request_log",
"workflow_invoker3_ink_fw",
"source_key_constants",
"workflow_params"
]

manifest_enrichment_laser_printer_id_xref = [
"DQDictionaryMap",
"BAT",
"dq_summary",
"Enrichment_laser_xref_DQ_Validation",
"laser-printer-xref",
"pre_check_RS",
"ValidateRedshiftCount",
"DQValidationLibrary",
"workflow_invoker3", 
"source_key_constants",
"prod_load_laser_xref",
"workflow_params"
]

# COMMAND ----------

#####################################################################
# set noteBookSet varibles
#####################################################################
if noteBookSet == "s3_to_enrichment_product_registration":  
  s3notebooksDirList = notebooks_s3_to_enrichment_product_registration
  manifest = manifest_enrichment_product_registration
  workSpaceSuffix = "/ProductRegistration"

if noteBookSet == "s3_to_enrichment_shipment_scout":  
  s3notebooksDirList = notebooks_s3_to_enrichment_shipment_scout
  manifest = manifest_enrichment_shipment_scout
  workSpaceSuffix = "/ShipmentScout"
  
if noteBookSet == "s3_to_enrichment_ink_printer_id_xref":  
  s3notebooksDirList = notebooks_s3_to_enrichment_ink_printer_id_xref
  manifest = manifest_enrichment_ink_printer_id_xref
  workSpaceSuffix = "/PrinterIdXref"
  
if noteBookSet == "s3_to_enrichment_market_demo_acxiom":  
  s3notebooksDirList = notebooks_s3_to_enrichment_market_demo_acxiom
  manifest = manifest_enrichment_market_demo_acxiom
  workSpaceSuffix = "/MarketAcxiom"
  
if noteBookSet == "s3_to_enrichment_customer_demo_acxiom":  
  s3notebooksDirList = notebooks_s3_to_enrichment_customer_demo_acxiom
  manifest = manifest_enrichment_customer_demo_acxiom
  workSpaceSuffix = "/CustomerAcxiom"
  
if noteBookSet == "s3_to_enrichment_customer_ckm":  
  s3notebooksDirList = notebooks_s3_to_enrichment_customer_ckm
  manifest = manifest_enrichment_customer_ckm
  workSpaceSuffix = "/CustomerCkm"
  
if noteBookSet == "s3_to_enrichment_orca_hp":  
  s3notebooksDirList = notebooks_s3_to_enrichment_orca_hp
  manifest = manifest_enrichment_orca_hp
  workSpaceSuffix = "/OrcaHP"
  
if noteBookSet == "s3_to_enrichment_orca_walmart":  
  s3notebooksDirList = notebooks_s3_to_enrichment_orca_walmart
  manifest = manifest_enrichment_orca_walmart
  workSpaceSuffix = "/OrcaWalmart"
  
if noteBookSet == "s3_to_enrichment_trac":  
  s3notebooksDirList = notebooks_s3_to_enrichment_trac
  manifest = manifest_enrichment_trac
  workSpaceSuffix = "/trac"
  
if noteBookSet == "s3_to_enrichment_ink_fw_request_log":  
  s3notebooksDirList = notebooks_s3_to_enrichment_ink_fw_request_log
  manifest = manifest_enrichment_ink_fw_request_log
  workSpaceSuffix = "/ink_fw_request_log"
  
if noteBookSet == "s3_to_enrichment_laser_printer_id_xref":  
  s3notebooksDirList = notebooks_s3_to_enrichment_laser_printer_id_xref
  manifest = manifest_enrichment_laser_printer_id_xref
  workSpaceSuffix = "/laser_printer_id_xref"
  
manifest_length = len(manifest)


# COMMAND ----------

#####################################################################
# Define needed python functions
#####################################################################

from datetime import datetime
import json
from pathlib2 import Path
import json
import requests

def get_timestamp():
  myts = "{:%Y-%m-%d-%H%M%S}".format(datetime.now())
  return myts


def GetLocalFileContent(path):
  output = Path(path).read_text()
  return output

def PutLocalFileContent(path, content):
  file = open(path,"w") 
  file.write(content) 
  file.close() 


def GetLocalJSON(path):
  Input = Path(path).read_text()
  j = json.loads(Input)
  return j


def GetLastWorkspacePath(j):
  LastWorkspacePath = ""
  for obj in j['objects']:
    if obj['object_type'] == "DIRECTORY":
      if obj['path'] > LastWorkspacePath:
        LastWorkspacePath = obj['path']
  return LastWorkspacePath


def DeleteWorkspace(object):
  payload = {  "path": object, "recursive": True }
  res = requests.post(databricks_shard + "/api/2.0/workspace/delete", json=payload, auth=(DBAdminUser, password ))

def mountBucket(bucket):
# if bucket is mounted return current mnt point, else mount and return new mnt point  
  myMounts = dbutils.fs.mounts()

  mnt_point = "/mnt/" + bucket
  for myMount in myMounts:
    if mnt_point == myMount[0]:
      return myMount[0]
    
  # if no return create mount
  s3bucket = "s3a://" + bucket
  mountPoint = "/mnt/" + bucket
  print ('run dbutils.fs.mount("%s", "%s")' % (s3bucket, mountPoint))
  dbutils.fs.mount(s3bucket, mountPoint)
  return mountPoint

# bucket is first part of path, key is everything afterward bucket/dir/path/to/file.txt bucket = "bucket" key = "dir/path/to/file.txt"
def GetBucketPath(inSTR):
  plist = inSTR.split("/")
  last = plist.__len__()
  
  bucket = plist[0]
  
  path = ""
  for i in range(1, last):
    if i == 1: 
      path = plist[i]
    else:
      path = path + "/" + plist[i]
  
  return bucket, path




# COMMAND ----------

if CurrentLandscapeLocation == "DEV":
  if SourceId == "PEInk":
    latest_package_txt = mountBucket("hp-bigdata-software-packages") + "/dailyCI/latest-package.txt"
    #print latest_package_txt
    tag = dbutils.fs.head(latest_package_txt)
    tag = tag.strip()
    #print tag
    tag_ws = tag[:-5]
 
  elif SourceId == "PELaser":
    latest_package_txt = mountBucket("hp-bigdata-software-packages") + "/dailyCI_laser_pe/dev/latest-package.txt"
    #print latest_package_txt
    tag = dbutils.fs.head(latest_package_txt)
    tag = tag.strip()
    #print tag
    tag_ws = tag[:-5]  
  elif SourceId == "Enrichment":
    latest_package_txt = mountBucket("hp-bigdata-software-packages") + "/dailyCI_enrichment/dev/latest-package.txt"    
    tag = dbutils.fs.head(latest_package_txt)
    tag = tag.strip()
    #print tag
    tag_ws = tag[:-5]  
    
else:
  tag_ws = tag
  
print tag  
print tag_ws

# COMMAND ----------

#####################################################################
# Define needed workspace varibles
#####################################################################
#WorkSpacePath = "/1_std_raw_refactor/automated_daily_runs/pepto-CI"
if CurrentLandscapeLocation == "DEV":
  WorkSpacePath = WorkSpacePath + workSpaceSuffix
else:  
  WorkSpacePath = WorkSpacePath + "/" + tag_ws + "_" + get_timestamp()
print ("WorkSpacePath: " + WorkSpacePath)  
  
######################################################################################
#  get password for DBAdminUse
######################################################################################

if CurrentLandscapeLocation == "DEV":
  password =           GetLocalFileContent("/dbfs/tmp/password-file")    
  DBAdminUser =        "bigdata-dev-databricks-process@external.groups.hp.com"
  databricks_shard =   "https://hp-bigdata.cloud.databricks.com"
elif CurrentLandscapeLocation == "ITG":
  password =          GetLocalFileContent("/dbfs/tmp/password-file-itg")        
  DBAdminUser =       "bigdata-itg-databricks-admin@external.groups.hp.com"
  databricks_shard =  "https://hp-bigdata-itg.cloud.databricks.com"
elif CurrentLandscapeLocation == "PROD":
  password =          GetLocalFileContent("/dbfs/tmp/password-file-prod")    
  DBAdminUser =       "bigdata-prod-databricks-admin@external.groups.hp.com"
  databricks_shard =   "https://hp-bigdata-itg.cloud.databricks.com"
else:
  raise ValueError('Please choose a valid landscape location! CurrentLandscapeLocation: ', CurrentLandscapeLocation)



# COMMAND ----------

######################################################################################
#  Define os base64 load workspace python functions
######################################################################################
import json
import requests
import base64
import os

def os_get_file_list(path):
  return os.listdir(path)

def makeWorkspaceDir(path):
  
  payload = { "path": path}
  res = requests.post(databricks_shard + "/api/2.0/workspace/mkdirs", json=payload, auth=(DBAdminUser, password ))
  return res

def importNotebooks(fileList, filePathDir, WorkSpacePath, DBAdminUser, password):

  validExts = ["py","scala","sql","r"]
  validLANGs = {
    'py' : 'PYTHON',
    'sql' : 'SQL',
    'r' : 'R',
    'scala' : 'SCALA'
  }
  
  for file in fileList:
    
    fileSplit = file.split(".")
    fileExt = fileSplit[len(fileSplit) - 1]
    
    if fileExt in validExts:
      filePath = filePathDir + "/" + file
      fileNOExt = fileSplit[len(fileSplit) - 2] 
      WorkSpacePathFile = WorkSpacePath + "/" + fileNOExt
      print ("importing " + file + " to ws: " + WorkSpacePathFile)
      LANG = validLANGs[fileExt]
      print ("language: " + LANG)
      importOneNotebook(filePath, WorkSpacePathFile, LANG, DBAdminUser, password)

def importOneNotebook(filePath, WorkSpacePathFile, LANG, DBAdminUser, password):      
  
  # encode file to base64
  file = open(filePath, 'r')
  f1 = file.read()
  file.close()
  encoded64 = base64.b64encode(f1)

  payload = {
    "content": encoded64,
    "path": WorkSpacePathFile,
    "language": LANG,
    "overwrite": True,
    "format": "SOURCE"
  }

  res = requests.post(databricks_shard + "/api/2.0/workspace/import", json=payload, auth=(DBAdminUser, password ))
  return res



# COMMAND ----------

##############################################################
# Create Workspace
##############################################################

res = makeWorkspaceDir(WorkSpacePath)
print res.text

res = makeWorkspaceDir(CommonDirectory)
print res.text

# COMMAND ----------

######################################################################################
#  Create Paths for Notebooks in S3 and load notebooks into db workspace
######################################################################################

bucket, path = GetBucketPath(SoftwareBucketDir)

LinuxBaseDir = "/dbfs" + mountBucket(bucket) + "/" + path + "/" + tag + "/"

for dir in s3notebooksDirList:
  dir2process = LinuxBaseDir + dir
  print dir2process
  listOfNotebooks = os.listdir(dir2process)
  print listOfNotebooks
  importNotebooks(listOfNotebooks, dir2process, WorkSpacePath, DBAdminUser, password) 


# COMMAND ----------

for commondir in s3notebooksDirList:
  dir2process = LinuxBaseDir + dir
  print dir2process
  listOfNotebooks = ["credentials_for_enrichment.py","enrich_dq_module.py"]
  importNotebooks(listOfNotebooks, dir2process, CommonDirectory, DBAdminUser, password) 


# COMMAND ----------

##############################################################
# Check Manifest Files (if none, nothing is done)
##############################################################
if manifest_length > 0:   

  #payload = {'job_id': job_id, 'notebook_params': {"RenameJSONList": "False", "S3JSONList": S3JSONList, "start_time_job": start_time_job} }
  print("WSP: " + WorkSpacePath )
  payload = { 'path': WorkSpacePath }

  res = requests.get(databricks_shard + "/api/2.0/workspace/list", json=payload, auth=(DBAdminUser, password ))
  #print(res.json())
  #print(res.text)

  notebooks = res.json()['objects']

  for each in notebooks:
    temp=each['path'].split("/")  # get notebook name
    NBName = temp[-1]
    if not (NBName in manifest):
      print (NBName + " not in manifest.  Deleting.")
      DeleteWorkspace(each['path'])             

  payload = { 'path': WorkSpacePath }
  res = requests.get(databricks_shard + "/api/2.0/workspace/list", json=payload, auth=(DBAdminUser, password ))
  notebooks = res.json()['objects']


  # check if all notebooks in new workspace are in manifest, if not report
  wsList = []
  for each in notebooks:
    temp=each['path'].split("/")  # get notebook name
    NBName = temp[-1]
    wsList.append(NBName) 

  for each in manifest:
    if not (each in wsList):
      print (each + " is in manifest but not in current workspace - " + WorkSpacePath)

# COMMAND ----------

dbutils.notebook.exit(WorkSpacePath)