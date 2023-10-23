# Databricks notebook source
import collections
DQDictionary = collections.OrderedDict()
DQDictionary['DQ1'] = "DQ Check : DQ1 - Invalid SN"
DQDictionary['DQ2'] = "DQ Check : DQ2 - Invalid UUID"
DQDictionary['DQ3'] = "DQ Check : DQ3 - Invalid PN"
DQDictionary['DM4'] = "DM Check : DM4 - Modification of SN"
DQDictionary['DQ5'] = "DQ Check : DQ5 - SN w multi PNs"
DQDictionary['DQ6'] = "DQ Check : DQ6 - SKEY w multi PNs"
DQDictionary['DQ7'] = "DQ Check : DQ7 - PN+SN w multi SKEYs"
DQDictionary['DQ8'] = "DQ Check : DQ8 - PN+SKEY w multi SNs"
DQDictionary['DQ9'] = "DQ Check : DQ9 - Corroborating keys"
DQDictionary['DQ10'] = "DQ Check : DQ10 - Non-ascii chars"
DQDictionary['DQ11'] = "DQ Check : DQ11 - Ship to Country Check"
DQDictionary['DQ13'] = "DQ Check : DQ13 - Registration Date Check"
DQDictionary['DM12'] = "DM Check : DM12 - Product Number Check"
DQDictionary['DM14'] = "DM Check : DM14 - Ship Address Check"
DQDictionary['DM15'] = "DM Check : DM15 - Ship to Country Check"
DQDictionary['DM16'] = "DM Check : DM16 - Channel Patterns Check"
DQDictionary['DM17'] = "DM Check : DM17 - Business Patterns Check"
DQDictionary['DQ18'] = "DQ Check : DQ18 - CID Check"
DQDictionary['DM19'] = "DM Check : DM19 - Segment_Cd Description Check"
DQDictionary['DM20'] = "DM Check : DM20 - Area Code Check"
DQDictionary['DM21'] = "DM Check : DM21 - Iso Country Replacement Check"
DQDictionary['DM22'] = "DM Check : DM22 - State Code Replacement Check"
DQDictionary['DM23'] = "DM Check : DM23 - Hps Customer Type Check"
DQDictionary['DM24'] = "DM Check : DM24 - Character Translation Issue Check"
DQDictionary['DQ25'] = "DQ Check : DQ25 - "
DQDictionary['DM26'] = "DM Check : DM26 - "
DQDictionary['DM27'] = "DM Check : DM27 - Wrong Length Check"
DQDictionary['DM28'] = "DM Check : DM28 - Standardize Unknown Values Check"
DQDictionary['DM29'] = "DM Check : DM29 - Standardize Business Values Check"
DQDictionary['DM30'] = "DM Check : DM30 - Standardize City-State Format Check"
DQDictionary['DM31'] = "DM Check : DM31 - "
DQDictionary['DM32'] = "DM Check : DM32 - Product Number (Pn) Longer Than 8 Characters Check"
DQDictionary['DQ33'] = "DQ Check : DQ33 - Product Number (Pn) Lengths Check"
DQDictionary['DQ34'] = "DQ Check : DQ34 - Length Check"
DQDictionary['DQ35'] = "DQ Check : DQ35 - Regex Check"
DQDictionary['DQ36'] = "DQ Check : DQ36 - Entity Description Check"
DQDictionary['DM37'] = "DM Check : DM37 - Non-Critical Invalid Entry Check"
DQDictionary['DQ38'] = "DQ Check : DQ38 - Value in List Check"
DQDictionary['DM39'] = "DM Check : DM39 - Remove Preceeding 0s Check"
DQDictionary['DM40'] = "DM Check : DM40 - Lookup Code Check"
DQDictionary['DQ41'] = "DQ Check : DQ41 - Hex Characters Check"
DQDictionary['DM42'] = "DM Check : DM42 - Replace Null Values Check"
DQDictionary['DM43'] = "DM Check : DM43 - Decode Values Check"
DQDictionary['DM44'] = "DM Check : DM44 - Specified Invalid Values Check"
DQDictionary['DM45'] = "DM Check : DM45 - Subset Total Check"
DQDictionary['DM46'] = "DM Check : DM46 - Numeric Characters Only Check"
DQDictionary['DM47'] = "DM Check : DM47 - Matching Fields Check"
DQDictionary['DM48'] = "DM Check : DM48 - Subgroup Fields Check"
DQDictionary['DM49'] = "DM Check : DM49 - Mask PII Values Check"
DQDictionary['DM50'] = "DM Check : DM50 - Clean Non-Standard Similar Values Check"
DQDictionary['DM51'] = "DM Check : DM51 - Case (SQL) Check"
DQDictionary['DM52'] = "DM Check : DM52 - Alphanumeric and Single Spaces Check"
DQDictionary['DM53'] = "DM Check : DM53 - Date Check"
DQDictionary['DM54'] = "DM Check : DM54 - Postal Code Regex Check"
DQDictionary['DM55'] = "DM Check : DM55 - Currency Codes Check"
DQDictionary['DM56'] = "DM Check : DM56 - Letters Only Check"
DQDictionary['DM57'] = "DM Check : DM57 - Letters and Single Spaces Only Check"
DQDictionary['DM58'] = "DM Check : DM58 - Strip Quotation Marks Check"
DQDictionary['DM59'] = "DM Check : DM59 - Fix String Encoding Check"
DQDictionary['DM60'] = "DM Check : DM60 - Address Removal by Extension Check"
DQDictionary['DM61'] = "DM Check : DM61 - String Translate"
DQDictionary['DM62'] = "DM Check : DM62 - String Translate Complement"
DQDictionary['DM63'] = "DM Check : DM63 - Remove Non-Printable Characters"
DQDictionary['DM64'] = "DM Check : DM64 - Negative Values"
DQDictionary['DM65'] = "DM Check : DM65 - Starting Substring"
DQDictionary['DQ66'] = "DQ Check : DQ66 - Non-numeric records check"
DQDictionary['DQ67'] = "DQ Check : DQ67 - Records not having required values check"
DQDictionary['DQ68'] = "DQ Check : DQ68 - Not matching cohort pairs check"
DQDictionary['DM69'] = "DM Check : DM69 - Fill null cohort pairs"
DQDictionary['DQ70'] = "DQ Check : DQ70 - Null Record Check"
DQDictionary['DQ71'] = "DQ Check : DQ71 - Compare two dates"
DQDictionary['DQ72'] = "DQ Check : DQ72 - Check for valid date"
DQDictionary['DQ73'] = "DQ Check : DQ73 -  Check valid datetime format(generic rule)"
DQDictionary['DQ74'] = "DQ Check : DQ74 - Mark if printer_id_key = serial_number"
DQDictionary['DQ75'] = "DQ Check : DQ75 - Mark if record is not present in more than or equal to 75% of data sources"
DQDictionary['DQ76'] = "DQ Check : DQ76 - Mark only Alphabetic"
DQDictionary['DQ80'] = "DQ Check : DQ80 - Returns a DQ for value in column is not in given range"
DQDictionary['DQ81'] = "DQ Check : DQ81 - Returns a DQ if value is not in valid list (enum), nulls not allowed"
DQDictionary['DQ82'] = "DQ Check : DQ82 - Returns a DQ if there is No match with our supplied regex"
DQDictionary['DM83'] = "DM Check : DM83 - Returns a DM if value is not in valid list (enum)"
DQDictionary['DM84'] = "DM Check : DM84 - Returns a DM if nulls are replaced with valid values"
DQDictionary['DQ85'] = "DQ Check : DQ85 - Returns a DQ if value is not in valid list (enum), Nulls are allowed"
DQDictionary['DQ998'] = "DQ Check : DQ998 - DSR Record Check"
DQDictionary['DQ999'] = "DQ Check : DQ999 - DSR Record Check"
DQDictionary['DQ93'] = "DQ Check : DQ93 - Return DQ if regex not matching"
DQDictionary['DQ94'] = "DQ Check : DQ94 - Null Record Check FOR Two column combinations"

# COMMAND ----------

dbutils.notebook.exit(DQDictionary)