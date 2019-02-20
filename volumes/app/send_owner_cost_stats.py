#!/usr/bin/python3

from ExasolDatabaseConnector import Database
import pprint
from datetime import datetime
import boto3
import botocore
import os
import csv
import gzip
import json

exa_staging_tablename = "Costs_Staging"
exa_prod_tablename = "Costs"

columns = [
  {
    "category":"identity",
    "name":"LineItemId",
    "type":"String"
  },{
    "category":"identity",
    "name":"TimeInterval",
    "type":"Interval"
  },{
    "category":"bill",
    "name":"InvoiceId",
    "type":"String"
  },{
    "category":"bill",
    "name":"BillingEntity",
    "type":"String"
  },{
    "category":"bill",
    "name":"BillType",
    "type":"String"
  },{
    "category":"bill",
    "name":"PayerAccountId",
    "type":"String"
  },{
    "category":"bill",
    "name":"BillingPeriodStartDate",
    "type":"DateTime"
  },{
    "category":"bill",
    "name":"BillingPeriodEndDate",
    "type":"DateTime"
  },{
    "category":"lineItem",
    "name":"UsageAccountId",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"LineItemType",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"UsageStartDate",
    "type":"DateTime"
  },{
    "category":"lineItem",
    "name":"UsageEndDate",
    "type":"DateTime"
  },{
    "category":"lineItem",
    "name":"ProductCode",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"UsageType",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"Operation",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"AvailabilityZone",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"ResourceId",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"UsageAmount",
    "type":"BigDecimal"
  },{
    "category":"lineItem",
    "name":"NormalizationFactor",
    "type":"OptionalBigDecimal"
  },{
    "category":"lineItem",
    "name":"NormalizedUsageAmount",
    "type":"OptionalBigDecimal"
  },{
    "category":"lineItem",
    "name":"CurrencyCode",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"UnblendedRate",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"UnblendedCost",
    "type":"BigDecimal"
  },{
    "category":"lineItem",
    "name":"BlendedRate",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"BlendedCost",
    "type":"BigDecimal"
  },{
    "category":"lineItem",
    "name":"LineItemDescription",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"TaxType",
    "type":"String"
  },{
    "category":"lineItem",
    "name":"LegalEntity",
    "type":"String"
  },{
    "category":"product",
    "name":"ProductName",
    "type":"String"
  },{
    "category":"product",
    "name":"accountAssistance",
    "type":"String"
  },{
    "category":"product",
    "name":"alarmType",
    "type":"String"
  },{
    "category":"product",
    "name":"architecturalReview",
    "type":"String"
  },{
    "category":"product",
    "name":"architectureSupport",
    "type":"String"
  },{
    "category":"product",
    "name":"availability",
    "type":"String"
  },{
    "category":"product",
    "name":"bestPractices",
    "type":"String"
  },{
    "category":"product",
    "name":"capacitystatus",
    "type":"String"
  },{
    "category":"product",
    "name":"caseSeverityresponseTimes",
    "type":"String"
  },{
    "category":"product",
    "name":"clockSpeed",
    "type":"String"
  },{
    "category":"product",
    "name":"currentGeneration",
    "type":"String"
  },{
    "category":"product",
    "name":"customerServiceAndCommunities",
    "type":"String"
  },{
    "category":"product",
    "name":"databaseEdition",
    "type":"String"
  },{
    "category":"product",
    "name":"databaseEngine",
    "type":"String"
  },{
    "category":"product",
    "name":"dedicatedEbsThroughput",
    "type":"String"
  },{
    "category":"product",
    "name":"deploymentOption",
    "type":"String"
  },{
    "category":"product",
    "name":"description",
    "type":"String"
  },{
    "category":"product",
    "name":"durability",
    "type":"String"
  },{
    "category":"product",
    "name":"ecu",
    "type":"String"
  },{
    "category":"product",
    "name":"endpointType",
    "type":"String"
  },{
    "category":"product",
    "name":"engineCode",
    "type":"String"
  },{
    "category":"product",
    "name":"enhancedNetworkingSupported",
    "type":"String"
  },{
    "category":"product",
    "name":"fromLocation",
    "type":"String"
  },{
    "category":"product",
    "name":"fromLocationType",
    "type":"String"
  },{
    "category":"product",
    "name":"group",
    "type":"String"
  },{
    "category":"product",
    "name":"groupDescription",
    "type":"String"
  },{
    "category":"product",
    "name":"includedServices",
    "type":"String"
  },{
    "category":"product",
    "name":"instanceFamily",
    "type":"String"
  },{
    "category":"product",
    "name":"instanceType",
    "type":"String"
  },{
    "category":"product",
    "name":"instanceTypeFamily",
    "type":"String"
  },{
    "category":"product",
    "name":"io",
    "type":"String"
  },{
    "category":"product",
    "name":"launchSupport",
    "type":"String"
  },{
    "category":"product",
    "name":"licenseModel",
    "type":"String"
  },{
    "category":"product",
    "name":"location",
    "type":"String"
  },{
    "category":"product",
    "name":"locationType",
    "type":"String"
  },{
    "category":"product",
    "name":"maxIopsBurstPerformance",
    "type":"String"
  },{
    "category":"product",
    "name":"maxIopsvolume",
    "type":"String"
  },{
    "category":"product",
    "name":"maxThroughputvolume",
    "type":"String"
  },{
    "category":"product",
    "name":"maxVolumeSize",
    "type":"String"
  },{
    "category":"product",
    "name":"memory",
    "type":"String"
  },{
    "category":"product",
    "name":"messageDeliveryFrequency",
    "type":"String"
  },{
    "category":"product",
    "name":"messageDeliveryOrder",
    "type":"String"
  },{
    "category":"product",
    "name":"minVolumeSize",
    "type":"String"
  },{
    "category":"product",
    "name":"networkPerformance",
    "type":"String"
  },{
    "category":"product",
    "name":"normalizationSizeFactor",
    "type":"String"
  },{
    "category":"product",
    "name":"operatingSystem",
    "type":"String"
  },{
    "category":"product",
    "name":"operation",
    "type":"String"
  },{
    "category":"product",
    "name":"operationsSupport",
    "type":"String"
  },{
    "category":"product",
    "name":"physicalProcessor",
    "type":"String"
  },{
    "category":"product",
    "name":"preInstalledSw",
    "type":"String"
  },{
    "category":"product",
    "name":"proactiveGuidance",
    "type":"String"
  },{
    "category":"product",
    "name":"processorArchitecture",
    "type":"String"
  },{
    "category":"product",
    "name":"processorFeatures",
    "type":"String"
  },{
    "category":"product",
    "name":"productFamily",
    "type":"String"
  },{
    "category":"product",
    "name":"programmaticCaseManagement",
    "type":"String"
  },{
    "category":"product",
    "name":"provisioned",
    "type":"String"
  },{
    "category":"product",
    "name":"queueType",
    "type":"String"
  },{
    "category":"product",
    "name":"region",
    "type":"String"
  },{
    "category":"product",
    "name":"servicecode",
    "type":"String"
  },{
    "category":"product",
    "name":"servicename",
    "type":"String"
  },{
    "category":"product",
    "name":"sku",
    "type":"String"
  },{
    "category":"product",
    "name":"storage",
    "type":"String"
  },{
    "category":"product",
    "name":"storageClass",
    "type":"String"
  },{
    "category":"product",
    "name":"storageMedia",
    "type":"String"
  },{
    "category":"product",
    "name":"technicalSupport",
    "type":"String"
  },{
    "category":"product",
    "name":"tenancy",
    "type":"String"
  },{
    "category":"product",
    "name":"thirdpartySoftwareSupport",
    "type":"String"
  },{
    "category":"product",
    "name":"toLocation",
    "type":"String"
  },{
    "category":"product",
    "name":"toLocationType",
    "type":"String"
  },{
    "category":"product",
    "name":"training",
    "type":"String"
  },{
    "category":"product",
    "name":"transferType",
    "type":"String"
  },{
    "category":"product",
    "name":"usageFamily",
    "type":"String"
  },{
    "category":"product",
    "name":"usagetype",
    "type":"String"
  },{
    "category":"product",
    "name":"vcpu",
    "type":"String"
  },{
    "category":"product",
    "name":"version",
    "type":"String"
  },{
    "category":"product",
    "name":"volumeType",
    "type":"String"
  },{
    "category":"product",
    "name":"whoCanOpenCases",
    "type":"String"
  },{
    "category":"pricing",
    "name":"RateId",
    "type":"String"
  },{
    "category":"pricing",
    "name":"publicOnDemandCost",
    "type":"BigDecimal"
  },{
    "category":"pricing",
    "name":"publicOnDemandRate",
    "type":"String"
  },{
    "category":"pricing",
    "name":"term",
    "type":"String"
  },{
    "category":"pricing",
    "name":"unit",
    "type":"String"
  },{
    "category":"reservation",
    "name":"AmortizedUpfrontCostForUsage",
    "type":"OptionalBigDecimal"
  },{
    "category":"reservation",
    "name":"AmortizedUpfrontFeeForBillingPeriod",
    "type":"OptionalBigDecimal"
  },{
    "category":"reservation",
    "name":"EffectiveCost",
    "type":"OptionalBigDecimal"
  },{
    "category":"reservation",
    "name":"EndTime",
    "type":"OptionalString"
  },{
    "category":"reservation",
    "name":"ModificationStatus",
    "type":"OptionalString"
  },{
    "category":"reservation",
    "name":"NormalizedUnitsPerReservation",
    "type":"String"
  },{
    "category":"reservation",
    "name":"RecurringFeeForUsage",
    "type":"OptionalBigDecimal"
  },{
    "category":"reservation",
    "name":"StartTime",
    "type":"OptionalString"
  },{
    "category":"reservation",
    "name":"SubscriptionId",
    "type":"String"
  },{
    "category":"reservation",
    "name":"TotalReservedNormalizedUnits",
    "type":"String"
  },{
    "category":"reservation",
    "name":"TotalReservedUnits",
    "type":"String"
  },{
    "category":"reservation",
    "name":"UnitsPerReservation",
    "type":"String"
  },{
    "category":"reservation",
    "name":"UnusedAmortizedUpfrontFeeForBillingPeriod",
    "type":"OptionalBigDecimal"
  },{
    "category":"reservation",
    "name":"UnusedNormalizedUnitQuantity",
    "type":"OptionalBigDecimal"
  },{
    "category":"reservation",
    "name":"UnusedQuantity",
    "type":"OptionalBigDecimal"
  },{
    "category":"reservation",
    "name":"UnusedRecurringFee",
    "type":"OptionalBigDecimal"
  },{
    "category":"reservation",
    "name":"UpfrontValue",
    "type":"OptionalBigDecimal"
  },{
    "category":"resourceTags",
    "name":"aws:createdBy",
    "type":"String"
  },{
    "category":"resourceTags",
    "name":"user:exa:department",
    "type":"String"
  },{
    "category":"resourceTags",
    "name":"user:exa:owner",
    "type":"String"
  },{
    "category":"resourceTags",
    "name":"user:exa:project",
    "type":"String"
  }
]

import argparse
parser = argparse.ArgumentParser()
parser.add_argument(
        "--billing_period", 
        help="The billing period, that should be imported",
        action="store",
        default=""
    )
parser.add_argument(
        "--exa_connectionString", 
        help="Exasol Database Connection String (IP..IP:Port)",
        action="store"
    )
parser.add_argument(
        "--exa_username", 
        help="Exasol Database Username",
    )
parser.add_argument(
        "--exa_password", 
        help="Exasol Database Password",
    )
parser.add_argument(
        "--exa_schema", 
        help="Exasol Database Schema Name",
        action="store"
    )
parser.add_argument(
        "--s3_region", 
        help="S3 Region Name",
        action="store",
        default="eu-central-1"
    )
parser.add_argument(
        "--s3_bucketName", 
        help="S3 Bucket Name",
        action="store",
        default="aws.billing.reports"
    )
args = parser.parse_args()

s3_resource = boto3.resource('s3') 


# S3
def getFile(object_summary):
    try:
        # Download All-1.csv.gz
        print("Downloading " + object_summary.key)
        object_summary.Object().download_file('/tmp/All-1.csv.gz')
        reCreateTable(exa_staging_tablename)
        importCSV('/tmp/All-1.csv.gz')
        mergeStagingToProd()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

def getAllFiles():
    my_bucket = s3_resource.Bucket(args.s3_bucketName)
    # for object in my_bucket.objects.all():
    #     pprint.pprint(object)
    for object_summary in my_bucket.objects.all():
        if ("All-1.csv.gz" in object_summary.key):
            getFile(object_summary)

def getLatestFile():
    # Download latest manifest
    manifest_data = {}
    my_bucket = s3_resource.Bucket(args.s3_bucketName)
    for object_summary in my_bucket.objects.all():
        if ("%s/All-Manifest.json" % args.billing_period in object_summary.key):
          print("Loading manifest from %s" % object_summary.key)
          object_summary.Object().download_file('/tmp/All-Manifest.json')
          with open('/tmp/All-Manifest.json') as f:
            manifest_data = json.load(f)
          break
    # pprint.pprint(manifest_data)

    # Download and import Latest Report
    for object_summary in my_bucket.objects.all():
      if (manifest_data['reportKeys'][0] in object_summary.key):
        getFile(object_summary)

# CSV processing
def importCSV(filename):
    # Open DB
    db = Database(args.exa_connectionString, args.exa_username, args.exa_password, autocommit = False)
    db.execute('OPEN SCHEMA ' + args.exa_schema + ';')

    # Turn on profiling
    # db.execute("alter session set profile='off';")
    # db.execute("alter session set NLS_NUMERIC_CHARACTERS='.,';")
    # db.execute("alter session set NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH:MI:SS.ff3';")
    # db.execute("alter session set profile='on';")

    print("Importing csv")
    db_col_names = []
    for col in columns:
        db_col_names.append( col['category'] + '_' + col['name'].replace(':', '_') )
    db_col_names = ', '.join(db_col_names)

    row_count = 0
    with gzip.open(filename, 'rt') as csvfile:
        # pprint.pprint(csvfile.read())
        reader = csv.DictReader(csvfile)
        data_rows = []
        for row in reader:
            # pprint.pprint(row.keys())
            data = []
            for col in columns:
                csv_col_name = col['category'] + '/' + col['name']
                if csv_col_name in row:
                    # pprint.pprint((csv_col_name, row[csv_col_name]))
                    data.append( db.escapeString(row[csv_col_name]) )
                else:
                    data.append( db.escapeString("") )
            data_rows.append('(' + ', '.join(data) + ')')
            row_count = row_count + 1
            if (row_count % 500) == 0:
                sql = 'INSERT INTO ' + exa_staging_tablename + ' (' + db_col_names + ') values ' + ','.join(data_rows) + ';'
                data_rows = []
                # print(sql)
                db.execute(sql)
                db.execute("commit;")
                print("{} {} rows".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row_count))
                # db.execute("alter session set profile='off';")
                # db.execute("flush statistics;")
                # db.execute("alter session set profile='on';")
                # with open("profile_output.csv", "a") as myfile:
                #     myfile.write(
                #         "\n".join(
                #             map(
                #                 str, 
                #                 db.execute("select * from EXA_STATISTICS.EXA_USER_PROFILE_LAST_DAY where session_id = current_session;")
                #             )
                #         )
                #     )
    db.execute("commit;")
    print("Commited {} rows".format(row_count))


def reCreateTable(tableName):
    db_col_names = []
    for col in columns:
        db_col_names.append( col['category'] + '_' + col['name'].replace(':', '_') + "  VARCHAR(255)")
    db_col_names = ",\n".join(db_col_names)
    sql = "CREATE TABLE " + tableName + " (\n" + db_col_names + "\n);"
    db = Database(args.exa_connectionString, args.exa_username, args.exa_password, autocommit = False)
    db.execute('OPEN SCHEMA ' + args.exa_schema + ';')
    db.execute('DROP TABLE IF EXISTS ' + tableName + ';')
    db.execute(sql)
    db.execute("COMMIT;")
    print("Recreated Table '%s'" % tableName)
    
def mergeStagingToProd():
    db_col_names = []
    db_stage_col_names = []
    for col in columns:
        db_col_names.append( col['category'] + '_' + col['name'].replace(':', '_') )
        db_stage_col_names.append( col['category'] + '_' + col['name'].replace(':', '_') )
    db_stage_col_names = ",\n".join(db_stage_col_names)
    db_col_names = ",\n".join(db_col_names)
    sql = """
      MERGE INTO Costs PROD
          USING Costs_staging STAGE
          ON (PROD.IDENTITY_LINEITEMID = STAGE.IDENTITY_LINEITEMID) AND (PROD.IDENTITY_TIMEINTERVAL = STAGE.IDENTITY_TIMEINTERVAL)
      WHEN NOT MATCHED THEN 
          INSERT (%s) 
          VALUES (%s);
    """ % (db_col_names, db_stage_col_names)
    db = Database(args.exa_connectionString, args.exa_username, args.exa_password, autocommit = False)
    db.execute('OPEN SCHEMA ' + args.exa_schema + ';')
    db.execute(sql)
    db.execute("COMMIT;")
    print("Merged STAGING into PROD")

# call it
# reCreateTable(exa_prod_tablename) # Only if you want to clean the data that was already imported
# getAllFiles() # Import all files from the bucket
getLatestFile() # Import the latest report file from the bucket, that matches "billing_period"
