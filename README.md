# aws-cost-reporting

Collection of Exasol scripts to record and analyze the costs produced by an AWS account.

## Preparation

At the moment we do not support downloading the cost report CSV files directly from S3 into the Exasol database. Instead we assume that you sync those file onto a file system which is accessible by the Exasol database client.

In the following example we show how to sync the file using the AWS CLI. Remember that you have to configure the AWS CLI first if you have not done that already.

```bash
aws configure
```

After this you can start the download.

```bash
cost_report_dir='/data/aws-billing-reports'
bucket_url='s3://com.example.billing.reports'
mkrdir -p "cost_report_dir"
aws s3 sync "$bucket_url" "$cost_report_dir"
```

The cost reports can produce surprising amounts of data. So you will want either store the files temporarily only on your machine (in which case `aws s3 sync` does not work), or store them on cheap space.

## Import

At the moment the import is done "by-hand" using the `IMPORT` SQL command. Future versions will improve this situation by using scripts based on Exasol's internal Lua interpreter.