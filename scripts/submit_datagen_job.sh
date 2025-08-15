#!/bin/bash

# Submit data generation job to EMR Serverless

# Configuration
APPLICATION_ID="00fuqoprdkc3m625"
EXECUTION_ROLE_ARN="arn:aws:iam::801822494933:role/service-role/AmazonEMR-ExecutionRole-1755211406596"
S3_BUCKET="panikk-emr-fraud-detect-1337"
S3_CODE_PREFIX="versions/v1/code"
S3_OUTPUT_PREFIX="versions/v1/input"

# Job parameters
NUM_CUSTOMERS=${1:-100000}
START_DATE=${2:-"2023-01-01"}
END_DATE=${3:-"2023-12-31"}
JOB_NAME="fraud-datagen-$(date +%Y%m%d-%H%M%S)"

echo "Submitting EMR Serverless data generation job..."
echo "Application ID: $APPLICATION_ID"
echo "Customers: $NUM_CUSTOMERS"
echo "Date range: $START_DATE to $END_DATE"
echo "Output: s3://$S3_BUCKET/$S3_OUTPUT_PREFIX"

aws emr-serverless start-job-run \
  --application-id $APPLICATION_ID \
  --name "$JOB_NAME" \
  --execution-role-arn $EXECUTION_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'$S3_BUCKET'/'$S3_CODE_PREFIX'/scripts/generate_to_s3.py",
      "entryPointArguments": [
        "--bucket", "'$S3_BUCKET'",
        "--s3-prefix", "'$S3_OUTPUT_PREFIX'",
        "-n", "'$NUM_CUSTOMERS'",
        "--start-date", "'$START_DATE'",
        "--end-date", "'$END_DATE'",
        "--master", "yarn",
        "--executor-memory", "3g",
        "--executor-cores", "2"
      ],
      "sparkSubmitParameters": "--py-files s3://'$S3_BUCKET'/'$S3_CODE_PREFIX'/python-deps.zip --conf spark.executor.cores=2 --conf spark.executor.memory=3g --conf spark.executor.instances=3 --conf spark.driver.memory=3g"
    }
  }' \
  --configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults",
        "properties": {
		  "spark.hadoop.fs.s3a.path.style.access": "false",
          "spark.hadoop.fs.s3a.endpoint.region": "ap-southeast-1",
          "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
          "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",

          "spark.hadoop.fs.s3a.connection.timeout": "600000",
          "spark.hadoop.fs.s3a.connection.establish.timeout": "600000",
          "spark.hadoop.fs.s3a.socket.recv.buffer": "65536",
          "spark.hadoop.fs.s3a.socket.send.buffer": "65536",
          "spark.hadoop.fs.s3a.attempts.maximum": "20",
          "spark.hadoop.fs.s3a.retry.limit": "10",
          "spark.hadoop.fs.s3a.retry.interval": "5s",
          "spark.hadoop.fs.s3a.connection.maximum": "100",

          "spark.sql.adaptive.enabled": "true",
          "spark.sql.adaptive.coalescePartitions.enabled": "true",
          "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        }
      }
    ],
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://'$S3_BUCKET'/emr-serverless/logs/"
      }
    }
  }' \
  --tags '{
    "job-type": "fraud-data-generation",
    "dataset-size": "'$NUM_CUSTOMERS'",
    "date-range": "'$START_DATE'-to-'$END_DATE'"
  }' \
  --execution-timeout-minutes 120

echo "Job submitted. Check AWS console for job status."
