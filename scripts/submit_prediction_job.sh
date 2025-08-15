#!/bin/bash

# Submit fraud prediction/detection job to EMR Serverless

# Configuration
APPLICATION_ID="00fuqoprdkc3m625"
EXECUTION_ROLE_ARN="arn:aws:iam::801822494933:role/service-role/AmazonEMR-ExecutionRole-1755211406596"
S3_BUCKET="panikk-emr-fraud-detect-1337"
S3_CODE_PREFIX="versions/v1/code"
S3_DATA_PREFIX="versions/v1/input"
S3_OUTPUT_PREFIX="versions/v1/output"

# Job parameters
DATA_PATH=${1:-"s3://$S3_BUCKET/$S3_DATA_PREFIX"}
MODEL_OUTPUT_PATH=${2:-"s3://$S3_BUCKET/$S3_OUTPUT_PREFIX/model"}
PREDICTIONS_PATH=${3:-"s3://$S3_BUCKET/$S3_OUTPUT_PREFIX/predictions"}
JOB_NAME="fraud-detection-$(date +%Y%m%d-%H%M%S)"

echo "Submitting EMR Serverless fraud detection job..."
echo "Application ID: $APPLICATION_ID"
echo "Data path: $DATA_PATH"
echo "Model output: $MODEL_OUTPUT_PATH"
echo "Predictions output: $PREDICTIONS_PATH"

aws emr-serverless start-job-run \
  --application-id $APPLICATION_ID \
  --name "$JOB_NAME" \
  --execution-role-arn $EXECUTION_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://'$S3_BUCKET'/'$S3_CODE_PREFIX'/spark_fraud_detection_code.zip/scripts/run_fraud_detection.py",
      "entryPointArguments": [
        "--data-path", "'$DATA_PATH'",
        "--output-path", "'$MODEL_OUTPUT_PATH'",
        "--predictions-path", "'$PREDICTIONS_PATH'"
      ],
      "sparkSubmitParameters": "--py-files s3://'$S3_BUCKET'/'$S3_CODE_PREFIX'/python-deps.zip --conf spark.executor.cores=2 --conf spark.executor.memory=3g  --conf spark.executor.instances=3 --conf spark.driver.memory=3g"
    }
  }' \
  --configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults", 
        "properties": {
          "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
          "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
          "spark.sql.adaptive.enabled": "true",
          "spark.sql.adaptive.coalescePartitions.enabled": "true",
          "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
          "spark.sql.execution.arrow.pyspark.enabled": "true",
          "spark.sql.adaptive.skewJoin.enabled": "true"
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
    "job-type": "fraud-detection-training",
    "data-source": "'$DATA_PATH'",
    "model-output": "'$MODEL_OUTPUT_PATH'"
  }' \
  --execution-timeout-minutes 180

echo "Job submitted. Check AWS console for job status."
