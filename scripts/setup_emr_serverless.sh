#!/bin/bash

# Setup script for EMR Serverless application and dependencies

# Configuration
APP_NAME="fraud-detection-spark"
S3_BUCKET="panikk-emr-fraud-detect-1337"
EXECUTION_ROLE_NAME="AmazonEMR-ExecutionRole-1755211406596"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== EMR Serverless Setup ===${NC}"

# Step 1: Create S3 bucket if it doesn't exist
echo -e "${YELLOW}Step 1: Setting up S3 bucket...${NC}"
if aws s3 ls "s3://$S3_BUCKET" 2>&1 | grep -q 'NoSuchBucket'; then
    echo "Creating S3 bucket: $S3_BUCKET"
    aws s3 mb "s3://$S3_BUCKET"
else
    echo "S3 bucket $S3_BUCKET already exists"
fi

# Step 2: Export and package dependencies  
echo -e "${YELLOW}Step 2: Creating dependency package...${NC}"
uv export --format requirements-txt --no-hashes --output-file requirements.txt

# Install dependencies to local directory
mkdir -p python-deps
pip install -r requirements.txt -t python-deps/ --no-deps

# Create dependency archive
zip -r python-deps.zip python-deps/
echo "Dependencies packaged in python-deps.zip"


 # Check if we're in the right directory
 if [ ! -f "pyproject.toml" ] || [ ! -d "spark_fraud_detection" ]; then
     echo "Error: Not in project root directory"
     echo "Please run from directory containing pyproject.toml and spark_fraud_detection/"
     exit 1
 fi

 # Clean up any existing uploads
 echo "Cleaning existing uploads..."
 aws s3 rm s3://$BUCKET/versions/v1/code/ --recursive --quiet || true

# Step 3: Upload python-deps to S3
echo -e "${YELLOW}Step 4: Uploading to S3...${NC}"
aws s3 cp python-deps.zip "s3://$S3_BUCKET/versions/v1/code/"

# Step 4: Upload code to S



 # Upload core Python package
 echo "Uploading spark_fraud_detection package..."
 aws s3 sync spark_fraud_detection/ s3://$BUCKET/$PREFIX/spark_fraud_detection/ \
   --exclude "__pycache__/*" \
   --exclude "*.pyc" \
   --exclude "*.pyo" \
   --delete

 # Upload scripts
 echo "Uploading scripts..."
 aws s3 sync scripts/ s3://$BUCKET/$PREFIX/scripts/ \
   --exclude "__pycache__/*" \
   --exclude "*.pyc" \
   --delete

 # Upload configuration data
 echo "Uploading generation configs..."
 aws s3 sync data/generation_configs/ s3://$BUCKET/$PREFIX/data/generation_configs/ \
   --delete


echo -e "${GREEN}Code uploaded to: s3://$S3_BUCKET/versions/v1/code/${NC}"

# Step 5: Create EMR Serverless application (if it doesn't exist)
echo -e "${YELLOW}Step 5: Setting up EMR Serverless application...${NC}"

# Check if application already exists
APP_ID=$(aws emr-serverless list-applications \
  --query "applications[?name=='$APP_NAME'].id" \
  --output text)

if [ -z "$APP_ID" ] || [ "$APP_ID" = "None" ]; then
    echo "Creating EMR Serverless application..."
    APP_RESPONSE=$(aws emr-serverless create-application \
      --name "$APP_NAME" \
      --type "Spark" \
      --release-label "emr-6.15.0" \
      --initial-capacity '{
        "Driver": {
          "workerCount": 1,
          "workerConfiguration": {
            "cpu": "2 vCPU",
            "memory": "4 GB",
            "disk": "20 GB"
          }
        },
        "Executor": {
          "workerCount": 3,
          "workerConfiguration": {
            "cpu": "2 vCPU", 
            "memory": "4 GB",
            "disk": "20 GB"
          }
        }
      }' \
      --maximum-capacity '{
        "cpu": "12 vCPU",
        "memory": "32 GB",
		"disk": "100 GB"
      }' \
      --auto-start-configuration '{
        "enabled": true
      }' \
      --auto-stop-configuration '{
        "enabled": true,
        "idleTimeoutMinutes": 15
      }' \
      --tags '{
        "project": "fraud-detection",
        "environment": "production"
      }')
    
    APP_ID=$(echo "$APP_RESPONSE" | jq -r '.applicationId')
    echo -e "${GREEN}Created EMR Serverless application: $APP_ID${NC}"
else
    echo -e "${GREEN}Using existing EMR Serverless application: $APP_ID${NC}"
fi

# Step 6: Check/Create execution role
echo -e "${YELLOW}Step 6: Checking execution role...${NC}"
ROLE_ARN=$(aws iam get-role --role-name "$EXECUTION_ROLE_NAME" \
  --query 'Role.Arn' --output text 2>/dev/null)

if [ $? -ne 0 ]; then
    echo -e "${RED}Execution role $EXECUTION_ROLE_NAME not found.${NC}"
    echo "Please create the role manually or run:"
    echo "aws iam create-role --role-name $EXECUTION_ROLE_NAME --assume-role-policy-document file://trust-policy.json"
    echo "aws iam attach-role-policy --role-name $EXECUTION_ROLE_NAME --policy-arn arn:aws:iam::aws:policy/AmazonEMRServerlessServiceRolePolicy"
    echo "aws iam attach-role-policy --role-name $EXECUTION_ROLE_NAME --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess"
else
    echo -e "${GREEN}Execution role found: $ROLE_ARN${NC}"
fi

# Step 7: Update job submission scripts with actual values
echo -e "${YELLOW}Step 7: Updating job submission scripts...${NC}"
sed -i "s/your-emr-serverless-app-id/$APP_ID/g" scripts/submit_*_job.sh
sed -i "s/your-s3-bucket/$S3_BUCKET/g" scripts/submit_*_job.sh
if [ ! -z "$ROLE_ARN" ]; then
    sed -i "s|arn:aws:iam::your-account:role/EMRServerlessExecutionRole|$ROLE_ARN|g" scripts/submit_*_job.sh
fi

# Make scripts executable
chmod +x scripts/submit_*_job.sh

echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""
echo "Next steps:"
echo "1. Update the execution role ARN in submit_*_job.sh if needed"
echo "2. Run data generation: ./scripts/submit_datagen_job.sh 50000"
echo "3. Run fraud detection: ./scripts/submit_prediction_job.sh"
echo ""
echo "Application ID: $APP_ID"
echo "S3 Code Location: s3://$S3_BUCKET/emr-serverless/code/"
echo "Job submission scripts: scripts/submit_*_job.sh"
