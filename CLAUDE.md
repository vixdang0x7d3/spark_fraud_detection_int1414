# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a distributed fraud detection system for INT1414 Distributed Database Course. The project includes:
- A Spark-native fraud transaction data generator (`spark_fraud_detection/`)
- Legacy data generator (`sparkov_gen/`) - still referenced in existing docs
- Dockerized Spark cluster with MinIO object storage
- Machine learning fraud detection pipeline using RandomForest
- Goal to handle >100GB of data across multiple VMs

## Key Commands

### Environment Setup
```bash
# Install dependencies using uv (Python 3.11+ required)
uv sync

# Or using pip
pip install -r requirements.txt

# Install AWS CLI (for S3 integration)
uv add awscli
# Or: pip install awscli

# Configure AWS credentials (one-time setup)
aws configure
# Enter: Access Key ID, Secret Access Key, Region (e.g. us-east-1), Output format (json)

# Verify AWS setup
aws sts get-caller-identity

# Start Spark cluster with MinIO
docker-compose up -d

# Scale workers (default: 1 worker)
docker-compose up --scale spark-worker=3

# Stop the cluster
docker-compose down
```

### Data Generation (Spark-Native)
```bash
# Generate fraud data using Spark (recommended approach)
uv run python -m spark_fraud_detection.generation.datagen \
  -n <NUMBER_OF_CUSTOMERS> \
  -o <OUTPUT_PATH> \
  --start_date <START_DATE> \
  --end_date <END_DATE>

# Example: Generate 10K customers with full year of transactions
uv run python -m spark_fraud_detection.generation.datagen \
  -n 10000 \
  -o ./data/devset \
  --start_date 2023-01-01 \
  --end_date 2023-12-31

# Optional parameters:
# --seed <INT> (default: 42)
# --master <SPARK_MASTER> (default: local[*])
# --app_name <NAME> (default: FraudDataGenerator)

# Submit to Spark cluster
uv run python -m spark_fraud_detection.generation.datagen \
  -n 50000 \
  -o ./data/cluster_output \
  --master spark://localhost:7077 \
  --start_date 2023-01-01 \
  --end_date 2023-12-31
```

### Fraud Detection (Machine Learning)
```bash
# Run fraud detection on generated data
uv run python -m spark_fraud_detection.ml.fraud_detector \
  --data-path ./data/devset \
  --model-output ./models/fraud_detection_model \
  --master local[*]

# Run fraud detection on Spark cluster
uv run python -m spark_fraud_detection.ml.fraud_detector \
  --data-path ./data/devset \
  --model-output ./models/fraud_detection_model \
  --master spark://localhost:7077

# Use convenience script for fraud detection
uv run python scripts/run_fraud_detection.py \
  --data-path ./data/devset \
  --output-dir ./output

# Adjust PCA components (default: 18)
uv run python -m spark_fraud_detection.ml.fraud_detector \
  --data-path ./data/devset \
  --pca-components 15
```

### Development and Analysis
```bash
# Launch interactive notebooks (Marimo)
uv run marimo edit notebooks/test_cluster.py
uv run marimo edit notebooks/devset_analysis.py

# Access Spark Web UIs (when cluster is running)
# - Master UI: http://localhost:9090
# - History Server: http://localhost:18080  
# - MinIO Console: http://localhost:9001 (admin/minioadmin123)
```

### Testing and Validation
```bash
# No formal test suite currently - use examples and notebooks for validation
# Check data generation output
uv run python -c "import duckdb; print(duckdb.sql('SELECT COUNT(*) FROM parquet_scan(\"./data/devset/**/*.parquet\")'))"

# Verify Spark cluster connectivity
uv run python notebooks/test_cluster.py

# Test AWS S3 configuration
aws s3 ls
uv run python scripts/generate_to_s3.py --bucket test-bucket --dry-run
```

### AWS CLI Quick Setup
```bash
# Complete AWS setup workflow
# 1. Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install

# 2. Configure credentials
aws configure

# 3. Create project buckets
aws s3 mb s3://your-fraud-data-bucket
aws s3 mb s3://your-fraud-emr-logs

# 4. Test configuration
aws sts get-caller-identity
uv run python scripts/generate_to_s3.py --bucket your-fraud-data-bucket --dry-run

# 5. Create EMR service roles (one-time)
aws emr create-default-roles

# See docs/aws-setup.md for detailed instructions
```

### S3 Data Generation
```bash
# Generate directly to AWS S3
uv run python scripts/generate_to_s3.py \
  --bucket my-fraud-data \
  --s3-prefix fraud-dataset-2023 \
  -n 100000 \
  --start-date 2023-01-01 \
  --end-date 2023-12-31

# Generate to MinIO (local development)
uv run python scripts/generate_to_s3.py \
  --bucket fraud-data \
  --s3-endpoint http://localhost:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin123 \
  --no-ssl \
  -n 10000

# Test S3 configuration (dry run)
uv run python scripts/generate_to_s3.py \
  --bucket my-bucket \
  --dry-run

# Generate with environment variables
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
uv run python scripts/generate_to_s3.py --bucket my-bucket -n 50000

# Submit to Spark cluster with S3 output
uv run python scripts/generate_to_s3.py \
  --bucket cluster-fraud-data \
  --master spark://localhost:7077 \
  -n 500000 \
  --verbose
```

### Cloud Deployment (EMR)
```bash
# Upload code to S3 first
aws s3 cp --recursive spark_fraud_detection/ s3://your-bucket/code/spark_fraud_detection/

# Use the S3 script directly on EMR cluster
aws emr add-steps --cluster-id j-XXXXX --steps '[{
  "Name": "Generate S3 Fraud Data",
  "HadoopJarStep": {
    "Jar": "command-runner.jar",
    "Args": [
      "spark-submit",
      "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
      "s3://your-bucket/code/spark_fraud_detection/scripts/generate_to_s3.py",
      "--bucket", "your-output-bucket",
      "--s3-prefix", "large-fraud-dataset",
      "-n", "1000000",
      "--master", "yarn"
    ]
  }
}]'
```

## Architecture Overview

### Data Generation Pipeline
```
Customer Profiles → Demographic Segmentation → Transaction Generation → Fraud Injection → Parquet Output
     ↓                        ↓                      ↓                     ↓               ↓
   Faker Data           Profile Configs        Time Series          Random Fraud      Partitioned
   Generation          (age/gender/geo)       Generation           Events (~1%)       Storage
```

### ML Pipeline Architecture  
```
Raw Data → Feature Engineering → Preprocessing → Scaling/PCA → Training → Model Evaluation
    ↓              ↓                   ↓              ↓            ↓            ↓
 Parquet      Time Features     String Indexing  StandardScaler  RandomForest  ROC/AUC
 Files        + Distance        + OneHotEncoder  + 18 PCA       CrossVal      Metrics
```

### Spark-Native Data Generation (`spark_fraud_detection/generation/`)
- **`datagen.py`**: Main Spark application with CLI interface and pipeline orchestration
- **`customer.py`**: Customer profile generation with demographics using Faker  
- **`transaction.py`**: Transaction generation with fraud pattern injection
- **`profile.py`**: Customer spending behavior profiles and fraud pattern definitions
- **`header.py`**: Data schema and field definitions

The Spark generator uses:
- Partitioned RDD processing for scalable customer generation
- DataFrame transformations for transaction pipeline
- Parquet output with partitioning by year/month/profile
- Configurable fraud rates and demographic targeting

### Fraud Detection ML Pipeline (`spark_fraud_detection/ml/`)
- **`fraud_detector.py`**: Complete ML pipeline with feature engineering, training, and evaluation
- **Feature Engineering**: Time-based features, customer demographics, distance calculations, log transformations
- **Model**: RandomForest classifier with hyperparameter tuning via CrossValidator
- **Evaluation**: AUC, Precision, Recall, F1-score with confusion matrix analysis
- **Output**: Model artifacts saved to specified path, predictions in CSV/Parquet formats

### Infrastructure Components
- **Spark Master**: Cluster coordinator (port 9090 WebUI, 7077 for workers)
- **Spark Workers**: Auto-scaling compute nodes for distributed processing
- **History Server**: Application monitoring and job history (port 18080)
- **MinIO**: S3-compatible object storage (port 9000 API, 9001 console)
- **Docker Volumes**: Persistent storage for data, logs, and applications

### Key Configuration Files
- `data/generation_configs/profiles/`: Customer demographic profiles and fraud patterns
  - `main_config.json`: Master configuration mapping demographic segments
  - Individual profile JSON files: Spending behavior patterns for each customer segment
  - Fraud profile variants: Fraudulent transaction patterns for each demographic
- `docker-compose.yml`: Multi-service cluster definition with MinIO
- `Dockerfile`: Spark + Python environment with uv dependency management  
- `conf/spark-defaults.conf`: Optimized Spark configuration for S3A and performance

## Data Schema and Output Structure

### Generated Transaction Schema
```
customer_id (int), trans_num (string), trans_date (string), trans_time (string), 
unix_time (long), category (string), amt (double), is_fraud (int), merchant (string),
merch_lat (double), merch_long (double), ssn (string), cc_num (string), 
first (string), last (string), gender (string), street (string), city (string), 
state (string), zip (string), lat (double), long (double), city_pop (int), 
job (string), dob (string), acct_num (string), profile (string)
```

### Output Directory Structure
```
./data/devset/
├── _SUCCESS
└── year=2023/
    ├── month=1/
    │   ├── profile=adults_2550_male_urban.json/
    │   │   └── part-*.snappy.parquet
    │   └── profile=young_adults_female_urban.json/
    │       └── part-*.snappy.parquet
    └── month=12/
        └── ...
```

### Customer Demographic Profiles
The system generates 12 distinct customer segments:
- **Age Groups**: Young adults (0-25), Adults (25-50), Seniors (50+)
- **Gender**: Male, Female  
- **Geography**: Urban (pop >2500), Rural (pop ≤2500)
- **Fraud Patterns**: Each segment has corresponding fraud behavior variants

## Performance & Benchmarks

### Spark-Native Performance (Observed)
Based on the existing `data/devset/` output structure:
- **Output Format**: Partitioned Parquet files by year/month/profile
- **Schema**: Full customer and transaction data with fraud indicators
- **Fraud Rate**: ~1% of transactions (configurable via profile configs)
- **Demographic Profiles**: 12 customer segments (age/gender/geography combinations)

### ML Pipeline Performance
- **Feature Count**: ~30+ features after engineering and encoding
- **PCA Components**: 18 (configurable, captures most variance)
- **Model Type**: RandomForest with hyperparameter tuning
- **Evaluation Metrics**: ROC-AUC, PR-AUC, Accuracy, F1, Precision, Recall
- **Output**: Predictions saved to `./output/` in both CSV and Parquet formats

### Optimization Notes
- **Spark Generator**: Optimized for cluster environments, better memory management, built-in partitioning
- **Parquet Output**: Compressed columnar format significantly reduces storage footprint
- **Partitioning Strategy**: Year/month/profile enables efficient analytical queries
- **MinIO Integration**: Local S3-compatible storage for development and testing
- **ML Pipeline**: Uses dataset balancing, cross-validation, and feature scaling for optimal performance

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.