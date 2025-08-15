#!/usr/bin/env python3
"""
S3 Data Generation Script for Fraud Detection System

Generates synthetic fraud transaction data and writes directly to S3-compatible storage.
Supports both AWS S3 and MinIO endpoints with configurable parameters.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pyspark.sql import SparkSession

# Add parent directory to Python path to find spark_fraud_detection module
script_dir = Path(__file__).parent
root_dir = script_dir.parent
sys.path.insert(0, str(root_dir))

from spark_fraud_detection.generation.datagen import SparkFraudDataGenerator  # ty: ignore


def valid_date(s: str) -> datetime:
    """Parse date string in multiple formats"""
    formats = ["%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    raise argparse.ArgumentTypeError(f"Not a valid date: {s!r}")


def verify_s3_access(
    endpoint_url: Optional[str], bucket: str, access_key: Optional[str] = None, secret_key: Optional[str] = None
) -> bool:
    """Verify S3 bucket access and create bucket if it doesn't exist"""
    try:
        # Build client kwargs, only include credentials if provided
        client_kwargs = {}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        if access_key and secret_key:
            client_kwargs["aws_access_key_id"] = access_key
            client_kwargs["aws_secret_access_key"] = secret_key
        
        # Let boto3 auto-discover credentials if none provided
        s3_client = boto3.client("s3", **client_kwargs)

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket)
            print(f"✓ Bucket '{bucket}' exists and is accessible")
            return True
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                # Bucket doesn't exist, try to create it
                try:
                    s3_client.create_bucket(Bucket=bucket)
                    print(f"✓ Created bucket '{bucket}'")
                    return True
                except ClientError as create_error:
                    print(f"✗ Failed to create bucket '{bucket}': {create_error}")
                    return False
            else:
                print(f"✗ Cannot access bucket '{bucket}': {e}")
                return False

    except NoCredentialsError:
        print("✗ S3 credentials not found")
        return False
    except Exception as e:
        print(f"✗ S3 connection failed: {e}")
        return False


def create_spark_session(
    app_name: str,
    master: str,
    s3_endpoint: Optional[str],
    s3_access_key: Optional[str] = None,
    s3_secret_key: Optional[str] = None,
    use_ssl: bool = True,
    executor_memory: Optional[str] = None,
    executor_cores: Optional[int] = None,
    max_executors: Optional[int] = None,
) -> SparkSession:
    """Create Spark session with S3 configuration"""

    builder = SparkSession.builder.appName(app_name).master(master)

    # S3A Configuration
    builder = builder.config(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    
    # Only set credentials if provided, otherwise let AWS SDK handle credential discovery
    if s3_access_key and s3_secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    else:
        # Use AWS credential provider chain for automatic credential discovery
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
    
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled", str(use_ssl).lower()
    )

    if s3_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)

    # Required JARs for S3 support
    builder = builder.config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )

    # S3A optimizations (using compatible settings)
    builder = builder.config("spark.hadoop.fs.s3a.fast.upload", "true")
    builder = builder.config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
    builder = builder.config("spark.hadoop.fs.s3a.multipart.threshold", "104857600")  # 100MB
    
    # Connection and timeout settings
    builder = builder.config("spark.hadoop.fs.s3a.connection.maximum", "15")
    builder = builder.config("spark.hadoop.fs.s3a.connection.timeout", "200000")
    builder = builder.config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    builder = builder.config("spark.hadoop.fs.s3a.retry.limit", "3")
    builder = builder.config("spark.hadoop.fs.s3a.retry.interval", "2s")
    
    # Block size settings
    builder = builder.config("spark.hadoop.fs.s3a.block.size", "134217728")  # 128MB
    
    # Memory and performance optimizations
    builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.maxPartitions", "100")
    builder = builder.config("spark.sql.parquet.compression.codec", "snappy")
    builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
    builder = builder.config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
    builder = builder.config("spark.sql.files.maxRecordsPerFile", "100000")
    
    # Dynamic executor settings based on master type and overrides
    if "local" in master.lower():
        # Local development settings (conservative)
        exec_memory = executor_memory or "1g"
        exec_cores = executor_cores or 1
        builder = builder.config("spark.executor.memory", exec_memory)
        builder = builder.config("spark.executor.cores", str(exec_cores))
        builder = builder.config("spark.driver.memory", "512m")
        builder = builder.config("spark.driver.maxResultSize", "256m")
        builder = builder.config("spark.sql.shuffle.partitions", "8")
    else:
        # Cluster/EMR settings (performance optimized)
        exec_memory = executor_memory or "4g"
        exec_cores = executor_cores or 4
        max_exec = max_executors or 20
        
        builder = builder.config("spark.executor.memory", exec_memory)
        builder = builder.config("spark.executor.cores", str(exec_cores))
        builder = builder.config("spark.executor.memoryFraction", "0.8")
        builder = builder.config("spark.driver.memory", "2g")
        builder = builder.config("spark.driver.maxResultSize", "1g")
        builder = builder.config("spark.sql.shuffle.partitions", "200")
        
        # EMR-specific optimizations
        builder = builder.config("spark.dynamicAllocation.enabled", "true")
        builder = builder.config("spark.dynamicAllocation.minExecutors", "2")
        builder = builder.config("spark.dynamicAllocation.maxExecutors", str(max_exec))
        builder = builder.config("spark.dynamicAllocation.initialExecutors", "4")
        
        # S3A performance for cluster
        builder = builder.config("spark.hadoop.fs.s3a.connection.maximum", "50")
        builder = builder.config("spark.hadoop.fs.s3a.multipart.size", "67108864")  # 64MB
        builder = builder.config("spark.hadoop.fs.s3a.multipart.threshold", "67108864")
        builder = builder.config("spark.hadoop.fs.s3a.threads.max", "20")
        
        # Enable EMRFS optimizations if available
        builder = builder.config("spark.hadoop.fs.s3.impl", "com.amazon.ws.emr.hadoop.fs.EmrFileSystem")
        builder = builder.config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")

    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic fraud data directly to S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate to AWS S3 (uses AWS CLI credentials automatically)
  python scripts/generate_to_s3.py \\
    --bucket my-fraud-data \\
    --s3-prefix fraud-dataset-2023 \\
    -n 100000 \\
    --start-date 2023-01-01 \\
    --end-date 2023-12-31

  # Generate to MinIO (local development)
  python scripts/generate_to_s3.py \\
    --bucket fraud-data \\
    --s3-endpoint http://localhost:9000 \\
    --s3-access-key minioadmin \\
    --s3-secret-key minioadmin123 \\
    --no-ssl \\
    -n 10000

  # Generate with explicit environment variables
  export AWS_ACCESS_KEY_ID=your-key
  export AWS_SECRET_ACCESS_KEY=your-secret
  python scripts/generate_to_s3.py --bucket my-bucket -n 50000

  # Generate using AWS CLI configured credentials (no credentials needed)
  aws configure  # run this once to set up credentials
  python scripts/generate_to_s3.py --bucket my-bucket -n 50000

  # EMR cluster usage (high performance)
  python scripts/generate_to_s3.py \\
    --bucket my-production-bucket \\
    --master yarn \\
    --executor-memory 8g \\
    --executor-cores 4 \\
    --max-executors 50 \\
    -n 1000000
        """,
    )

    # Data generation parameters
    parser.add_argument(
        "-n",
        "--num-customers",
        type=int,
        default=10000,
        help="Number of customers to generate (default: 10000)",
    )
    parser.add_argument(
        "--start-date",
        type=valid_date,
        default="2023-01-01",
        help="Start date for transactions (YYYY-MM-DD, default: 2023-01-01)",
    )
    parser.add_argument(
        "--end-date",
        type=valid_date,
        default="2023-12-31",
        help="End date for transactions (YYYY-MM-DD, default: 2023-12-31)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )

    # S3 configuration
    parser.add_argument(
        "--bucket", required=True, help="S3 bucket name for output data"
    )
    parser.add_argument(
        "--s3-prefix",
        default="fraud-data",
        help="S3 prefix/folder for the dataset (default: fraud-data)",
    )
    parser.add_argument(
        "--s3-endpoint",
        help="S3 endpoint URL (for MinIO or other S3-compatible storage)",
    )
    parser.add_argument(
        "--s3-access-key",
        default=os.environ.get("AWS_ACCESS_KEY_ID"),
        help="S3 access key (default: AWS_ACCESS_KEY_ID env var, or AWS CLI config)",
    )
    parser.add_argument(
        "--s3-secret-key",
        default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        help="S3 secret key (default: AWS_SECRET_ACCESS_KEY env var, or AWS CLI config)",
    )
    parser.add_argument(
        "--no-ssl",
        action="store_true",
        help="Disable SSL for S3 connections (for local MinIO)",
    )

    # Spark configuration
    parser.add_argument(
        "--master", default="local[*]", help="Spark master URL (default: local[*])"
    )
    parser.add_argument(
        "--app-name", default="S3FraudDataGeneration", help="Spark application name"
    )
    parser.add_argument(
        "--executor-memory", help="Executor memory (e.g., 4g). Auto-detected based on master if not specified."
    )
    parser.add_argument(
        "--executor-cores", type=int, help="Executor cores. Auto-detected based on master if not specified."
    )
    parser.add_argument(
        "--max-executors", type=int, help="Max executors for dynamic allocation (EMR only, default: 20)"
    )

    # Output options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Verify configuration without generating data",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Validate credentials (only required for custom endpoints like MinIO)
    if args.s3_endpoint and (not args.s3_access_key or not args.s3_secret_key):
        print(
            "✗ S3 credentials required for custom endpoints. Set --s3-access-key and --s3-secret-key or use environment variables."
        )
        sys.exit(1)

    # Build S3 output path
    s3_output_path = f"s3a://{args.bucket}/{args.s3_prefix}"

    print("=== S3 Fraud Data Generation ===")
    print(f"Customers: {args.num_customers:,}")
    print(
        f"Date range: {args.start_date.strftime('%Y-%m-%d')} to {args.end_date.strftime('%Y-%m-%d')}"
    )
    print(f"S3 Output: {s3_output_path}")
    print(f"S3 Endpoint: {args.s3_endpoint or 'AWS S3'}")
    print(f"SSL: {'Disabled' if args.no_ssl else 'Enabled'}")
    
    # Show credential source
    if args.s3_access_key and args.s3_secret_key:
        print(f"Credentials: Explicit (provided via args/env vars)")
    else:
        print(f"Credentials: Auto-discovery (AWS CLI config, IAM roles, etc.)")

    # Verify S3 access
    print("\n=== Verifying S3 Access ===")
    if not verify_s3_access(
        args.s3_endpoint, args.bucket, args.s3_access_key, args.s3_secret_key
    ):
        print(
            "✗ S3 verification failed. Please check your credentials and bucket configuration."
        )
        sys.exit(1)

    if args.dry_run:
        print("✓ Dry run completed successfully. Configuration is valid.")
        return

    # Create Spark session
    print("\n=== Initializing Spark ===")
    spark = create_spark_session(
        app_name=args.app_name,
        master=args.master,
        s3_endpoint=args.s3_endpoint,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        use_ssl=not args.no_ssl,
        executor_memory=args.executor_memory,
        executor_cores=args.executor_cores,
        max_executors=args.max_executors,
    )

    try:
        print(f"✓ Spark session created: {spark.sparkContext.master}")
        print(f"✓ Spark version: {spark.version}")

        # Initialize data generator
        print("\n=== Starting Data Generation ===")
        generator = SparkFraudDataGenerator(spark)

        # Run the pipeline with progress monitoring
        print("Starting data generation pipeline...")
        print("Note: The final write phase may take several minutes for large datasets")
        print("If it appears to hang, it's likely during the S3 commit phase - please wait")
        
        generator.run_pipeline(
            num_customers=args.num_customers,
            start_date=args.start_date,
            end_date=args.end_date,
            output_path=s3_output_path,
            seed=args.seed,
        )

        print("\n✓ Data generation completed successfully!")
        print(f"✓ Output location: {s3_output_path}")

        # Show final statistics
        if args.verbose:
            print("\n=== Final Verification ===")
            try:
                df = spark.read.parquet(s3_output_path)
                total_records = df.count()
                fraud_count = df.filter(df.is_fraud == 1).count()
                print(f"Total records: {total_records:,}")
                print(
                    f"Fraud records: {fraud_count:,} ({fraud_count / total_records * 100:.1f}%)"
                )
            except Exception as e:
                print(f"Could not verify output: {e}")

    except KeyboardInterrupt:
        print(f"\n⚠️  Data generation interrupted by user")
        print("Cleaning up Spark session...")
        sys.exit(130)
    except Exception as e:
        print(f"\n✗ Data generation failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check S3 credentials and bucket permissions")
        print("2. Verify network connectivity to S3")
        print("3. Try reducing dataset size (-n parameter)")
        print("4. Check Spark logs for detailed error messages")
        sys.exit(1)

    finally:
        try:
            spark.stop()
        except:
            pass


if __name__ == "__main__":
    main()
