#!/usr/bin/env python3
"""
Run fraud detection on the sample dataset
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession


# Add parent directory to Python path
script_dir = Path(__file__).parent
root_dir = script_dir.parent
sys.path.insert(0, str(root_dir))

from spark_fraud_detection.ml.fraud_detector import SparkFraudDetector


def main():
    """Run fraud detection on sample data"""

    # Create Spark session with optimized settings
    spark = (
        SparkSession.builder.appName("FraudDetection-DevSet")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=== Spark Fraud Detection System ===")
        print(f"Spark version: {spark.version}")
        print(f"Running on: {spark.sparkContext.master}")

        # Initialize fraud detector
        detector = SparkFraudDetector(spark)

        # Data path - can be local or S3
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--data-path",
            default="data/devset_tmp_3",
            help="Data path (local or s3://)",
        )
        parser.add_argument(
            "--output-path",
            default="output/fraud_detection_model",
            help="Output path (local or s3://)",
        )
        parser.add_argument(
            "--predictions-path", default="output", help="Predictions output path"
        )
        args, _ = parser.parse_known_args()

        data_path = args.data_path
        model_output_path = args.output_path

        print(f"Loading data from: {data_path}")

        # Run the complete pipeline
        metrics = detector.run_full_pipeline(
            data_path=data_path,
            model_output_path=model_output_path,
            predictions_output_path=args.predictions_path,
        )

        print("\n" + "=" * 50)
        print("FINAL PERFORMANCE METRICS")
        print("=" * 50)

        for metric, value in metrics.items():
            print(f"{metric.upper():15}: {value:.4f}")

        print(f"\nModel saved to: {model_output_path}")
        print("Pipeline completed successfully!")

    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    exit(main())

