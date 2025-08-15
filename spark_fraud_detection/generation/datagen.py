import random
import json
import argparse

from datetime import datetime, timedelta

from pathlib import Path


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F


from spark_fraud_detection.generation import (
    Profile,
    CustomerGenerator,
    make_cities,
    make_age_gender_dict,
    make_merchants,
    get_customer_data,
    create_complete_trans,
)


root_dir = Path(__file__).parent.parent.parent
config_dir = root_dir / "data" / "generation_configs"


def valid_date(s):
    formats = ["%m-%d-%Y", "%Y-%m-%d", "%d/%m/%Y"]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    msg = "not a valid date: {0!r}".format(s)
    raise argparse.ArgumentTypeError(msg)


class SparkFraudDataGenerator:
    def __init__(
        self,
        sc: SparkSession,
        config_dir: Path = config_dir,
    ):
        self.sc = sc
        self.main_config_path = config_dir / "profiles" / "main_config.json"
        with open(self.main_config_path, "r") as f:
            self.profile_configs = json.load(f)

        self.cities = make_cities(str(config_dir / "demographic_data"))
        self.age_gender = make_age_gender_dict(str(config_dir / "demographic_data"))
        self.merchants = make_merchants(str(config_dir / "demographic_data"))

    def generate_customers(self, num_customers: int, seed: int = 42) -> DataFrame:
        """Generate customers and save as a Spark DataFrame"""

        schema = T.StructType(
            [
                T.StructField("ssn", T.StringType(), True),
                T.StructField("cc_num", T.StringType(), True),
                T.StructField("first", T.StringType(), True),
                T.StructField("last", T.StringType(), True),
                T.StructField("gender", T.StringType(), True),
                T.StructField("street", T.StringType(), True),
                T.StructField("city", T.StringType(), True),
                T.StructField("state", T.StringType(), True),
                T.StructField("zip", T.StringType(), True),
                T.StructField("lat", T.DoubleType(), True),
                T.StructField("long", T.DoubleType(), True),
                T.StructField("city_pop", T.IntegerType(), True),
                T.StructField("job", T.StringType(), True),
                T.StructField("dob", T.StringType(), True),
                T.StructField("acct_num", T.StringType(), True),
                T.StructField("profile", T.StringType(), True),
                T.StructField("customer_id", T.IntegerType(), True),
            ]
        )

        # Extract values to avoid serializing self
        main_config_path = self.main_config_path
        age_gender = self.age_gender
        cities = self.cities

        def generate_customers_partition(partition_id, iterator):
            """Generate a batch of customers for a partition"""
            random.seed(seed + partition_id)

            customer_gen = CustomerGenerator(
                config_path=main_config_path, seed=seed + partition_id
            )

            customers = []
            for customer_id in iterator:
                customer_data = customer_gen.generate_customer(age_gender, cities)
                customer_data.append(customer_id)
                customers.append(customer_data)

            return iter(customers)

        # Parallelized generation with auto-partitioning
        customer_ids_rdd = self.sc.sparkContext.parallelize(range(num_customers))
        customers_rdd = customer_ids_rdd.mapPartitionsWithIndex(
            generate_customers_partition
        )

        customers_df = self.sc.createDataFrame(customers_rdd, schema)
        return customers_df

    def generate_transactions(
        self,
        customers_df: DataFrame,
        start_date: datetime,
        end_date: datetime,
    ) -> DataFrame:
        """Generate transactions for customers and save as Spark DataFrame"""

        schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("trans_num", T.StringType(), True),
                T.StructField("trans_date", T.StringType(), True),
                T.StructField("trans_time", T.StringType(), True),
                T.StructField("unix_time", T.LongType(), True),
                T.StructField("category", T.StringType(), True),
                T.StructField("amt", T.DoubleType(), True),
                T.StructField("is_fraud", T.IntegerType(), True),
                T.StructField("merchant", T.StringType(), True),
                T.StructField("merch_lat", T.DoubleType(), True),
                T.StructField("merch_long", T.DoubleType(), True),
                # Include customer data for final output
                T.StructField("ssn", T.StringType(), True),
                T.StructField("cc_num", T.StringType(), True),
                T.StructField("first", T.StringType(), True),
                T.StructField("last", T.StringType(), True),
                T.StructField("gender", T.StringType(), True),
                T.StructField("street", T.StringType(), True),
                T.StructField("city", T.StringType(), True),
                T.StructField("state", T.StringType(), True),
                T.StructField("zip", T.StringType(), True),
                T.StructField("lat", T.DoubleType(), True),
                T.StructField("long", T.DoubleType(), True),
                T.StructField("city_pop", T.IntegerType(), True),
                T.StructField("job", T.StringType(), True),
                T.StructField("dob", T.StringType(), True),
                T.StructField("acct_num", T.StringType(), True),
                T.StructField("profile", T.StringType(), True),
            ]
        )

        # Extract values to avoid serializing self
        merchants = self.merchants

        def generate_transactions_partition(iterable):
            """Generate transactions for a partition of customer"""
            transactions = []
            for row in iterable:
                customer_data = get_customer_data(row)
                profile_name = customer_data["profile"]
                profile_path = config_dir / "profiles" / profile_name
                fraud_profile_path = config_dir / "profiles" / f"fraud_{profile_name}"

                try:
                    with open(profile_path, "r") as f:
                        profile_obj = json.load(f)
                    with open(fraud_profile_path, "r") as f:
                        fraud_profile_obj = json.load(f)

                    profile = Profile(profile_obj)
                    profile.set_date_range(start_date, end_date)
                    fraud_profile = Profile(fraud_profile_obj)

                    is_fraud = 0
                    fraud_flag = random.randint(0, 100)
                    if fraud_flag < 99:
                        print("FRAUD!!!")
                        fraud_interval = random.randint(1, 1)
                        inter_val = (end_date - start_date).days - 7
                        rand_interval = random.randint(1, max(inter_val, 1))

                        newstart = start_date + timedelta(days=rand_interval)
                        newend = newstart + timedelta(days=fraud_interval)

                        fraud_profile.set_date_range(newstart, newend)
                        is_fraud = 1

                        fraud_trans = fraud_profile.sample_from(is_fraud)
                        complete_records = create_complete_trans(
                            customer_data, fraud_trans, merchants, is_fraud
                        )

                        # fix data types and add customer_id for each transaction
                        for record in complete_records:
                            customer_id = customer_data["customer_id"]
                            # skip customer_id from customer data since we're adding it manually
                            customer_fields = [
                                v
                                for k, v in customer_data.items()
                                if k != "customer_id"
                            ]
                            fixed_record = (
                                [
                                    customer_id,  # customer_id (int)
                                    record[0],  # trans_num (string)
                                    record[1],  # trans_date (string)
                                    record[2],  # trans_time (string)
                                    int(record[3]),  # unix_time (long)
                                    record[4],  # category (string)
                                    float(record[5]),  # amt (double)
                                    int(record[6]),  # is_fraud (int)
                                    record[7],  # merchant (string)
                                    float(record[8]),  # merch_lat (double)
                                    float(record[9]),  # merch_long (double)
                                ]
                                + customer_fields
                            )  # customer fields (excluding customer_id)
                            transactions.append(fixed_record)

                    clean_trans = profile.sample_from(0)
                    complete_records = create_complete_trans(
                        customer_data, clean_trans, merchants, 0
                    )

                    # fix data types and add customer_id for each transaction
                    print("NO FRAUD")
                    for record in complete_records:
                        customer_id = customer_data["customer_id"]
                        # skip customer_id from customer data since we're adding it manually
                        customer_fields = [
                            v for k, v in customer_data.items() if k != "customer_id"
                        ]
                        fixed_record = [
                            customer_id,  # customer_id (int)
                            record[0],  # trans_num (string)
                            record[1],  # trans_date (string)
                            record[2],  # trans_time (string)
                            int(record[3]),  # unix_time (long)
                            record[4],  # category (string)
                            float(record[5]),  # amt (double)
                            int(record[6]),  # is_fraud (int)
                            record[7],  # merchant (string)
                            float(record[8]),  # merch_lat (double)
                            float(record[9]),  # merch_long (double)
                        ] + customer_fields  # customer fields (excluding customer_id)
                        transactions.append(fixed_record)

                except Exception:
                    continue
            return iter(transactions)

        # Parallelized generation with auto-partitioning
        transactions_rdd = customers_df.rdd.mapPartitions(
            generate_transactions_partition
        )
        transactions_df = self.sc.createDataFrame(transactions_rdd, schema)

        return transactions_df

    def run_pipeline(
        self,
        num_customers: int,
        start_date: datetime,
        end_date: datetime,
        output_path: str,
        seed: int = 42,
    ):
        """Run the complete synthetic data generation pipeline"""

        print(
            f"Generating {num_customers} customers with transactions from {start_date} to {end_date}"
        )

        # Stage 1: Generating customers
        print("Generating customers...")
        customers_df = self.generate_customers(num_customers, seed)
        customers_df.cache()

        print(f"Generated {customers_df.count()} customers")

        # Stage 2: Generate transactions
        print("Generating transactions...")
        transactions_df = self.generate_transactions(customers_df, start_date, end_date)

        print(f"Generated {transactions_df.count()} transactions")

        transactions_final = transactions_df.withColumn(
            "year", F.year(F.to_date(F.col("trans_date"), "yyyy-MM-dd"))
        ).withColumn("month", F.month(F.to_date(F.col("trans_date"), "yyyy-MM-dd")))

        print(f"Writing to Parquet format at {output_path}")

        transactions_final.write.mode("overwrite").partitionBy(
            "year", "month", "profile"
        ).option("maxRecordsPerFile", 50000).parquet(output_path)

        # Generate summary statistics
        print("\n=== Generation Summary ===")
        print(f"Total customers: {customers_df.count()}")
        print(f"Total transactions: {transactions_df.count()}")

        fraud_stats = transactions_df.groupBy("is_fraud").count().collect()
        for row in fraud_stats:
            print(row)

        profile_stats = (
            transactions_df.groupby("profile")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )

        print("\nTransactions by profile")
        for row in profile_stats[:10]:
            print(f" {row['profile']}: {row['count']}")

        customers_df.unpersist()  # Release cache

        print(f"\nData generation complete! Output written to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Spark Synthetic Fraud Data Generator")
    parser.add_argument(
        "-n",
        "--num_customers",
        type=int,
        default=10000,
        help="Number of customers to generate",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="./data/devset",
        help="Output path for Parquet files",
    )
    parser.add_argument(
        "--start_date",
        type=valid_date,
        default="2023-01-01",
        help="Start date for transactions (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end_date",
        type=valid_date,
        default="2023-12-31",
        help="End date for transactions (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility"
    )
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    parser.add_argument(
        "--app_name", default="FraudDataGenerator", help="Spark application name"
    )

    args = parser.parse_args()

    # Create Spark session
    spark = (
        SparkSession.builder.appName(args.app_name)
        .master(args.master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        # Initialize generator
        generator = SparkFraudDataGenerator(spark)

        # Run pipeline
        generator.run_pipeline(
            num_customers=args.num_customers,
            start_date=args.start_date,
            end_date=args.end_date,
            output_path=args.output,
            seed=args.seed,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
