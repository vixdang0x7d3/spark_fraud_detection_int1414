import sys
from pathlib import Path
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    PCA,
    StringIndexer,
    OneHotEncoder,
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.linalg import VectorUDT
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class SparkFraudDetector:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = self._setup_logging()
        self.model = None
        self.pipeline = None
        self.feature_cols = []
        self.scaler_model = None
        self.pca_model = None

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        return logger

    def load_data(self, data_path: str) -> DataFrame:
        self.logger.info(f"Loading data from: {data_path}")

        df = self.spark.read.parquet(data_path)

        total_records = df.count()
        fraud_records = df.filter(col("is_fraud") == 1).count()
        fraud_rate = fraud_records / total_records

        self.logger.info(f"Loaded {total_records:,} records")
        self.logger.info(f"Fraud records: {fraud_records:,} ({fraud_rate:.2f}%)")

        return df

    def feature_engineering(self, df: DataFrame) -> DataFrame:
        self.logger.info("Start feature engineering...")

        # parse datetime features
        df = df.withColumn(
            "trans_datetime",
            F.to_timestamp(
                F.concat(col("trans_date"), lit(" "), col("trans_time")),
            ),
        )

        # extract time components
        df = df.withColumn("hour", F.hour("trans_datetime"))
        df = df.withColumn("day_of_week", F.dayofweek("trans_datetime"))
        df = df.withColumn("month", F.month("trans_datetime"))
        df = df.withColumn("day_of_month", F.dayofmonth("trans_datetime"))

        # calculate age
        current_date = lit(datetime.now().strftime("%Y-%m-%d"))
        df = df.withColumn(
            "age",
            F.date_diff(current_date, F.to_date(col("dob"))) / 365.25,  # type: ignore
        )

        # distance between customer and merchnt
        df = df.withColumn(
            "distance",
            F.sqrt(
                F.pow(col("lat") - col("merch_lat"), 2)  # type: ignore
                + F.pow(col("long") - col("merch_long"), 2),  # type: ignore
            ),
        )

        # log-scaling amount
        df = df.withColumn("log_amt", F.log1p(col("amt")))

        # binning for converting to categorical
        df = df.withColumn(
            "amt_range",
            when(col("amt") < 50, "low")  # type: ignore
            .when(col("amt") < 200, "medium")  # type: ignore
            .when(col("amt") < 500, "high")  # type: ignore
            .otherwise("very_high"),
        )

        feature_cols = [
            "category",
            "amt",
            "log_amt",
            "zip",
            "lat",
            "long",
            "city_pop",
            "merch_lat",
            "merch_long",
            "age",
            "hour",
            "day_of_week",
            "month",
            "day_of_month",
            "distance",
            "amt_range",
            "gender",
            "is_fraud",
        ]

        df_features = df.select(*feature_cols)

        self.logger.info(
            f"Feature engineering completed. Selected {len(feature_cols) - 1} features"
        )

        return df_features

    def prepare_features(self, df: DataFrame) -> tuple[DataFrame, list[str]]:
        self.logger.info("Perfom feature transformation for ML pipeline...")

        categorical_cols = ["category", "amt_range", "gender"]

        df = df.withColumn("zip_numeric", col("zip").cast("double"))

        numerical_cols = [
            "amt",
            "log_amt",
            "zip_numeric",
            "lat",
            "long",
            "city_pop",
            "merch_lat",
            "merch_long",
            "age",
            "hour",
            "day_of_week",
            "month",
            "day_of_month",
            "distance",
        ]

        indexers = [
            StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
            for c in categorical_cols
        ]

        encoders = [
            OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_encoded")
            for c in categorical_cols
        ]

        encoded_categorical_cols = [f"{c}_encoded" for c in categorical_cols]
        all_feature_cols = numerical_cols + encoded_categorical_cols

        assembler = VectorAssembler(
            inputCols=all_feature_cols, outputCol="raw_features"
        )

        pipeline_stages = indexers + encoders + [assembler]
        preprocessing_pipeline = Pipeline(stages=pipeline_stages)

        # define transformation pipeline
        preprocessing_model = preprocessing_pipeline.fit(df)
        df_processed = preprocessing_model.transform(df)

        self.feature_cols = all_feature_cols
        self.logger.info(
            f"Feature transformation completed. Total features: {len(all_feature_cols)}"
        )

        return df_processed, all_feature_cols

    def apply_scaling_and_pca(self, df: DataFrame, n_components: int = 18) -> DataFrame:
        self.logger.info(f"Applying scaling and PCA with {n_components} components...")

        # feature scaling
        scaler = StandardScaler(inputCol="raw_features", outputCol="scaled_features")
        self.scaler_model = scaler.fit(df)
        df_scaled = self.scaler_model.transform(df)

        # dimension reduction
        pca = PCA(k=n_components, inputCol="scaled_features", outputCol="features")
        self.pca_model = pca.fit(df_scaled)
        df_pca = self.pca_model.transform(df_scaled)

        explained_variance = self.pca_model.explainedVariance.toArray()
        total_variance = np.sum(explained_variance)

        self.logger.info(
            f"PCA completed. Total explained variance: {total_variance:.4f}"
        )

        return df_pca

    def balance_dataset(self, df: DataFrame, target_col: str = "is_fraud") -> DataFrame:
        """Balance dataset using oversampling"""
        self.logger.info("Balancing dataset...")

        # Get class counts
        fraud_count = df.filter(col(target_col) == 1).count()
        non_fraud_count = df.filter(col(target_col) == 0).count()

        self.logger.info(
            f"Original distribution - Fraud: {fraud_count}, Non-fraud: {non_fraud_count}"
        )

        if fraud_count == 0:
            self.logger.warning("No fraud cases found in dataset!")
            return df

        # Calculate sampling ratios
        total_count = fraud_count + non_fraud_count
        fraud_ratio = fraud_count / total_count

        if fraud_ratio < 0.1:  # If fraud is less than 10%, oversample
            # Target 30% fraud rate
            target_fraud_ratio = 0.3
            oversample_ratio = (target_fraud_ratio * non_fraud_count) / (
                fraud_count * (1 - target_fraud_ratio)
            )

            # Oversample minority class
            fraud_df = df.filter(col(target_col) == 1)
            non_fraud_df = df.filter(col(target_col) == 0)

            # Create oversampled fraud data
            fraud_oversampled = fraud_df.sample(
                withReplacement=True, fraction=oversample_ratio, seed=42
            )

            # Combine datasets
            balanced_df = non_fraud_df.union(fraud_oversampled)

            new_fraud_count = balanced_df.filter(col(target_col) == 1).count()
            new_non_fraud_count = balanced_df.filter(col(target_col) == 0).count()

            self.logger.info(
                f"Balanced distribution - Fraud: {new_fraud_count}, Non-fraud: {new_non_fraud_count}"
            )

            return balanced_df
        else:
            self.logger.info("Dataset already reasonably balanced")
            return df

    def train_model(
        self, df: DataFrame, test_size: float = 0.2
    ) -> tuple[DataFrame, DataFrame]:
        self.logger.info("Traning model...")

        train_df, test_df = df.randomSplit([1 - test_size, test_size], seed=42)

        balanced_train_df = self.balance_dataset(train_df)

        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="is_fraud",
            numTrees=100,
            maxDepth=10,
            seed=42,
        )

        param_grid = (
            ParamGridBuilder()
            .addGrid(rf.numTrees, [50, 100])
            .addGrid(rf.maxDepth, [5, 10, 15])
            .build()
        )

        evaluator = BinaryClassificationEvaluator(
            labelCol="is_fraud", metricName="areaUnderROC"
        )

        cv = CrossValidator(
            estimator=rf,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            seed=42,
        )

        # fit model
        self.logger.info("Fitting model with cross-validation...")
        cv_model = cv.fit(balanced_train_df)
        self.model = cv_model.bestModel

        self.logger.info("Model training completed")

        return train_df, test_df

    def evaluate_model(self, test_df: DataFrame) -> dict[str, float]:
        if self.model is None:
            raise ValueError("Model not trained yet!")

        self.logger.info("Evaluating model...")

        predictions = self.model.transform(test_df)

        # binary classification metrics
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="is_fraud", metricName="areaUnderROC"
        )
        evaluator_pr = BinaryClassificationEvaluator(
            labelCol="is_fraud", metricName="areaUnderPR"
        )

        # multiclass metrics
        evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="is_fraud", metricName="accuracy"
        )
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="is_fraud", metricName="f1"
        )
        evaluator_precision = MulticlassClassificationEvaluator(
            labelCol="is_fraud", metricName="weightedPrecision"
        )
        evaluator_recall = MulticlassClassificationEvaluator(
            labelCol="is_fraud", metricName="weightedRecall"
        )

        # compute metrics
        auc = evaluator_auc.evaluate(predictions)
        pr_auc = evaluator_pr.evaluate(predictions)
        accuracy = evaluator_acc.evaluate(predictions)
        f1 = evaluator_f1.evaluate(predictions)
        precision = evaluator_precision.evaluate(predictions)
        recall = evaluator_recall.evaluate(predictions)

        confusion_matrix = (
            predictions.groupBy("is_fraud", "prediction").count().collect()
        )

        metrics = {
            "auc": auc,
            "pr_auc": pr_auc,
            "accuracy": accuracy,
            "f1": f1,
            "precision": precision,
            "recall": recall,
        }

        self.logger.info("=== MODEL EVALUATION RESULTS ===")
        for metric, value in metrics.items():
            self.logger.info(f"{metric.upper()}: {value:.4f}")

        self.logger.info("Confusion Matrix:")
        for row in confusion_matrix:
            self.logger.info(
                f"Actual: {row['is_fraud']}, Predicted: {row['prediction']}, Count: {row['count']}"
            )

        # show sample predictions
        self.logger.info("Sample predictions:")
        predictions.select("is_fraud", "prediction", "probability").show(10)

        # save predictions to specified output directory
        output_dir = getattr(self, "predictions_output_path", "./output")

        predictions_with_id = predictions.withColumn(
            "row_id", F.monotonically_increasing_id()
        )

        def extract_prob(v):
            return float(v[1]) if v is not None else 0.0

        extrac_prob_udf = udf(extract_prob, T.DoubleType())

        predictions_final = predictions_with_id.withColumn(
            "fraud_probability", extrac_prob_udf(col("probability"))
        )

        self.logger.info(f"Saving predictions to {output_dir}")

        # save as CSV (with extracted probability)
        predictions_final.select(
            "row_id", "is_fraud", "prediction", "fraud_probability"
        ).write.mode("overwrite").csv(
            f"{output_dir}/fraud_predictions_csv", header=True
        )

        # save as Parquet (with full probability vector)
        predictions_with_id.select(
            "row_id", "is_fraud", "prediction", "probability"
        ).write.mode("overwrite").parquet(f"{output_dir}/fraud_predictions_parquet")

        return metrics

    def save_model(self, model_path: str):
        if self.model is None:
            raise ValueError("No model to save!")

        self.logger.info(f"Saving model to: {model_path}")
        self.model.write().overwrite().save(model_path)

        # Save preprocessing models as well
        if self.scaler_model:
            self.scaler_model.write().overwrite().save(f"{model_path}/scaler")
        if self.pca_model:
            self.pca_model.write().overwrite().save(f"{model_path}/pca")

    def run_full_pipeline(
        self,
        data_path: str,
        model_output_path: str | None = None,
        predictions_output_path: str = "./output",
    ) -> dict[str, float]:
        self.logger.info("=== Starting Fraud Detection Pipeline ===")

        df = self.load_data(data_path)

        df_features = self.feature_engineering(df)

        df_processed, feature_cols = self.prepare_features(df_features)

        df_final = self.apply_scaling_and_pca(df_processed)

        train_df, test_df = self.train_model(df_final)

        self.predictions_output_path = predictions_output_path

        metrics = self.evaluate_model(test_df)

        if model_output_path:
            self.save_model(model_output_path)

        self.logger.info("=== Pipeline Completed Successfully ===")
        return metrics


def main():
    """Main function for running fraud detection"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Distributed Fraud Detection with Spark"
    )
    parser.add_argument(
        "--data-path", required=True, help="Path to fraud data (parquet files)"
    )
    parser.add_argument("--model-output", help="Path to save trained model")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    parser.add_argument(
        "--app-name", default="FraudDetection", help="Spark application name"
    )
    parser.add_argument(
        "--pca-components", type=int, default=18, help="Number of PCA components"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(args.app_name)
        .master(args.master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        detector = SparkFraudDetector(spark)

        metrics = detector.run_full_pipeline(
            data_path=args.data_path, model_output_path=args.model_output
        )

        print("\n=== FINAL RESULTS ===")
        for metric, value in metrics.items():
            print(f"{metric.upper()}: {value:.4f}")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
