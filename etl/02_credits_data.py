import boto3
from botocore.exceptions import ClientError
import configparser
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    IntegerType,
    StringType
)

config = configparser.RawConfigParser()
config_path = Path(__file__).parent / "all_conn"

if not config.read(config_path):
    raise FileNotFoundError(f"Configuration file not found: {config_path}")

endpoint_url = config.get("conn", "endpoint_url")
aws_access_key_id = config.get("conn", "aws_access_key_id")
aws_secret_access_key = config.get("conn", "aws_secret_access_key")

bucket_name1 = config.get("conn", "bucket_name1") # bronze
bucket_name2 = config.get("conn", "bucket_name2") # silver
raw_data = "/credits" # parquet directory

spark = (
    SparkSession.builder
    .appName("Transform Data-01")
    .master("local[4]")
    .config("spark.driver.memory", "1024m")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "io.delta:delta-spark_2.12:3.3.2"
    )
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

try:
    credits_df = (
    spark.read
    .format("parquet")
    .load(f"s3a://{bucket_name1}{raw_data}")
    )

    credits_df = credits_df.withColumn(
        "movie_id",
        F.col("movie_id").cast("string")
    )

    cast_schema = ArrayType(
        StructType([
            StructField("cast_id", IntegerType(), True),
            StructField("character", StringType(), True),
            StructField("credit_id", StringType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("order", IntegerType(), True)
        ])
    )

    cast_df = (
        credits_df
        .withColumn("cast_parsed", F.from_json(F.col("cast"), cast_schema)
        )
        .select("movie_id", "title", F.inline_outer(F.col("cast_parsed"))
        )
        .drop("order")
    )

    cast_df = cast_df.fillna(
        {"credit_id": "0000000000"}
    )

    crew_schema = ArrayType(
        StructType([
            StructField("credit_id", StringType(), True),
            StructField("department", StringType(), True),
            StructField("gender", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("job", StringType(), True),
            StructField("name", StringType(), True)
        ])
    )

    crew_df = (
        credits_df
        .withColumn("crew_parsed", F.from_json(F.col("crew"), crew_schema)
        )
        .select("movie_id","title", F.inline_outer("crew_parsed")
        )
    )

    crew_df = crew_df.fillna(
        {"credit_id" : "0000000000"}
    )      

    # =========== RustFS ============
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name2)
        print(f"{bucket_name2} bucket exists.")

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        if error_code in ("404", "NoSuchBucket", "NotFound"):
            s3_client.create_bucket(Bucket=bucket_name2)
            print(f"{bucket_name2} bucket created.")
        else:
            raise
 # =========== Writing Process ============
    cast_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"s3a://{bucket_name2}/cast")

    crew_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"s3a://{bucket_name2}/crew")

    print("========== Process Finished ==========")

finally:    
    spark.stop()