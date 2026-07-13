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
raw_data = "/movies" # parquet directory

spark = (
    SparkSession.builder
    .appName("Transform Data-02")
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
    df = (
        spark.read
        .format("parquet")       
        .load(f"s3a://{bucket_name1}{raw_data}")
    )

    df = (
        df
        .withColumnRenamed("id", "movie_id")
        .withColumn(
            "movie_id",
            F.col("movie_id").cast("string")
        )
    )

    movies_df = df.select(
        F.col("movie_id"),
        F.col("title").cast("string").alias("title"),
        F.col("budget").cast("double").alias("budget"),
        F.col("homepage").cast("string").alias("homepage"),
        F.col("original_language").cast("string").alias("original_language"),
        F.col("original_title").cast("string").alias("original_title"),
        F.col("overview").cast("string").alias("overview"),
        F.col("popularity").cast("float").alias("popularity"),
        F.to_date(F.col("release_date")).alias("release_date"),
        F.col("revenue").cast("double").alias("revenue"),
        F.col("runtime").cast("integer").alias("runtime"),
        F.col("status").cast("string").alias("status"),
        F.col("tagline").cast("string").alias("tagline"),
        F.col("vote_average").cast("float").alias("vote_average"),
        F.col("vote_count").cast("integer").alias("vote_count")
    )

    id_name_schema = ArrayType(
        StructType([    
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
    )

    genres_df = (
        df
        .withColumn("genres_parsed", F.from_json(F.col("genres"), id_name_schema)
        )
        .select("movie_id", F.inline_outer(F.col("genres_parsed"))
        )
    )

    genres_df = genres_df.fillna({"id": -9999})

    kw_df = (
        df
        .withColumn("kw_parsed", F.from_json(F.col("keywords"), id_name_schema)
        )
        .select("movie_id", F.inline_outer(F.col("kw_parsed"))
        )
    )

    kw_df = kw_df.fillna({"id": -9999})

    comp_df = (
        df
        .withColumn("comp_parsed", F.from_json(F.col("production_companies"), id_name_schema)
        )
        .select("movie_id", F.inline_outer(F.col("comp_parsed"))
        )
    )

    comp_df = comp_df.fillna({"id": -9999})

    countries_schema = ArrayType(
        StructType([    
            StructField("iso_3166_1", StringType(), True),
            StructField("name", StringType(), True)
        ])
    )

    countries_df = (
        df
        .withColumn("countries_parsed", F.from_json(F.col("production_countries"), countries_schema)
        )
        .select("movie_id", F.inline_outer(F.col("countries_parsed"))
        )
    )

    countries_df = countries_df.fillna(
        {"iso_3166_1": "XX"}
    )

    lng_schema = ArrayType(
        StructType([    
            StructField("iso_639_1", StringType(), True),
            StructField("name", StringType(), True)
        ])
    )

    lng_df = (
        df
        .withColumn("lng_parsed", F.from_json(F.col("spoken_languages"), lng_schema)
        )
        .select("movie_id", F.inline_outer(F.col("lng_parsed"))
        )
    )

    lng_df = lng_df.fillna(
        {"iso_639_1": "XX"}
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
    tables = [
    ("movies", movies_df, "/movies"),
    ("genres", genres_df, "/genres"),
    ("keywords", kw_df, "/keywords"),
    ("production_companies", comp_df, "/production_companies"),
    ("production_countries", countries_df, "/production_countries"),
    ("spoken_languages", lng_df, "/spoken_languages")
    ]    

    for name, table, directory in tables:
        print(f"Starting: {name}")

        table.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"s3a://{bucket_name2}{directory}")

        print(f"Completed: {name}")

    print("========== Process Finished ==========")

finally:
    spark.stop()