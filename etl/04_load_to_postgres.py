import configparser
from pathlib import Path
from pyspark.sql import SparkSession

config = configparser.RawConfigParser()
config_path = Path(__file__).parent / "all_conn"

if not config.read(config_path):
    raise FileNotFoundError(f"Configuration file not found: {config_path}")

endpoint_url = config.get("conn", "endpoint_url")
aws_access_key_id = config.get("conn", "aws_access_key_id")
aws_secret_access_key = config.get("conn", "aws_secret_access_key")
bucket_name2 = config.get("conn", "bucket_name2") # silver

pg_host = config.get("conn", "pg_host")
pg_port = config.get("conn", "pg_port")
pg_database = config.get("conn", "pg_database")
pg_user = config.get("conn", "pg_user")
pg_password = config.get("conn", "pg_password")
jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

spark = (
    SparkSession.builder
    .appName("Load to PostgreSQL")
    .master("local[4]")
    .config("spark.driver.memory", "1024m")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "io.delta:delta-spark_2.12:3.3.2,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.7.11"
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

spark.sparkContext.setLogLevel('ERROR')

try:

    table_paths = [
    ("cast_table", "cast"),
    ("crew", "crew"),
    ("genres", "genres"),
    ("keywords", "keywords"),
    ("movies", "movies"),
    ("production_companies", "production_companies"),
    ("production_countries", "production_countries"),
    ("spoken_languages", "spoken_languages")
    ]
    # =========== Writing to PostgreSQL ============
    for table_name, delta_path in table_paths:
        print(f"Starting: {table_name}")

        df = (
            spark.read
            .format("delta")
            .load(f"s3a://{bucket_name2}/{delta_path}")
        )

        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", pg_user)
            .option("password", pg_password)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        print(f"Completed: {table_name}")

    print("========== Process Finished ==========")

finally:    
    spark.stop()