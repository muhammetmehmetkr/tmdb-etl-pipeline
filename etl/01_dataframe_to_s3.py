import argparse
import io
import logging
import time
from datetime import datetime
import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError
import configparser
from pathlib import Path

config = configparser.RawConfigParser()
config_path = Path(__file__).parent / "all_conn"

if not config.read(config_path):
    raise FileNotFoundError(f"Configuration file not found: {config_path}")

endpoint_url = config.get("conn", "endpoint_url")
aws_access_key_id = config.get("conn", "aws_access_key_id")
aws_secret_access_key = config.get("conn", "aws_secret_access_key")
bucket_name = config.get("conn", "bucket_name1")

class DataFrameDataGenerator:
    def __init__(
        self, input, bucket, key, access_key_id, secret_access_key,
        endpoint_url, batch_interval, repeat, shuffle, batch_size,
        sep, log_sep, source_file_extension, output_header,
        is_output_format_parquet, output_index, excluded_cols,
        order_by=None, ascending=True
    ):
        self.sep = sep
        print("self.sep:", self.sep)

        self.log_sep = log_sep
        print("self.log_sep:", self.log_sep)

        self.input = input
        print("self.input:", self.input)

        self.excluded_cols = excluded_cols
        print("self.excluded_cols:", self.excluded_cols)

        self.shuffle = shuffle
        print("self.shuffle:", self.shuffle)

        self.order_by = order_by
        print("self.order_by:", self.order_by)

        self.ascending = ascending
        print("self.ascending:", self.ascending)

        self.bucket = bucket
        print("self.bucket:", self.bucket)

        self.key = key
        print("self.key:", self.key)

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

        self.endpoint_url = endpoint_url
        print("self.endpoint_url:", self.endpoint_url)

        self.batch_size = batch_size
        print("self.batch_size:", self.batch_size)

        self.batch_interval = batch_interval
        print("self.batch_interval:", self.batch_interval)

        self.repeat = repeat
        print("self.repeat:", self.repeat)

        self.output_header = output_header
        print("self.output_header:", self.output_header)

        self.is_output_format_parquet = is_output_format_parquet
        print("self.is_output_format_parquet:", self.is_output_format_parquet)

        self.output_index = output_index
        print("self.output_index:", self.output_index)

        # Source dosyayı oku
        self.df = self.read_source_file(source_file_extension)
        print("DataFrame row count:", len(self.df))

        # S3 client sadece bir kez oluşturulur
        self.s3_client = self.get_s3_client()

        print(
            "First batch will be written in approximately "
            f"{self.batch_interval * self.batch_size:.2f} seconds."
        )

    def read_source_file(self, extension="csv"):
        if extension.lower() == "csv":
            df = pd.read_csv(self.input, sep=self.sep)
        else:
            df = pd.read_parquet(self.input, engine="auto")

        # order_by, shuffle işleminden önceliklidir
        if self.order_by:
            print(
                "Ordering by:",
                self.order_by,
                "ascending:",
                self.ascending
            )

            df = df.sort_values(
                by=self.order_by,
                ascending=self.ascending
            )

        elif self.shuffle:
            print("Shuffling DataFrame")
            df = df.sample(frac=1).reset_index(drop=True)

        columns_to_write = [
            column
            for column in df.columns
            if column not in self.excluded_cols
        ]

        print("columns_to_write:", columns_to_write)

        return df[columns_to_write]

    def get_s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=Config(
                signature_version="s3v4",
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required"
            )
        )

    def ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            print(f"Bucket already exists: {self.bucket}")

        except ClientError as error:
            error_code = str(
                error.response.get("Error", {}).get("Code", "")
            )

            if error_code in ("404", "NoSuchBucket", "NotFound"):
                print(
                    f"Bucket does not exist. "
                    f"Creating: {self.bucket}"
                )

                self.s3_client.create_bucket(Bucket=self.bucket)
                print(f"Bucket created: {self.bucket}")

            else:
                print(f"Bucket check failed: {error_code}")
                raise

    def save_df_to_s3(self, df, key):
        # Object key "/" ile başlamamalı
        key = key.lstrip("/")

        try:
            if self.is_output_format_parquet:
                buffer = io.BytesIO()

                df.to_parquet(
                    buffer,
                    engine="pyarrow",
                    index=self.output_index,
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True
                )

                self.s3_client.put_object(
                    Body=buffer.getvalue(),
                    Bucket=self.bucket,
                    Key=key
                )

            else:
                buffer = io.StringIO()

                df.to_csv(
                    buffer,
                    index=self.output_index,
                    header=self.output_header,
                    sep=self.log_sep
                )

                self.s3_client.put_object(
                    Body=buffer.getvalue(),
                    Bucket=self.bucket,
                    Key=key
                )

            print(f"Saved: s3://{self.bucket}/{key}")

            logging.info(
                "%s saved to bucket %s",
                key,
                self.bucket
            )

        except Exception as error:
            logging.exception(error)
            raise

    def df_to_s3_as_log(self):
        # Bucket yalnızca bir kez kontrol edilir
        self.ensure_bucket_exists()

        df_size = len(self.df)
        total_rows = df_size * self.repeat
        total_estimated_time = self.batch_interval * total_rows

        print()
        print("Generation information")
        print("----------------------")
        print("Rows in dataset:", df_size)
        print("Repeat:", self.repeat)
        print("Total rows:", total_rows)
        print("Batch size:", self.batch_size)
        print(
            "Estimated minimum duration:",
            f"{total_estimated_time / 60:.2f} minutes"
        )
        print()

        time_list_for_each_batch = []
        total_counter = 0

        for repeat_index in range(self.repeat):
            print(
                f"Starting repeat "
                f"{repeat_index + 1}/{self.repeat}"
            )

            batch_start_index = 0

            for row_index in range(1, df_size + 1):
                time.sleep(self.batch_interval)

                event_time = datetime.now()
                time_list_for_each_batch.append(event_time)

                total_counter += 1

                batch_is_full = (
                    row_index % self.batch_size == 0
                )

                dataset_is_finished = (
                    row_index == df_size
                )

                if batch_is_full or dataset_is_finished:
                    df_batch = self.df.iloc[
                        batch_start_index:row_index, :
                    ].copy()

                    df_batch["event_time"] = time_list_for_each_batch
                    time_list_for_each_batch = []

                    # Mikro saniye de eklenerek
                    # object key çakışması engellenir
                    timestamp = datetime.now().strftime(
                        "%Y%m%d-%H%M%S-%f"
                    )

                    if self.is_output_format_parquet:
                        object_key = (
                            f"{self.key}_"
                            f"{timestamp}.parquet"
                        )
                    else:
                        object_key = (
                            f"{self.key}_"
                            f"{timestamp}.csv"
                        )

                    self.save_df_to_s3(
                        df=df_batch,
                        key=object_key
                    )

                    batch_start_index = row_index

                    completed_percentage = (
                        total_counter / total_rows
                    ) * 100

                    remaining_rows = total_rows - total_counter

                    remaining_time_seconds = (
                        remaining_rows * self.batch_interval
                    )

                    remaining_time_minutes = (
                        remaining_time_seconds / 60
                    )

                    print(
                        f"{total_counter}/{total_rows} processed, "
                        f"{completed_percentage:.2f}% completed, "
                        f"estimated minimum remaining time: "
                        f"{remaining_time_minutes:.2f} mins."
                    )

            print(f"Repeat {repeat_index + 1} completed.")

        print()
        print("Data generation completed.")
        print(f"Total processed rows: {total_counter}")


def str2bool(value):
    if isinstance(value, bool):
        return value

    value = value.lower()

    if value in ("yes", "true", "t", "y", "1"):
        return True

    if value in ("no", "false", "f", "n", "0"):
        return False

    raise argparse.ArgumentTypeError(
        "Boolean value expected."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-buc", "--bucket",
        required=False,
        type=str,
        default=bucket_name,
        help="Target S3 bucket name"
    )

    parser.add_argument(
        "-k", "--key",
        required=False,
        type=str,
        default="credits/credits_part",
        help="Object key prefix"
    )

    parser.add_argument(
        "-aki", "--access_key_id",
        required=False,
        type=str,
        default=aws_access_key_id,
        help="S3 access key ID"
    )

    parser.add_argument(
        "-sac", "--secret_access_key",
        required=False,
        type=str,
        default=aws_secret_access_key,
        help="S3 secret access key"
    )

    parser.add_argument(
        "-eu", "--endpoint_url",
        required=False,
        type=str,
        default=endpoint_url,
        help="S3-compatible endpoint URL"
    )

    parser.add_argument(
        "-s", "--sep",
        required=False,
        type=str,
        default=",",
        help="Input CSV delimiter"
    )

    parser.add_argument(
        "-ls", "--log_sep",
        required=False,
        type=str,
        default=",",
        help="Output CSV delimiter"
    )

    parser.add_argument(
        "-i", "--input",
        required=False,
        type=str,
        default="/dataops/raw_data/tmdb_5000_credits.csv",
        help="Input CSV or Parquet file path"
    )

    parser.add_argument(
        "-b", "--batch_interval",
        required=False,
        type=float,
        default=0.1,
        help="Sleep time for every row"
    )

    parser.add_argument(
        "-z", "--batch_size",
        required=False,
        type=int,
        default=500,
        help="Number of rows in each output object"
    )

    parser.add_argument(
        "-e", "--source_file_extension",
        required=False,
        type=str,
        default="csv",
        help="Source file extension: csv or parquet"
    )

    parser.add_argument(
        "-oh", "--output_header",
        required=False,
        type=str2bool,
        default=False,
        help="Include CSV header"
    )

    parser.add_argument(
        "-ofp", "--is_output_format_parquet",
        required=False,
        type=str2bool,
        default=False,
        help="Write output as Parquet"
    )

    parser.add_argument(
        "-idx", "--output_index",
        required=False,
        type=str2bool,
        default=False,
        help="Include DataFrame index in output"
    )

    parser.add_argument(
        "-r", "--repeat",
        required=False,
        type=int,
        default=1,
        help="Number of times to repeat the dataset"
    )

    parser.add_argument(
        "-shf", "--shuffle",
        required=False,
        type=str2bool,
        default=False,
        help="Shuffle the dataset"
    )

    parser.add_argument(
        "-exc", "--excluded_cols",
        required=False,
        nargs="+",
        default=[],
        help="Columns excluded from output"
    )

    parser.add_argument(
        "-ob", "--order_by",
        required=False,
        nargs="+",
        default=None,
        help="Columns used to sort the dataset"
    )

    parser.add_argument(
        "-asc", "--ascending",
        required=False,
        type=str2bool,
        default=True,
        help="Sort ascending or descending"
    )

    args = vars(parser.parse_args())

    generator = DataFrameDataGenerator(
        bucket=args["bucket"],
        key=args["key"],
        access_key_id=args["access_key_id"],
        secret_access_key=args["secret_access_key"],
        endpoint_url=args["endpoint_url"],
        input=args["input"],
        batch_interval=args["batch_interval"],
        batch_size=args["batch_size"],
        sep=args["sep"],
        log_sep=args["log_sep"],
        source_file_extension=args["source_file_extension"],
        output_header=args["output_header"],
        is_output_format_parquet=args["is_output_format_parquet"],
        output_index=args["output_index"],
        repeat=args["repeat"],
        shuffle=args["shuffle"],
        excluded_cols=args["excluded_cols"],
        order_by=args["order_by"],
        ascending=args["ascending"]
    )

    generator.df_to_s3_as_log()
