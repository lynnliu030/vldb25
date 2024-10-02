import argparse
import random
import time
import boto3
import json

import boto3
import os
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd


def generate_random_data(size_in_mb):
    return "".join(
        random.choices(string.ascii_letters + string.digits, k=size_in_mb * 1024 * 1024)
    )


def measure_latency(
    s3_client, bucket_name, file_name, file_size, operation, thread_count
):
    start_time = time.time()
    print("start time", start_time)

    if operation == "download":
        if thread_count > 1 and file_size >= 1024:
            print("multipart download", thread_count)
            multipart_download(s3_client, bucket_name, file_name, thread_count)
        else:
            print("singlepart download", bucket_name, file_name)
            s3_client.download_file(bucket_name, file_name, file_name)

    elif operation == "upload":
        if thread_count > 1 and file_size >= 1024:
            multipart_upload(s3_client, bucket_name, file_name, thread_count)
        else:
            s3_client.upload_file(file_name, bucket_name, file_name)

    print("done")

    end_time = time.time()
    return end_time - start_time


def multipart_upload(s3_client, bucket_name, file_name, thread_count):
    config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=1024 * 1024, max_concurrency=thread_count
    )
    s3_client.upload_file(file_name, bucket_name, file_name, Config=config)


def multipart_download(s3_client, bucket_name, file_name, thread_count):
    # Get file size
    print("Getting file size", bucket_name, file_name)
    response = s3_client.head_object(Bucket=bucket_name, Key=file_name)
    file_size = response["ContentLength"]

    # Calculate part size
    part_size = file_size // thread_count

    def download_part(part_number):
        start = part_number * part_size
        end = start + part_size
        if part_number == thread_count - 1:
            end = ""

        response = s3_client.get_object(
            Bucket=bucket_name, Key=file_name, Range=f"bytes={start}-{end}"
        )
        with open(f"{file_name}.part{part_number}", "wb") as file:
            file.write(response["Body"].read())

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        executor.map(download_part, range(thread_count))

    # Combine parts
    with open(file_name, "wb") as outfile:
        for part_number in range(thread_count):
            with open(f"{file_name}.part{part_number}", "rb") as infile:
                outfile.write(infile.read())
            os.remove(f"{file_name}.part{part_number}")


def benchmark_r2(
    instance_region,
    region,
    bucket,
    access_key_id,
    secret_access_key,
    files,
    file_sizes,
    aws_access_key_id,
    aws_secret_access_key,
):
    filename = f"benchmark_results_instance_{instance_region}_bucket_{region}.csv"
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"https://6352a629df56be37d8d72fc17b30afbe.r2.cloudflarestorage.com",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",  # explicity set region, otherwise may be read from AWS boto3 env
        )

        print(region, bucket)

        results = []
        for file_name, file_size in zip(files, file_sizes):
            # Download file and measure latency
            for thread_count in [1, 4, 8, 16, 32]:
                print(f"Downloading {file_name} with {thread_count} threads")
                download_latency = measure_latency(
                    s3_client, bucket, file_name, file_size, "download", thread_count
                )
                print(download_latency)
                results.append(
                    [
                        bucket,
                        file_name,
                        file_size,
                        thread_count,
                        download_latency,
                        "download",
                    ]
                )

            # Generate a file with random data
            random_data = generate_random_data(file_size)
            with open(file_name, "w") as file:
                file.write(random_data)

            # Upload file with different thread counts and measure latency
            for thread_count in [1, 2, 4, 8, 16, 32]:
                print(f"Uploading {file_size} bytes with {thread_count} threads")
                upload_latency = measure_latency(
                    s3_client, bucket, file_name, file_size, "upload", thread_count
                )
                print(upload_latency)
                results.append(
                    [
                        bucket,
                        file_name,
                        file_size,
                        thread_count,
                        upload_latency,
                        "upload",
                    ]
                )

        df = pd.DataFrame(
            results,
            columns=[
                "bucket",
                "file_name",
                "file_size",
                "thread_count",
                "latency",
                "operation",
            ],
        )
        df.to_csv(filename, index=False)
    except Exception as e:
        print(e)
        open(filename, "w").write(f"Error: {str(e)}")

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        s3_client.upload_file(filename, "skystore-benchmark-results", filename)
        print("Uploaded", filename, "to", "skystore-benchmark-results")
    except Exception as e:
        print("Failed", str(e))


def benchmark_s3(
    instance_region,
    region,
    bucket,
    aws_access_key_id,
    aws_secret_access_key,
    files,
    file_sizes,
):
    filename = f"benchmark_results_instance_{instance_region}_bucket_{region}.csv"
    try:
        s3_client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        print(region, bucket)

        results = []
        for file_name, file_size in zip(files, file_sizes):
            # Download file and measure latency
            for thread_count in [1, 4, 8, 16, 32]:
                print(f"Downloading {file_name} with {thread_count} threads")
                download_latency = measure_latency(
                    s3_client, bucket, file_name, file_size, "download"
                )
                print(download_latency)
                results.append(
                    [
                        bucket,
                        file_name,
                        file_size,
                        thread_count,
                        download_latency,
                        "download",
                    ]
                )

            # Generate a file with random data
            random_data = generate_random_data(file_size)
            with open(file_name, "w") as file:
                file.write(random_data)

            # Upload file with different thread counts and measure latency
            for thread_count in [1, 2, 4, 8, 16, 32]:
                print(f"Uploading {file_size} bytes with {thread_count} threads")
                upload_latency = measure_latency(
                    s3_client, bucket, file_name, file_size, "upload"
                )
                print(upload_latency)
                results.append(
                    [
                        bucket,
                        file_name,
                        file_size,
                        thread_count,
                        upload_latency,
                        "upload",
                    ]
                )

        df = pd.DataFrame(
            results,
            columns=[
                "bucket",
                "file_name",
                "file_size",
                "thread_count",
                "latency",
                "operation",
            ],
        )
        df.to_csv(filename, index=False)
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    except Exception as e:
        open(filename, "w").write(f"Error: {str(e)}")

    try:
        s3_client.upload_file(filename, "skystore-benchmark-results", filename)
        print("Uploaded", filename, "to", "skystore-benchmark-results")
    except Exception as e:
        print("Failed", str(e))


def benchmark_gcs(region):
    pass


def generate_random_data(size_in_mb):
    return "".join(
        random.choices(string.ascii_letters + string.digits, k=size_in_mb * 1024 * 1024)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Object store benchmarker")
    parser.add_argument(
        "--r2-access-key",
        type=str,
        default=None,
        required=False,
        help="Region tag (provider:region",
    )
    parser.add_argument(
        "--r2-secret-key",
        type=str,
        default=None,
        required=False,
        help="Region tag (provider:region",
    )
    parser.add_argument(
        "--s3-access-key",
        type=str,
        default=None,
        required=False,
        help="Region tag (provider:region",
    )
    parser.add_argument(
        "--s3-secret-key",
        type=str,
        default=None,
        required=False,
        help="Region tag (provider:region",
    )
    # parser.add_argument("--benchmark-file", type=str, required=True, help="File specifying what to benchmark")
    parser.add_argument("--region", type=str, required=True, help="Region")
    args = parser.parse_args()

    benchmark_file = "benchmark.json"

    benchmark_data = json.load(open(benchmark_file, "r"))
    for benchmark in benchmark_data:
        if benchmark["provider"] == "r2":
            benchmark_r2(
                args.region,
                benchmark["region"],
                benchmark["bucket"],
                args.r2_access_key,
                args.r2_secret_key,
                benchmark["files"],
                benchmark["file_sizes"],
                args.s3_access_key,
                args.s3_secret_key,
            )