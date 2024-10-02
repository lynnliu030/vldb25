import boto3
import os
import subprocess as sp
import time
from uuid import uuid4

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# bucket_name = "default-skybucket"
bucket_name = "my-sky-bucket-2"

# create boto3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="us-west-1",
    # endpoint_url="http://127.0.0.1:8002",
)


times = []

small_file_names = []

print("-----------------------Starting small file upload test-----------------------")
print("\n\n")

# upload small files 100 times
for i in range(100):
    # create small file
    small_file_name = "small_file_{}".format(uuid4())
    small_file_names.append(small_file_name)
    print(f"Creating small file {i}...")
    create_small_file = (
        f"dd if=/dev/urandom of={small_file_name} bs=131072 count=1"  # 128kB
    )
    sp.run(create_small_file, shell=True)
    print("create finished.")
    content = b""
    with open(small_file_name, "rb") as f:
        content = f.read()
    # upload small file
    print(f"uploading small file {i}...")
    start = time.time()
    res = s3_client.put_object(
        Bucket=bucket_name, Key=small_file_name, Body=content, ContentLength=131072
    )
    end = time.time()
    print("upload finished.")
    print(f"Small file upload time: {end - start}")
    times.append(end - start)

avg_upload_time = float(sum(times)) / float(len(times))

print("Average upload time: ", avg_upload_time)

# calculate standard deviation
std = 0
for time1 in times:
    std += (time1 - avg_upload_time) ** 2
std = (std / len(times)) ** 0.5

with open("measurements.txt", "w") as f:
    f.write("aws small file put: " + str(avg_upload_time) + "\n")
    f.write("aws small file put std: " + str(std) + "\n")

# clear times
times = []

print("-----------------------Starting small file download test-----------------------")
print("\n\n")

# download small files uploaded
for small_file in small_file_names:
    print(f"Downloading small file {small_file}...")
    start = time.time()
    res = s3_client.get_object(Bucket=bucket_name, Key=small_file)
    end = time.time()
    # store the file in local to streaming it
    content = res["Body"].read()

    print("download finished.")
    print(f"Small file download time: {end - start}")
    times.append(end - start)

avg_download_time = float(sum(times)) / float(len(times))
print("Average download time: ", avg_download_time)

# calculate standard deviation
std = 0
for time1 in times:
    std += (time1 - avg_download_time) ** 2
std = (std / len(times)) ** 0.5

with open("measurements.txt", "a") as f:
    f.write("aws small file get: " + str(avg_download_time) + "\n")
    f.write("aws small file get std: " + str(std) + "\n")

# clear times
times = []

# print("-----------------------Starting big file upload test-----------------------")
# print("\n\n")

# # upload big file by using multipart upload
# # create big file
# print("Creating big file...")
# big_file_name = "big_file_{}".format(uuid4())
# create_big_file = (
#     f"dd if=/dev/urandom of={big_file_name} bs=1048576 count=1024"  # 1024MB
# )
# sp.run(create_big_file, shell=True)
# print("create finished.")

# data_chunks = []

# # split big file into 4096KB chunks
# print("splitting big file into chunks...")
# with open(big_file_name, "rb") as file:
#     data = file.read(5 * 1024 * 1024)  # Convert KB to Bytes
#     while data:
#         data_chunks.append(data)
#         data = file.read(5 * 1024 * 1024)


# print("split finished.")

# # upload big file multipartly
# print("uploading big file...")
# # start = time.time()

# multipart_upload = s3_client.create_multipart_upload(
#     Bucket=bucket_name, Key=big_file_name
# )

# # end = time.time()

# # times.append(end - start)

# upload_id = multipart_upload["UploadId"]

# part_info = {"Parts": []}
# try:
#     # Read the file in chunks and upload each chunk
#     with open(big_file_name, "rb") as file:
#         part_number = 1
#         for data in data_chunks:
#             print(f"Uploading part {part_number}...")
#             start = time.time()
#             response = s3_client.upload_part(
#                 Bucket=bucket_name,
#                 Key=big_file_name,
#                 PartNumber=part_number,
#                 UploadId=upload_id,
#                 Body=data,
#             )
#             end = time.time()
#             times.append(end - start)
#             part_info["Parts"].append(
#                 {"PartNumber": part_number, "ETag": response["ETag"]}
#             )
#             part_number += 1

#     # Complete multipart upload
#     # start = time.time()
#     s3_client.complete_multipart_upload(
#         Bucket=bucket_name,
#         Key=big_file_name,
#         UploadId=upload_id,
#         MultipartUpload=part_info,
#     )
#     # end = time.time()
#     # times.append(end - start)
#     print("Upload completed successfully.")
# except Exception as e:
#     print("Upload failed:", e)
#     s3_client.abort_multipart_upload(
#         Bucket=bucket_name, Key=big_file_name, UploadId=upload_id
#     )

# avg_big_upload_time = float(sum(times)) / float(len(times))
# print("big file upload time: ", avg_big_upload_time)

# with open("measurements.txt", "a") as f:
#     f.write("aws big file put: " + str(avg_big_upload_time) + "\n")

# print("-----------------------Starting big file download test-----------------------")
# print("\n\n")

# times = []

# # download big file
# print("Downloading big file...")
# for _ in range(100):
#     start = time.time()
#     res = s3_client.get_object(Bucket=bucket_name, Key=big_file_name)
#     end = time.time()
#     times.append(end - start)
#     # store the file in local to streaming it
#     # with open(big_file_name, "w") as f:
#     #     f.write(str(res['Body']))
#     print("download finished.")
#     print(f"Big file download time: {end - start}")

# avg_big_download_time = float(sum(times)) / float(len(times))
# print("Average download time: ", avg_big_download_time)

# with open("measurements.txt", "a") as f:
#     f.write("aws big file get: " + str(avg_big_download_time) + "\n")

# # clean up all big files in the bucket
# print("cleaning up big files...")

# res = s3_client.delete_objects(
#     Bucket=bucket_name, Delete={"Objects": [{"Key": big_file_name}]}
# )

# times = []

print("-----------------------Starting list objects test-----------------------")
print("\n\n")

# list objects
print("Listing objects...")
for _ in range(100):
    start = time.time()
    res = s3_client.list_objects_v2(Bucket=bucket_name)
    end = time.time()
    times.append(end - start)
    print("list finished.")
    print(f"List objects time: {end - start}")

avg_list_time = float(sum(times)) / float(len(times))
print("Average list objects time: ", avg_list_time)

# calculate standard deviation
std = 0
for time1 in times:
    std += (time1 - avg_list_time) ** 2
std = (std / len(times)) ** 0.5

with open("measurements.txt", "a") as f:
    f.write("aws list objects: " + str(avg_list_time) + "\n")
    f.write("aws list objects std: " + str(std) + "\n")

print("-----------------------Starting head objects test-----------------------")
print("\n\n")

times = []

# head objects
print("Head objects...")
for small_file in small_file_names:
    start = time.time()
    res = s3_client.head_object(Bucket=bucket_name, Key=small_file)
    end = time.time()
    times.append(end - start)
    print("head finished.")
    print(f"Head objects time: {end - start}")

avg_head_time = float(sum(times)) / float(len(times))
print("Average head objects time: ", avg_head_time)

# calculate standard deviation
std = 0
for time1 in times:
    std += (time1 - avg_head_time) ** 2
std = (std / len(times)) ** 0.5

with open("measurements.txt", "a") as f:
    f.write("aws head objects: " + str(avg_head_time) + "\n")
    f.write("aws head objects std: " + str(std) + "\n")

print("-----------------------Starting delete objects test-----------------------")
print("\n\n")

times = []

# delete objects
print("Deleting objects...")
for small_file in small_file_names:
    start = time.time()
    res = s3_client.delete_object(Bucket=bucket_name, Key=small_file)
    end = time.time()
    times.append(end - start)
    print("delete finished.")
    print(f"Delete objects time: {end - start}")

avg_delete_time = float(sum(times)) / float(len(times))
print("Average delete objects time: ", avg_delete_time)

# calculate standard deviation
std = 0
for time1 in times:
    std += (time1 - avg_delete_time) ** 2
std = (std / len(times)) ** 0.5


with open("measurements.txt", "a") as f:
    f.write("aws delete objects: " + str(avg_delete_time) + "\n")
    f.write("aws delete objects std: " + str(std) + "\n")
