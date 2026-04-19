#!/usr/bin/env python3
"""Example: use boto3 to interact with the blob server.

Start the server first:
    python run_server.py

Then run this script:
    python example_client.py
"""

import boto3
from botocore.config import Config

# Connect to the local blob server
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:8080",
    aws_access_key_id="dummy",
    aws_secret_access_key="dummy",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# Create a bucket
s3.create_bucket(Bucket="my-bucket")
print("Created bucket: my-bucket")

# Upload an object
s3.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"Hello, World!")
print("Uploaded: hello.txt")

# Upload with folder path
s3.put_object(Bucket="my-bucket", Key="docs/readme.txt",
              Body=b"This is the readme.")
s3.put_object(Bucket="my-bucket", Key="docs/guide.txt",
              Body=b"This is the guide.")
print("Uploaded: docs/readme.txt, docs/guide.txt")

# Download an object
resp = s3.get_object(Bucket="my-bucket", Key="hello.txt")
data = resp["Body"].read()
print(f"Downloaded hello.txt: {data}")

# Head object
resp = s3.head_object(Bucket="my-bucket", Key="hello.txt")
print(f"Head hello.txt: size={resp['ContentLength']}, etag={resp['ETag']}")

# List objects (top-level)
resp = s3.list_objects_v2(Bucket="my-bucket", Delimiter="/")
print("\nTop-level listing:")
for prefix in resp.get("CommonPrefixes", []):
    print(f"  [folder] {prefix['Prefix']}")
for obj in resp.get("Contents", []):
    print(f"  [file]   {obj['Key']} ({obj['Size']} bytes)")

# List objects (inside docs/)
resp = s3.list_objects_v2(Bucket="my-bucket", Prefix="docs/", Delimiter="/")
print("\ndocs/ listing:")
for obj in resp.get("Contents", []):
    print(f"  {obj['Key']} ({obj['Size']} bytes)")

# Range request
s3.put_object(Bucket="my-bucket", Key="numbers.bin",
              Body=bytes(range(256)))
resp = s3.get_object(Bucket="my-bucket", Key="numbers.bin",
                     Range="bytes=10-19")
print(f"\nRange read [10-19]: {list(resp['Body'].read())}")

# Delete an object
s3.delete_object(Bucket="my-bucket", Key="hello.txt")
print("\nDeleted: hello.txt")

# List buckets
resp = s3.list_buckets()
print(f"\nBuckets: {[b['Name'] for b in resp['Buckets']]}")

print("\nDone!")
