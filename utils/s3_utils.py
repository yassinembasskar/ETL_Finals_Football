import boto3
import os
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET', 'matchiq-data')

client = boto3.client(
    's3',
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)

def upload_file(local_path: str, s3_key: str) -> None:
    client.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"Uploaded {local_path} → s3://{S3_BUCKET}/{s3_key}")

def download_file(s3_key: str, local_path: str) -> None:
    client.download_file(S3_BUCKET, s3_key, local_path)
    print(f"Downloaded s3://{S3_BUCKET}/{s3_key} → {local_path}")

def list_files(prefix: str = '') -> list:
    response = client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', [])]

