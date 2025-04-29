import boto3
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def list_files(prefix: str):
    try:
        response = s3_client.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefix)
        return response.get("Contents", [])
    except Exception as e:
        raise Exception(f"Failed to list files: {str(e)}")

def generate_public_url(key: str):
    return f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{key}"

def generate_signed_url(key: str, expires_in: int = 3600):
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': AWS_BUCKET_NAME,
                'Key': key,
                'ResponseContentDisposition': 'inline'  # Force inline viewing instead of download
            },
            ExpiresIn=expires_in
        )
        return url
    except Exception as e:
        raise Exception(f"Failed to generate signed URL: {str(e)}")
