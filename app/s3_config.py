# backend/app/s3_config.py

import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize the S3 client with credentials from the environment
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
