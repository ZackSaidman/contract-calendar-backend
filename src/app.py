import boto3
from dateparser.search import search_dates
from datetime import datetime
import docx
import json
import os
from zoneinfo import ZoneInfo


# Initialize AWS clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Set your DynamoDB table name
DYNAMODB_TABLE = "DocxUploads"

def download_s3_file(s3link):
    """Downloads a file from an S3 URL using boto3 S3 client."""

    # Initialize the S3 client
    s3_client = boto3.client('s3')
    
    # Parse the S3 URL to get bucket name and file key
    s3_parts = s3link.split("/")

    if len(s3_parts) < 5:
        raise Exception(f"Invalid S3 Link")

    bucket_name = s3_parts[3]
    file_key = "/".join(s3_parts[4:])

    # Define the local file path to store the downloaded file
    filename = "/tmp/{}".format(os.path.basename(file_key))  # Use /tmp for Lambda storage

    try:
        # Download the file from S3
        s3_client.download_file(bucket_name, file_key, filename)
        return filename
    except Exception as e:
        raise Exception(f"Failed to download file from S3: {str(e)}")

def getText(filename):
    doc = docx.Document(filename)
    fullText = []
    for para in doc.paragraphs:
        fullText.append(para.text)
    return '\n'.join(fullText)

def upload_to_dynamodb(filename, s3link, data):
    """Uploads processed data to DynamoDB."""
    table = dynamodb.Table(DYNAMODB_TABLE)

    tableData = []
    for data_point in data:
        tableData.append({
            'text': data_point[0],
            'date': data_point[1].strftime('%Y-%m-%d')
        })
    item = {
        'filename': filename,
        's3link': s3link,
        'tableData': tableData
    }

    table.put_item(Item=item)

def lambda_handler(event, context):
    """Main Lambda function."""
    try:
        body = json.loads(event.get("body"))
        # Assume body contains S3 URL
        s3link = body.get("s3link")
        if not s3link:
            return {"statusCode": 400, "body": json.dumps({"error": "No S3 URL provided"})}

        # Step 1: Download file
        file_path = download_s3_file(s3link)

        # Step 2: Process file
        tz = ZoneInfo('America/Los_Angeles')
        relative_base = datetime.now(tz=tz)
        settings = {
            'RELATIVE_BASE': relative_base,
            'TIMEZONE': str(tz)
        }
        data = search_dates(getText((file_path)), settings=settings)

        if data is None:
            return {
                'statusCode': 500,
                'body': "No dates found."
            }

        # Step 3: Upload extracted data to DynamoDB
        upload_to_dynamodb(s3link.split("/")[-1], s3link, data)

        return {"statusCode": 200, "body": json.dumps({"message": "Data processed and uploaded"})}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
