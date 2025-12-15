import requests
import json
import boto3
from datetime import datetime

def extract_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=-34.61&longitude=-58.38&hourly=temperature_2m"

    response = requests.get(url)
    data = response.json()

    # guardar raw en MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
    )

    filename = f"weather_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    s3.put_object(
        Bucket="rawdata",
        Key=filename,
        Body=json.dumps(data)
    )

    print("Archivo subido a MinIO:", filename)
