import boto3
import json
import pandas as pd
from io import StringIO

def transform_data(**kwargs):
    print("=== INICIANDO TRANSFORMACIÓN ===")
    
    # Configuración
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_USER = "minio"
    MINIO_PASS = "minio123"
    BUCKET_RAW = "rawdata"
    BUCKET_PROCESSED = "processed" 
    OUTPUT_FILE = "weather_mobility_processed.csv"

    # Conexión
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
    )

    # Creación del bucket processed
    try:
        s3.head_bucket(Bucket=BUCKET_PROCESSED)
        print(f"Bucket '{BUCKET_PROCESSED}' encontrado.")
    except Exception:
        print(f"Bucket '{BUCKET_PROCESSED}' no existe. Creándolo ahora...")
        try:
            s3.create_bucket(Bucket=BUCKET_PROCESSED)
            print(f"Bucket '{BUCKET_PROCESSED}' creado exitosamente.")
        except Exception as e:
            print(f"No se pudo crear el bucket. Error: {e}")
            raise e

    # 1. Leer datos de raw
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_RAW)
        if "Contents" not in objects:
            print("ERROR: No hay archivos en rawdata.")
            raise Exception("Bucket rawdata vacío")
            
        latest = sorted(objects["Contents"], key=lambda x: x["LastModified"], reverse=True)[0]["Key"]
        print(f"Procesando archivo: {latest}")
        
        obj = s3.get_object(Bucket=BUCKET_RAW, Key=latest)
        data = json.loads(obj["Body"].read())

    except Exception as e:
        print(f"Error leyendo de MinIO: {e}")
        raise e

    # 2. Transforma
    try:
        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"]
        })
        
        df_clean = df.dropna()
        print(f"Datos listos. Filas: {len(df_clean)}")

    except Exception as e:
        print(f"Error transformando datos: {e}")
        raise e

    # 3. Carga
    try:
        csv_buffer = StringIO()
        df_clean.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=BUCKET_PROCESSED,
            Key=OUTPUT_FILE,
            Body=csv_buffer.getvalue()
        )
        print(f"¡ÉXITO! Archivo subido a {BUCKET_PROCESSED}/{OUTPUT_FILE}")
    except Exception as e:
        print(f"Error subiendo a MinIO: {e}")
        raise e