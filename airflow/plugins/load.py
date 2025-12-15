import pandas as pd
from sqlalchemy import create_engine
import boto3
from io import BytesIO

def load_data(**kwargs):
    print("Iniciando carga a Base de Datos...")
    
    # CONFIGURACIÓN
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_USER = "minio"
    MINIO_PASS = "minio123"
    BUCKET_PROCESSED = "processed"
    FILE_NAME = "weather_mobility_processed.csv"
    
    # Datos de conexión a postgresql 
    PG_URI = "postgresql://airflow:airflow@postgres:5432/airflow" # usuario/contraseña de postgres

    # Conexión s3
    s3 = boto3.client(
        "s3", 
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER, 
        aws_secret_access_key=MINIO_PASS
    )

    # 1. Descargar csv
    try:
        obj = s3.get_object(Bucket=BUCKET_PROCESSED, Key=FILE_NAME)
        df = pd.read_csv(obj['Body'])
        print(f"CSV descargado. Filas a insertar: {len(df)}")
    except Exception as e:
        print(f"Error descargando el archivo procesado (¿Quizás transform falló?): {e}")
        raise e

    # 2. Cargar a postgres
    try:
        engine = create_engine(PG_URI)
        df.to_sql("weather_mobility", engine, if_exists="replace", index=False)
        print("Carga a PostgreSQL exitosa.")
    except Exception as e:
        print(f"Error conectando a Postgres: {e}")
        raise e