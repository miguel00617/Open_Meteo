Proceso ETL de Datos(Ingeniería de Datos)
Proyecto de proceso de etl, con data lake y data warehouse.

Este proyecto de ingeniería de datos, extrae, transforma/procesa y carga datos desde una api pública de Pronóstico del clima, todo este flujo de datos se automatiza, se conteneriza y se carga a una base de datos.

Primero establecemos los archivos .py para que se orquesten con un Dag de Airflow, para que se complete todo el flujo. A partir de eso, activamos el dag en la interfaz web de airflow, asío comenzando todo el proceso.

Luego revisamos bien que esté todo en orden, observando en la interfaz de Minio los buckets y los datos en dos estados, dato en crudo(Data Lake, es un .json) y el procesado, listo para integrar en nuestra base de datos de Postresql(data Warehouse, es uyn .csv).

Con estos datos se pueden usar para realizar análisis y hasta visualizaciones y predicciones de los próximos días.

#Tencnologías: Python, Airflow, Minio, PostgreSQL y Docker. #Fuente de Datos: https://api.open-meteo.com/v1/forecast?latitude=-34.61&longitude=-58.38&hourly=temperature_2m . 

