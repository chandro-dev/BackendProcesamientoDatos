from fastapi import FastAPI, File, UploadFile, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import tempfile
import os

app = FastAPI()

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("FastAPI_PySpark_CSV") \
    .getOrCreate()

# Variable global para almacenar el DataFrame
df = None

@app.post("/uploadfile/")
async def upload_csv(file: UploadFile = File(...)):
    global df
    # Verificar si el archivo es CSV
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="El archivo debe ser un CSV")

    # Leer el contenido del archivo CSV
    contents = await file.read()

    # Crear un archivo temporal
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file.write(contents)
        temp_file_path = temp_file.name

    df = spark.read.option("delimiter", ";").option("header", True).csv(temp_file_path, header=True, inferSchema=True)

    # Mostrar el DataFrame resultante
    df.show()

    # Eliminar el archivo temporal después de usarlo
    #os.remove(temp_file_path)
    return {"message": "Archivo CSV subido exitosamente.", "columns": df.columns}

@app.get("/")
async def saludo():
    return ("Hola") 

@app.get("/column/{column_name}")
async def get_column_data(column_name: str):
    global df
    if df is None:
        raise HTTPException(status_code=400, detail="No se ha subido ningún archivo CSV aún")

    # Verificar si la columna existe
    if column_name not in df.columns:
        raise HTTPException(status_code=404, detail=f"La columna '{column_name}' no existe en el DataFrame")

    # Seleccionar y devolver los datos de la columna
    column_data = df.select(col(column_name)).collect()
    return {column_name: [row[column_name] for row in column_data]}

