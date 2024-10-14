from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from app.services.DataFrameService import DataFrameService
import os

router = APIRouter()

# Definir ruta para subir y procesar archivos CSV
@router.post("/upload")
async def upload_csv(file: UploadFile = File(...), service: DataFrameService = Depends()):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="El archivo debe ser un CSV")

    # Guardar el archivo en la carpeta 'data'
    file_path = os.path.join("data", file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    # Procesar el archivo CSV y cargar el DataFrame
    service.load_data(file_path)

    return {"message": "Archivo CSV subido y procesado exitosamente."}
