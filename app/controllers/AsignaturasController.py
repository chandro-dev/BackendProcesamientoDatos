
from fastapi import APIRouter, HTTPException
from pyspark.sql.functions import col, expr,regexp_replace
from app.services.DataFrameService import DataFrameService

servicesDataFrame = DataFrameService()
router = APIRouter()

df = servicesDataFrame.get_dataframe()

@router.get("")
async def get_asignaturas():
    try:
        # Filtrar las asignaturas
        asignaturas = df.select("ASIGNATURA").distinct().collect()

        asignaturas_list = [row["ASIGNATURA"] for row in asignaturas]

        return {"Asignaturas": asignaturas_list}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{nombre_asignatura}")
async def get_asignatura(nombre_asignatura: str):
    try:
       # Filtrar la fila que corresponde a la asignatura específica
        resultado = df.filter(col("ASIGNATURA") == nombre_asignatura).collect()

        if not resultado:
            raise HTTPException(status_code=404, detail=f"No se encontró la asignatura '{nombre_asignatura}'.")

        # Devolver la primera fila como un diccionario
        row = resultado[0].asDict()
        return {"Asignatura": row}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/periodos/")
async def get_asignaturas():
    try:
        # Excluir las columnas "AREAS" y "CICLOS"
        df_filtered = df.drop("AREAS", "CICLOS")
        asignaturas = df_filtered.collect()
        # Convertir los resultados a una lista de diccionarios
        asignaturas_list = [row.asDict() for row in asignaturas]

        return {"Asignaturas": asignaturas_list}

        return {"Asignaturas": asignaturas_list}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))