
from fastapi import APIRouter, HTTPException
from pyspark.sql.functions import col, expr,regexp_replace
from app.services.DataFrameService import DataFrameService

servicesDataFrame = DataFrameService()
router = APIRouter()

df = servicesDataFrame.get_dataframe()

@router.get("")
async def get_areas():
    try:
        # Filtrar las asignaturas
        areas = df.filter(col("AREAS").isNotNull()).drop_duplicates(["AREAS"]).distinct().collect()

        areas_list = [row["AREAS"] for row in areas]

        return {"areas": areas_list}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{nombre_area}")
async def get_area(nombre_area: str):
    try:
       # Filtrar la fila que corresponde a la asignatura específica
        resultado = df.filter(col("AREAS") == nombre_area).collect()

        if not resultado:
            raise HTTPException(status_code=404, detail=f"No se encontró la area '{nombre_area}'.")
        areas_list = [row.asDict()  for row in resultado]

        return {nombre_area: areas_list}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

##Todo por fechas 
@router.get("/periodos/")
async def get_asignaturas():
    try:
        # Excluir las columnas "AREAS" y "CICLOS"
        df_filtered = df.drop("AREAS", "CICLOS")
        asignaturas = df_filtered.collect()
        # Convertir los resultados a una lista de diccionarios
        asignaturas_list = [row.asDict() for row in asignaturas]

        return {"Asignaturas": asignaturas_list}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))