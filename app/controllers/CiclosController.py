
from fastapi import APIRouter, HTTPException
from pyspark.sql.functions import col, expr,regexp_replace
from app.services.DataFrameService import DataFrameService

servicesDataFrame = DataFrameService()
router = APIRouter()

df = servicesDataFrame.get_dataframe()

@router.get("")
async def get_ciclos():
    try:
        # Filtrar los ciclos
        ciclos = df.filter(col("CICLOS").isNotNull()).drop_duplicates(["AREAS"]).distinct().collect()

        ciclos_list = [row["CICLOS"] for row in ciclos]

        return {"ciclos": ciclos_list}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{nombre_ciclo}")
async def get_ciclo(nombre_ciclo: str):
    try:
       # Filtrar la fila que corresponde a l ciclo específicado
        resultado = df.filter(col("CICLOS") == nombre_ciclo).collect()

        if not resultado:
            raise HTTPException(status_code=404, detail=f"No se encontró el ciclo '{nombre_ciclo}'.")
        areas_list = [row.asDict()  for row in resultado]

        return {nombre_ciclo: areas_list}
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