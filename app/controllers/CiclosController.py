
from fastapi import APIRouter, HTTPException
from pyspark.sql.functions import col, collect_list
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
async def datos_por_ciclos():
    try:
        # Obtener el DataFrame desde el servicio
        df = servicesDataFrame.get_dataframe()

        # Definir las columnas que deseas incluir en el agrupamiento
        columnas = df.columns

        # Agrupar por "CICLOS" y recolectar todos los valores de cada columna
        exprs = [collect_list(col(c)).alias(c) for c in columnas]
        df_grouped = df.groupBy("CICLOS").agg(*exprs)

        # Recoger los resultados en una lista de diccionarios
        ciclos = df_grouped.collect()
        ciclos_list = [row.asDict() for row in ciclos]

        return {"ciclos": ciclos_list}
    
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))