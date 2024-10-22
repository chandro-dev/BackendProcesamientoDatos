
from fastapi import APIRouter, HTTPException
from pyspark.sql.functions import col, expr,regexp_replace,collect_list
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
async def informacion_por_asignatura():
    try:
        # Obtener el DataFrame desde el servicio
        df = servicesDataFrame.get_dataframe()

        # Agrupar por "ASIGNATURA" y recolectar todos los valores de las demás columnas
        columnas = df.columns

        # Colectar las columnas restantes para cada asignatura
        exprs = [collect_list(col(c)).alias(c) for c in columnas]
        df_grouped = df.groupBy("ASIGNATURA").agg(*exprs)

        # Recoger los resultados en una lista de diccionarios
        asignaturas = df_grouped.collect()
        asignaturas_list = [row.asDict() for row in asignaturas]

        return {"informacion_por_asignatura": asignaturas_list}
    
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))