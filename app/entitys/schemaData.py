from pyspark.sql.types import StructType, StructField, StringType
# Definir el esquema esperado para el CSV

schema =  StructType([
    StructField("ASIGNATURA", StringType(), True),
    StructField("2017 1", StringType(), True),
    StructField("2017 2", StringType(), True),
    StructField("2018 1", StringType(), True),
    StructField("2018 2", StringType(), True),
    StructField("2019 1", StringType(), True),
    StructField("2019 2", StringType(), True),
    StructField("2020 1", StringType(), True),
    StructField("2020 2", StringType(), True),
    StructField("2021 1", StringType(), True),
    StructField("2021 2", StringType(), True),
    StructField("2022 1", StringType(), True),
    StructField("2022 2", StringType(), True),
    StructField("2023 1", StringType(), True),
    StructField("2023 2", StringType(), True),
    StructField("2024 1", StringType(), True),
    StructField("CICLOS", StringType(), True),
    StructField("AREAS", StringType(), True),
])