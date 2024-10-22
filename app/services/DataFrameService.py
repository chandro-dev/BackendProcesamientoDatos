from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col,when, col, trim, regexp_replace

class DataFrameService:
    _instance = None
    default_path="data/tasa_mortalidad_asignaturas.csv"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataFrameService, cls).__new__(cls)
            cls._instance.spark = SparkSession.builder \
                .appName("FastAxI_PySpark_CSV") \
                .getOrCreate()
            cls._instance.df = None
            if cls.default_path is not None:
                cls._instance.load_data(cls.default_path)
        return cls._instance


    def load_data(self, file_path: str):
        # Cargar el DataFrame
        self.df = self.spark.read \
            .option("delimiter", ";") \
            .option("header", True) \
            .csv(file_path)

        # Limpiar las columnas de porcentaje
        columns_to_fix = [col_name for col_name in self.df.columns if col_name not in ["ASIGNATURA", "CICLOS", "AREAS"]]

        for column in columns_to_fix:
            self.df = self.df.withColumn(column, regexp_replace(col(column), "%", ""))
            self.df = self.df.withColumn(column, regexp_replace(col(column), ",", "."))
        
        for column in columns_to_fix:
            self.df = self.df.withColumn(column, col(column).cast("float"))

    # Reemplazar valores vacíos o nulos en las columnas "CICLOS" y "AREAS" con "PENSUM VIEJO"
        self.df = self.df.withColumn(
            "CICLOS", 
            when(col("CICLOS").isNull() | (trim(col("CICLOS")) == ""), "PENSUM VIEJO")
            .otherwise(col("CICLOS"))
        )
        
        self.df = self.df.withColumn(
            "AREAS", 
            when(col("AREAS").isNull() | (trim(col("AREAS")) == ""), "PENSUM VIEJO")
            .otherwise(col("AREAS"))
        )

    def get_dataframe(self) -> DataFrame:
        if self.df is None:
            raise ValueError("El DataFrame no ha sido cargado todavía.")
        return self.df
