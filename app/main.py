from fastapi import FastAPI, File, UploadFile, HTTPException
from .controllers import AsignaturasController, CSVController,AreasController,CiclosController

app = FastAPI()

# Montar los routers de los controladores
app.include_router(CSVController.router, prefix="/loadFile", tags=["loadFile"])
app.include_router(AsignaturasController.router, prefix="/asignaturas", tags=["asignaturas"])
app.include_router(CiclosController.router, prefix="/ciclos", tags=["ciclos"])
app.include_router(AreasController.router, prefix="/areas", tags=["areas"])

# Variable global para almacenar el DataFrame


