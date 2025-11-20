import asyncio
import sys

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import aiofiles
import ujson
from pathlib import Path

from parser import main

app = FastAPI()

DATA_FILE = Path("data/cars_data.json")
write_lock = asyncio.Lock()

if sys.platform.startswith("win") and sys.version_info >= (3, 12):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


# ----- чтение данных -----]
async def load_cars_data() -> list:
    if not DATA_FILE.exists():
        raise HTTPException(500, "cars_data.json not found")

    async with aiofiles.open(DATA_FILE, "r", encoding="utf-8") as f:
        content = await f.read()
        return ujson.loads(content)


# ----- 1. GET /api/cars — список машин -----]
@app.get("/api/cars")
async def get_all_cars():
    data = await load_cars_data()
    return JSONResponse(data)


# ----- 2. GET /api/cars/{model_name} — поиск модели -----]
@app.get("/api/cars/{model_name}")
async def get_car_by_model(model_name: str):
    data = await load_cars_data()

    model_name_lower = model_name.lower()

    for car in data:
        if car.get("model_name", "").lower() == model_name_lower:
            return JSONResponse(car)

    raise HTTPException(404, "Model not found")


# ----- 3. POST /api/refresh — обновить cars_data.json -----]
@app.post("/api/refresh")
async def refresh_data():
    await main()
    return {"message": "Data refreshed successfully"}
