from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = 'social_data'
COLLECTION_NAME = 'results'

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("✅ Conexión con MongoDB exitosa.")
except Exception as e:
    print(f"❌ Error al conectar con MongoDB: {e}")
    collection = None

@app.get("/get-data-mongo")
def get_data_mongo():
    if not collection:
        return {"status": "error", "message": "No hay conexión con MongoDB."}
    
    try:
        data = list(collection.find({}, {"_id": 0}))
        return {"status": "ok", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}
