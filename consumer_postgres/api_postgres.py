from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os

app = FastAPI()

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración PostgreSQL
POSTGRES_CONFIG = {
    "dbname": os.getenv('POSTGRES_DB'),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
    "host": os.getenv('POSTGRES_HOST'),
    "port": os.getenv('POSTGRES_PORT', '21406')
}

@app.get("/get-data-postgres")
def get_data_postgres():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()

        cur.execute("SELECT * FROM results")
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in rows]

        cur.close()
        conn.close()

        return {"status": "ok", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}
