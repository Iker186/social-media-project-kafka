from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
import traceback

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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

        return {"status": "ok", "data": data}

    except Exception as e:
        print("❌ Error al obtener datos de PostgreSQL:")
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

    finally:
        try:
            if cur: cur.close()
            if conn: conn.close()
        except:
            pass
