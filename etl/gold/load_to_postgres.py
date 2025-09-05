import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text

SILVER_NORMALIZED_DIR = os.path.join(os.path.dirname(__file__), "../../data/silver/raw_normalized")

engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

def load_to_postgres():
    parquet_files = glob.glob(os.path.join(SILVER_NORMALIZED_DIR, "*/*/*.parquet"))

    if not parquet_files:
        print("Nenhum arquivo encontrado para carga.")
        return

    dfs = [pd.read_parquet(f) for f in parquet_files]
    df_all = pd.concat(dfs, ignore_index=True)

    
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))

    
    df_all.to_sql(
        "precos_combustiveis",
        engine,
        schema="silver",
        if_exists="replace",
        index=False,
        method="multi"
    )

    print(f"Carregado {len(df_all)} registros no Postgres (schema silver).")

if __name__ == "__main__":
    load_to_postgres()
