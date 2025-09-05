import os
import logging
import pandas as pd
from unidecode import unidecode  

BRONZE_DIR = os.path.join(os.path.dirname(__file__), "../../data/bronze")
SILVER_RAW_DIR = os.path.join(os.path.dirname(__file__), "../../data/silver/raw")
SILVER_NORMALIZED_DIR = os.path.join(os.path.dirname(__file__), "../../data/silver/raw_normalized")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def export_raw_municipios(bronze_file: str, raw_file: str):

    tmp = pd.read_excel(bronze_file, sheet_name="MUNICIPIOS", header=None)

    
    mask = tmp.apply(lambda row: row.astype(str).str.contains("DATA INICIAL", case=False, na=False)).any(axis=1)
    if not mask.any():
        raise ValueError(f"Não foi possível encontrar 'DATA INICIAL' em {bronze_file}")

    header_row = mask.idxmax()
    df = pd.read_excel(bronze_file, sheet_name="MUNICIPIOS", header=header_row)

    df.to_parquet(raw_file, index=False)
    logging.info(f"Aba MUNICIPIOS exportada para Silver/Raw: {raw_file}")

    return df


def normalize_raw_municipios(df: pd.DataFrame, normalized_file: str):

    df.columns = [
        unidecode(str(c)).strip().lower().replace(" ", "_")
        for c in df.columns
    ]

    
    if "numero_de_postos_pesquisados" in df.columns:
        df["numero_de_postos_pesquisados"] = pd.to_numeric(
            df["numero_de_postos_pesquisados"], errors="coerce"
        ).fillna(0).astype(int)

    for col in ["data_inicial", "data_final"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    
    for col in ["estado", "municipio", "produto"]:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(lambda x: unidecode(x).upper().strip())

    
    df = df.rename(
        columns={"coef_de_variacao_revenda": "coeficiente_variacao_revenda"}
    )

    
    if "unidade_de_medida" in df.columns:
        df["unidade_de_medida"] = (
            df["unidade_de_medida"]
            .astype(str)
            .str.upper()
            .str.replace(" ", "")
            .str.replace("LITRO", "L")
            .str.replace("M³", "M3")
        )

        
        df["moeda"] = df["unidade_de_medida"].str.extract(r"([A-Z$]+)")
        df["medida"] = df["unidade_de_medida"].str.extract(r"/([A-Z0-9]+)")

    df.to_parquet(normalized_file, index=False)
    logging.info(f"Arquivo normalizado salvo em Silver/Raw_Normalized: {normalized_file}")


def process_silver():

    for year in os.listdir(BRONZE_DIR):
        year_path = os.path.join(BRONZE_DIR, year)
        if not os.path.isdir(year_path):
            continue

        for month in os.listdir(year_path):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue

            raw_month_dir = os.path.join(SILVER_RAW_DIR, year, month)
            normalized_month_dir = os.path.join(SILVER_NORMALIZED_DIR, year, month)
            os.makedirs(raw_month_dir, exist_ok=True)
            os.makedirs(normalized_month_dir, exist_ok=True)

            for file in os.listdir(month_path):
                if not file.endswith(".xlsx"):
                    continue

                bronze_file = os.path.join(month_path, file)
                base_name = file.replace(".xlsx", ".parquet")

                raw_file = os.path.join(raw_month_dir, base_name)
                normalized_file = os.path.join(normalized_month_dir, base_name)

                
                if not os.path.exists(raw_file):
                    try:
                        df = export_raw_municipios(bronze_file, raw_file)
                    except Exception as e:
                        logging.error(f"Erro exportando {bronze_file}: {e}")
                        continue
                else:
                    logging.info(f"Raw já existe, pulando: {raw_file}")
                    df = pd.read_parquet(raw_file)

                
                if not os.path.exists(normalized_file):
                    try:
                        normalize_raw_municipios(df, normalized_file)
                    except Exception as e:
                        logging.error(f"Erro normalizando {bronze_file}: {e}")
                else:
                    logging.info(f"Normalizado já existe, pulando: {normalized_file}")


if __name__ == "__main__":
    process_silver()
