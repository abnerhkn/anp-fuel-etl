import os
import requests
import logging
from datetime import datetime, timedelta

BASE_URL = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/arquivos-lpc"

DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data/bronze")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def download_weekly_reports():
    start_date = datetime(2025, 1, 5)  
    end_date = datetime(2025, 1, 11)   
    today = datetime.today()

    while start_date <= today:
        year = start_date.year
        month = str(start_date.month).zfill(2)
        month_dir = os.path.join(DATA_DIR, str(year), month)
        os.makedirs(month_dir, exist_ok=True)

        filename = f"resumo_semanal_lpc_{start_date.date()}_{end_date.date()}.xlsx"
        filepath = os.path.join(month_dir, filename)

        if os.path.exists(filepath):
            logging.info(f"Arquivo já existe, pulando: {filename}")
        else:
            url = f"{BASE_URL}/{year}/{filename}"
            logging.info(f"Baixando {url}")

            resp = requests.get(url, stream=True)
            if resp.status_code == 200:
                with open(filepath, "wb") as f:
                    f.write(resp.content)
                logging.info(f"Arquivo salvo em {filepath}")
            else:
                logging.warning(f"Arquivo não encontrado: {url}")
                break  

        
        start_date += timedelta(days=7)
        end_date += timedelta(days=7)


if __name__ == "__main__":
    download_weekly_reports()
