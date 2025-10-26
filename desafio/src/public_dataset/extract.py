import requests
import pandas as pd
from io import StringIO

def extract(dataset_url: str) -> pd.DataFrame:
    print(f"🔹 Extraindo dados de {dataset_url}")
    response = requests.get(dataset_url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))
