import pandas as pd
from pathlib import Path

csv_path = Path(__file__).parent / "viajes_slim.csv"

data_viajes = pd.read_csv(csv_path, sep="|")

print(f"Filas: {len(data_viajes):,}  |  Columnas: {data_viajes.shape[1]}")
print(data_viajes.describe())