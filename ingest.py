import pandas as pd
import os
from sqlalchemy import create_engine
import urllib.parse

USER = "RAGHUVALLURU"
PASSWORD = "Vkotianitha@1829"
ACCOUNT = "tctdvrk-xgc62761"
DATABASE = "F1_DB"
SCHEMA = "RAW"
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"

encoded_password = urllib.parse.quote_plus(PASSWORD)

engine = create_engine(
    f"snowflake://{USER}:{encoded_password}@{ACCOUNT}/{DATABASE}/{SCHEMA}"
    f"?warehouse={WAREHOUSE}&role={ROLE}"
)

data_path = "Data"


for file in os.listdir(data_path):
    if file.endswith(".csv"):
        table_name = file.replace(".csv", "").lower()
        file_path = os.path.join(data_path, file)

        print(f"\n Ingesting {file} → {table_name}")

        try:
            chunk_iter = pd.read_csv(file_path, chunksize=10000, low_memory=False)

            first_chunk = True

            for chunk in chunk_iter:
                if file == "lap_times.csv":
                    continue
                chunk.columns = chunk.columns.str.lower()

                chunk.to_sql(
                    table_name,
                    engine,
                    if_exists="replace" if first_chunk else "append",
                    index=False,
                    method="multi"
                )

                first_chunk = False
                print(f"   Inserted chunk into {table_name}")

            print(f" Finished {table_name}")

        except Exception as e:
            print(f" Error loading {file}: {e}")
print("\n All files ingested successfully!")