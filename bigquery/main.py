import os

from google.cloud import bigquery
import pandas as pd

project_id = "projekat-426619"
dataset_id = "projekat"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/Stefanche/Documents/GitHub/Inzinjerstvo/spark-weather-processing/bigquery/projekat-426619-4e44441363ff.json"

client = bigquery.Client()

job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

def build_dataframe(path):
    df = pd.read_json(path)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    else:
        df['timestamp_start'] = pd.to_datetime(df['timestamp_start'], format='%Y-%m-%d %H:%M:%S')
        df['timestamp_end'] = pd.to_datetime(df['timestamp_end'], format='%Y-%m-%d %H:%M:%S')
    return df


def write_to_bigquery(table_id, path):
    df = build_dataframe(path)
    client.load_table_from_dataframe(df, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config)
    print(f"Data loaded into {project_id}.{dataset_id}.{table_id}")

      
if __name__ == "__main__":
    write_to_bigquery("base", "C:/Users/Stefanche/Documents/GitHub/Inzinjerstvo/spark-weather-processing/merged_base_file.json")
    write_to_bigquery("average", "C:/Users/Stefanche/Documents/GitHub/Inzinjerstvo/spark-weather-processing/merged_average_file.json")
    write_to_bigquery("alert", "C:/Users/Stefanche/Documents/GitHub/Inzinjerstvo/spark-weather-processing/merged_alert_file.json")
