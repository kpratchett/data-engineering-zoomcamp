from pathlib import Path
import pandas as pd
from prefect import flow, task
from datetime import timedelta
import os
from google.cloud import storage


#cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read FHV data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    try:
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
    except:
         df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
         df.rename(columns={'dropOff_datetime': 'dropoff_datetime'}, inplace=True)


    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as compressed csv file"""
    output_file = f"{dataset_file}.csv.gz"
    output_dir = Path(f"../fhv_data/{year}")

    output_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_dir / output_file, compression="gzip")

    path = Path(f"fhv_data/{year}/{dataset_file}.csv.gz")
    return path

# @task()
# def write_gcs(path: Path) -> None:
#     """Upload local csv file to GCS using Prefect block"""
#     gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-hw")
#     #gcs_block = GcsBucket.load("zoomcamp-gcs")
#     gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
#     return

@task()
def write_gcs(source_file_name, destination_blob_name):
    """Uploads a file to the bucket using GCS"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="creds.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket("de-zoomcamp-bucket7")
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


@flow()
def etl_web_to_gcs(month:int, year:int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02d}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02d}.csv.gz"

    print(dataset_url)
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    dest = f"fhv_data/{year}/{dataset_file}.csv.gz"
    write_gcs(path,dest)

@flow()
def fhv_etl_main_flow(
    months: list[int] = [1, 2], year: int = 2020
):
    for month in months:
        etl_web_to_gcs(month, year)


if __name__ == "__main__":
    months = [i for i in range(1,13)]
    year = 2019
    fhv_etl_main_flow(months, year)