from pathlib import Path

def write_local(color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    output_file = f"{dataset_file}.parquet"
    output_dir = Path(f"data/{color}")

    output_dir.mkdir(parents=True, exist_ok=True)

    path = Path(f"data/{color}/{dataset_file}.parquet")
    print(output_dir / output_file)
    print(path)

write_local("yellow", "yellow_trip_data_2019")