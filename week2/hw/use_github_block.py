from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from green_etl_web_to_gcs import etl_web_to_gcs

@task()
def get_flow():
    github_block = GitHub.load("de-github")
    github_block.get_directory(from_path="week2/hw/", local_path=f"../github_pull")
    return

@flow()
def etl_github_to_gcs():
    get_flow()
    etl_web_to_gcs()


deploy = Deployment.build_from_flow(
    flow=etl_github_to_gcs,
    name="github-flow",
)


if __name__ == "__main__":
    deploy.apply()