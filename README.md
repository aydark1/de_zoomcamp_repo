DESCRIPTION
Project get youtube trending data from kaggle https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset, load data to object storage and data wareshouse.

TECHNOLOGIES
Cloud: Yandex
Infrastructure as code (IaC): Terraform
Workflow orchestration: Prefect
Object Storage: Yandex Object Storage (S3 compatible)
Data Wareshouse: Managed Service for PostgreSQL
Batch processing: Spark (PySpark)
Data visualization: Yandex DataLens
Dashboard: https://datalens.yandex/pz9nrbuqsruif

PROJECT STRUCTURE
01_terraform - create cloud resources
02_load_data_to_storage - extract data from kaggle and load it to object storage
03_transform_load_to_db - read data from object storage, transform, clean and save to datawarehouse




