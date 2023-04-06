from pathlib import Path
import pandas as pd
from prefect import flow, task
from random import randint
import boto3
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile


@task(retries=3)
def fetch_whole_dataset(dataset_url: str, download_path:str) -> str:
    """Get Youtube trending data US"""
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_url, path = download_path)
    path = download_path + '/youtube-trending-video-dataset.zip'
    return path
    
@task()
def extract_file_from_archive(zip_file: str, file_to_extract: str, path_to_save: str) -> None: 
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extract(file_to_extract, path=path_to_save)

@task(retries=3)
def read_csv(file_path:str) -> pd.DataFrame:
    """Get Youtube trending data US"""
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(dataset_url, dataset_file)
    df = pd.read_csv(dataset_file)
    return df


@task(retries=3)
def fetch_dataset(dataset_url: str, path:str) -> str:
    """Get Youtube trending data US"""
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(dataset_url, category_file)
    df = pd.read_json(category_file)
    return df
    

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["published_at"] = pd.to_datetime(df["published_at"])
    df["trending_date"] = pd.to_datetime(df["trending_date"])
    print(df.head(10))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path("data/US_youtube_trending_data.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_category_local(df: pd.DataFrame) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path("data/US_category_id.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_cloud(path: Path) -> None:
    """Upload local parquet file to Yandex Cloud"""
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    s3.upload_file(path, 'kaggleyoutubedata', os.path.basename(path))
    return



@flow()
def etl_web_to_cloud() -> None:
    """The main ETL function"""
    
    # Set variables
    dataset_file = "US_youtube_trending_data.csv"
    category_file = "US_category_id.json"
    data_path = str(Path(__file__).parent.resolve())+ '/data'
    dataset_url = "rsrishav/youtube-trending-video-dataset"
    
    # Download dataset from kaggle
    big_file_path = fetch_whole_dataset(dataset_url, data_path)
    #big_file_path=data_path + '/youtube-trending-video-dataset.zip'
    
    # Extract data file
    extract_file_from_archive(big_file_path, dataset_file, data_path)
    
    # Write data file to cloud object storage
    write_cloud(Path(data_path + '/' + dataset_file))
    
    # Extract category file
    extract_file_from_archive(big_file_path, category_file, data_path)
    
    # Write category file to cloud object storage
    write_cloud(Path(data_path + '/' + category_file))
    
    os.remove(big_file_path)
    
    
    '''
    df = fetch_category(dataset_url, category_file)
    path = write_category_local(df)
    write_cloud(path)
    
    df = fetch(dataset_url, dataset_file)
    df_clean = clean(df)
    path = write_local(df_clean)
    write_cloud(path)
    '''



    
if __name__ == "__main__":
    etl_web_to_cloud()
