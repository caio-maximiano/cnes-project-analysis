import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from azure.storage.filedatalake import DataLakeServiceClient
import zipfile
import requests
import shutil
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class Extractor:
    """
    Extractor class for downloading, extracting, and uploading CNES data to Azure Data Lake.
    """

    def __init__(self, year_month: str = None, months_back: int = 3):
        if year_month:
            self.year_month = year_month
        else:
            self.year_month = (datetime.today() - relativedelta(months=months_back)).strftime("%Y%m")

        self.account_name = "cnesstorage"
        self.account_key = "/ae47eZuE0NGPopxVHEkxOKsQwtEm3qQM0vBRPBRbB5nAW1zO6FPkEO9gwNQwkqExaVhOyHWgb68+AStIau+Uw=="#os.environ["AZURE_STORAGE_KEY"]
        self.file_system_name = "bronze"

        self.datalake_target_path = f"/{self.year_month}"
        self.local_zip_path = f"./local_storage/zip/BASE_DE_DADOS_CNES_{self.year_month}.ZIP"
        self.local_extract_dir = f"./local_storage/csv/cnes_extract_{self.year_month}"
        self.download_url = f"https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_{self.year_month}.ZIP"

    def download_zip(self):

        os.makedirs(os.path.dirname(self.local_zip_path), exist_ok=True)
        print(f"Starting File Download: {self.download_url}")

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        response = session.get(self.download_url, timeout=(15, 300))
        response.raise_for_status()

        with open(self.local_zip_path, "wb") as f:
            f.write(response.content)

        print(f"Download completed: {self.local_zip_path}")

    def extract_zip(self):
        os.makedirs(self.local_extract_dir, exist_ok=True)
        print(f"Extracting ZIP to {self.local_extract_dir}")
        with zipfile.ZipFile(self.local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.local_extract_dir)
        print("Extraction completed.")

    def upload_to_datalake(self):
        print("Connecting to Azure Data Lake...")
        datalake_client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=self.account_key
        )
        file_system_client = datalake_client.get_file_system_client(self.file_system_name)

        for root, _, files in os.walk(self.local_extract_dir):
            for file_name in files:
                if file_name.lower().endswith(".csv"):
                    local_file_path = os.path.join(root, file_name)
                    destination_path = f"{self.datalake_target_path}/{file_name}"

                    print(f"Uploading {file_name} to Data Lake -> {destination_path}")
                    file_client = file_system_client.get_file_client(destination_path)

                    with open(local_file_path, "rb") as data:
                        file_client.upload_data(
                            data,
                            overwrite=True,
                            max_concurrency=8,
                            chunk_size=4 * 1024 * 1024
                        )
        print("Upload completed successfully.")

    def cleanup(self):
        """
        Remove local ZIP file and extracted CSV directory to save disk space.
        """
        if os.path.exists(self.local_zip_path):
            os.remove(self.local_zip_path)
            print(f"Removed ZIP file: {self.local_zip_path}")
        if os.path.exists(self.local_extract_dir):
            shutil.rmtree(self.local_extract_dir)
            print(f"Removed extracted folder: {self.local_extract_dir}")

if __name__ == "__main__":
    extractor = Extractor()
    extractor.download_zip()
    extractor.extract_zip()
    extractor.upload_to_datalake()
    extractor.cleanup()

