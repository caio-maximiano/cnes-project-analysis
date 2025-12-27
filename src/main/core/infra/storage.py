# src/main/core/infra/storage.py

from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential


class Storage:
    def __init__(
        self,
        account_name: str = "cnesstorageaccount",
        file_system: str = "bronze",
    ):
        self.account_name = account_name
        self.file_system = file_system

        credential = DefaultAzureCredential()

        self.client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=credential,
        )

        self.fs = self.client.get_file_system_client(self.file_system)

# instâncias compartilhadas
bronze = Storage(file_system="bronze")
silver = Storage(file_system="silver")
gold = Storage(file_system="gold")
artifacts = Storage(file_system="artifacts")
