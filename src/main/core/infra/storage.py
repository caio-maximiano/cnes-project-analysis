import os
from azure.storage.filedatalake import DataLakeServiceClient


class Storage:
    def __init__(
        self,
        account_name: str | None = None,
        file_system: str = "bronze",
    ):
        self.account_name = account_name or os.getenv("STORAGE_ACCOUNT_NAME")
        self.file_system = file_system

        if not self.account_name:
            raise ValueError("STORAGE_ACCOUNT_NAME não definido")

        account_key = os.getenv("STORAGE_ACCOUNT_KEY")
        if not account_key:
            raise ValueError("STORAGE_ACCOUNT_KEY não definido")

        self.client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=account_key,
        )

        self.fs = self.client.get_file_system_client(self.file_system)


# instâncias compartilhadas
bronze = Storage(file_system="bronze")
silver = Storage(file_system="silver")
gold = Storage(file_system="gold")
artifacts = Storage(file_system="artifacts")
