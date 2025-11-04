import io
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

class Storage:
    def __init__(self, account_name: str = "cnesstorageaccount", file_system: str = "bronze"):
        self.account_name = account_name
        self.file_system = file_system
        key = "cP1htVg+Qtmzi+4dJKz0qEDb1c7uHu3f5VuDWK8/RV2FP/6Qa5GJzT7q2jcGLVvUfwpC3UaFbTEY+ASt38FW+A=="
        self.client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=key,
        )
        self.fs = self.client.get_file_system_client(self.file_system)

# instâncias compartilhadas
bronze = Storage(file_system="bronze")
silver = Storage(file_system="silver")
gold   = Storage(file_system="gold")
artifacts = Storage(file_system="artifacts") 
