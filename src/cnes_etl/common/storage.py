from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable
from azure.storage.filedatalake import DataLakeServiceClient


@dataclass
class DataLake:
    account_name: str
    account_key: str

    def _client(self) -> DataLakeServiceClient:
        return DataLakeServiceClient(
        account_url=f"https://{self.account_name}.dfs.core.windows.net",
        credential=self.account_key,
        )

    def upload_bytes(self, fs_name: str, dest_path: str, data: bytes, overwrite: bool = True) -> None:
        fs = self._client().get_file_system_client(fs_name)
        file = fs.get_file_client(dest_path)
        file.upload_data(data, overwrite=overwrite)

    def upload_file(self, fs_name: str, dest_path: str, local_path: str | bytes) -> None:
        fs = self._client().get_file_system_client(fs_name)
        file = fs.get_file_client(dest_path)
        mode = "rb" if isinstance(local_path, str) else None
        with (open(local_path, mode) if mode else local_path) as f: # type: ignore[arg-type]
            file.upload_data(f, overwrite=True)

    def download_bytes(self, fs_name: str, path: str) -> bytes:
        fs = self._client().get_file_system_client(fs_name)
        return fs.get_file_client(path).download_file().readall()