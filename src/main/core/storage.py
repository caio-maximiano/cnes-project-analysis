from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError


class Storage:
    """
    Camada fina para operações no Azure Data Lake Gen2 (DFS).
    Centraliza credenciais e expõe utilidades simples.
    """

    def __init__(self, account_name: str = "cnesstorageaccount", file_system: str = "bronze"):
        self.account_name = account_name
        # Recomendado: export AZURE_STORAGE_KEY="sua_chave"
        key = "cP1htVg+Qtmzi+4dJKz0qEDb1c7uHu3f5VuDWK8/RV2FP/6Qa5GJzT7q2jcGLVvUfwpC3UaFbTEY+ASt38FW+A=="#os.environ.get("AZURE_STORAGE_KEY")
        if not key:
            raise RuntimeError(
                "AZURE_STORAGE_KEY não definida no ambiente. "
                "Defina com: export AZURE_STORAGE_KEY='sua_chave'"
            )
        self._key = key
        self.file_system = file_system

        self.client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=self._key,
        )
        self.fs = self.client.get_file_system_client(self.file_system)

    # ------------------------------
    # Navegação
    # ------------------------------
    def list_paths(self, prefix: str = "") -> list[str]:
        return [p.name for p in self.fs.get_paths(path=prefix)]

    # ------------------------------
    # Diretórios
    # ------------------------------
    def ensure_dir(self, remote_dir: str) -> None:
        dir_client = self.fs.get_directory_client(remote_dir)
        try:
            dir_client.create_directory()
        except ResourceExistsError:
            pass

    # ------------------------------
    # Upload
    # ------------------------------
    def upload_file(self, local_path: str | Path, remote_path: str, overwrite: bool = True) -> None:
        local_path = Path(local_path)
        remote_dir = "/".join(remote_path.split("/")[:-1]).strip("/")
        if remote_dir:
            self.ensure_dir(remote_dir)

        file_client = self.fs.get_file_client(remote_path)
        with open(local_path, "rb") as f:
            file_client.upload_data(f, overwrite=overwrite)
        print(f"✅ Uploaded: {local_path}  →  {self.file_system}/{remote_path}")

    # ------------------------------
    # Download
    # ------------------------------
    def download_file(self, remote_path: str) -> bytes:
        file_client = self.fs.get_file_client(remote_path)
        return file_client.download_file().readall()

    # ------------------------------
    # Upload em lote opcional
    # ------------------------------
    def upload_many(self, items: Iterable[tuple[str | Path, str]], overwrite: bool = True) -> None:
        for local, remote in items:
            self.upload_file(local, remote, overwrite=overwrite)
