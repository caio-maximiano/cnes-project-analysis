from __future__ import annotations

import os
import zipfile
import shutil
import subprocess, shlex
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# SSL trust (macOS keychain se disponível; senão certifi)
try:
    import truststore  # type: ignore
    truststore.inject_into_ssl()
    _VERIFY = True
    print("✅ Using macOS truststore")
except Exception:
    import certifi  # type: ignore
    _VERIFY = certifi.where()
    print("⚠️ Using certifi bundle")

from ..core.storage import Storage

class Extractor:
    """
    Faz download do ZIP CNES, extrai localmente e envia os CSVs para o Data Lake (bronze).
    """

    def __init__(self, year_month: str | None = None, months_back: int = 3):
        if year_month:
            self.year_month = year_month
        else:
            self.year_month = (datetime.today() - relativedelta(months=months_back)).strftime("%Y%m")

        self.download_url = f"https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_{self.year_month}.ZIP"

        self.local_zip_path = Path(f"./local_storage/zip/BASE_DE_DADOS_CNES_{self.year_month}.ZIP")
        self.local_extract_dir = Path(f"./local_storage/csv/cnes_extract_{self.year_month}")
        self.local_zip_path.parent.mkdir(parents=True, exist_ok=True)
        self.local_extract_dir.mkdir(parents=True, exist_ok=True)

        # camadas no ADLS
        self.storage_bronze = Storage(file_system="bronze")

    # ------------------------------
    def download_zip(self) -> None:
        print(f"⬇️  Downloading: {self.download_url}")
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        try:
            with session.get(self.download_url, timeout=(15, 300), stream=True, verify=_VERIFY) as r:
                r.raise_for_status()
                with open(self.local_zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            print(f"✅ Downloaded to {self.local_zip_path}")
        except requests.exceptions.SSLError as e:
            print(f"[TLS] Requests falhou ({e}); tentando via curl…")
            cmd = f'curl -L --fail --retry 5 --retry-delay 5 -o "{self.local_zip_path}" "{self.download_url}"'
            subprocess.run(shlex.split(cmd), check=True)
            print(f"✅ Download via curl: {self.local_zip_path}")

    # ------------------------------
    def extract_zip(self) -> None:
        print(f"🗂️  Extracting to {self.local_extract_dir}")
        with zipfile.ZipFile(self.local_zip_path, "r") as z:
            z.extractall(self.local_extract_dir)
        print("✅ Extraction completed")

    # ------------------------------
    def upload_to_datalake(self) -> None:
        """
        Sobe todos os .csv extraídos para: bronze/<YYYYMM>/<arquivo.csv>
        """
        print("☁️  Uploading CSVs to Data Lake (bronze)…")
        uploads = []
        for root, _dirs, files in os.walk(self.local_extract_dir):
            for name in files:
                if name.lower().endswith(".csv"):
                    local = Path(root) / name
                    remote = f"{self.year_month}/{name}"
                    uploads.append((local, remote))
        self.storage_bronze.upload_many(uploads)
        print("✅ Upload completed")

    # ------------------------------
    def cleanup(self) -> None:
        if self.local_zip_path.exists():
            self.local_zip_path.unlink()
            print(f"🧹 Removed ZIP: {self.local_zip_path}")
        if self.local_extract_dir.exists():
            shutil.rmtree(self.local_extract_dir)
            print(f"🧹 Removed dir: {self.local_extract_dir}")
