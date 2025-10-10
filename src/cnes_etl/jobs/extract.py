from __future__ import annotations
import logging, zipfile, shutil
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
import subprocess, shlex


from cnes_etl.common.http import new_session
from cnes_etl.common.storage import DataLake
from cnes_etl.common.paths import CnesPaths
from cnes_etl.common.utils import ensure_dir
from cnes_etl.config import AppConfig
from cnes_etl.jobs.base import BaseJob


logger = logging.getLogger(__name__)


class ExtractJob(BaseJob):
    def __init__(self, yyyymm: str, cfg: AppConfig, dl: DataLake, paths: CnesPaths) -> None:
        self.yyyymm = yyyymm
        self.cfg = cfg
        self.dl = dl
        self.paths = paths

    def _url(self) -> str:
        return self.cfg.base_url + self.cfg.download_path.replace("{YYYYMM}", self.yyyymm)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _download(self, url: str, dest: Path) -> None:
        ensure_dir(dest.parent)
        s = new_session()
        try:
            with s.get(url, timeout=(15, 300), stream=True) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1_048_576):
                        if chunk:
                            f.write(chunk)
        except requests.exceptions.SSLError:
            # Fallback confiável no mac (usa Keychain do sistema)
            cmd = f'curl -L --fail --retry 5 --retry-delay 5 -o "{dest}" "{url}"'
            subprocess.run(shlex.split(cmd), check=True)

    def _extract_zip(self, zip_file: Path, target_dir: Path) -> None:
        ensure_dir(target_dir)
        with zipfile.ZipFile(zip_file, 'r') as zf:
            zf.extractall(target_dir)

    def _upload_csvs(self, src_dir: Path) -> None:
        for p in src_dir.rglob("*.csv"):
            dest = f"{self.paths.bronze_prefix()}/{p.name}"
            logger.info("Uploading %s -> %s", p.name, dest)
            self.dl.upload_file(self.cfg.fs_bronze, dest, str(p))

    def _cleanup(self, zip_file: Path, extract_dir: Path) -> None:
        if zip_file.exists():
            zip_file.unlink()
        if extract_dir.exists():
            shutil.rmtree(extract_dir)

    def run(self) -> None:
        url = self._url()
        zip_path = self.paths.zip_file()
        extract_dir = self.paths.extract_dir()

        logger.warning("Downloading %s", url)
        self._download(url, zip_path)

        logger.warning("Extracting to %s", extract_dir)
        self._extract_zip(zip_path, extract_dir)

        logger.warning("Uploading CSVs to bronze")
        self._upload_csvs(extract_dir)

        logger.warning("Cleanup local artifacts")
        self._cleanup(zip_path, extract_dir)