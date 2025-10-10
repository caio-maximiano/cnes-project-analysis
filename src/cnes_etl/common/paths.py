from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CnesPaths:
    yyyymm: str
    local_zip_dir: Path
    local_csv_dir_prefix: Path
    local_curated_dir: Path

    def zip_file(self) -> Path:
        return self.local_zip_dir / f"BASE_DE_DADOS_CNES_{self.yyyymm}.ZIP"

    def extract_dir(self) -> Path:
        return Path(f"{self.local_csv_dir_prefix}{self.yyyymm}")

    def silver_parquet(self, table: str) -> Path:
        return Path(self.local_curated_dir) / f"{table}_{self.yyyymm}.parquet"

    # Data Lake logical paths
    def bronze_prefix(self) -> str:
        return f"/{self.yyyymm}"

    def silver_object(self, table: str) -> str:
        return f"{table}/year_month={self.yyyymm}/data.parquet"