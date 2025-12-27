import io
import pandas as pd
from src.main.core.infra.table import Table
from src.main.core.infra.storage import bronze, silver as silver_store

class Silver(Table):
    layer = "silver"
    allowed_layers = ["bronze", "silver"]

    def __init__(self, name: str, bronze_store=bronze, silver_store=silver_store):
        super().__init__(name)
        self._bronze_fs = bronze_store.fs
        self._silver_fs = silver_store.fs

    def _read_csv_from_fs(self, fs_client, path: str) -> pd.DataFrame:
        file_client = fs_client.get_file_client(path)
        data = file_client.download_file().readall()
        try:
            return pd.read_csv(io.BytesIO(data), sep=";", quotechar='"', dtype=str,
                               encoding="latin-1", engine="python", on_bad_lines="warn")
        except UnicodeDecodeError:
            for enc in ("cp1252", "utf-8-sig"):
                try:
                    return pd.read_csv(io.BytesIO(data), sep=";", quotechar='"', dtype=str,
                                       encoding=enc, engine="python", on_bad_lines="warn")
                except UnicodeDecodeError:
                    continue
            raise

    def read_csv_from_bronze(self, path: str) -> pd.DataFrame:
        return self._read_csv_from_fs(self._bronze_fs, path)

    def read_csv_from_silver(self, path: str) -> pd.DataFrame:
        return self._read_csv_from_fs(self._silver_fs, path)

    def _write_parquet_to_silver(self, df: pd.DataFrame, year_month: str) -> None:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("definition() deve retornar um pandas.DataFrame")
        import pyarrow as pa  # opcional: mantido local
        import pyarrow.parquet as pq
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
        buf.seek(0)
        dest_path = f"{self.name}/{year_month}.parquet"
        self._silver_fs.get_file_client(dest_path).upload_data(buf.getvalue(), overwrite=True)
        print(f"  → Gravado em silver: {dest_path} ({len(df)} registros)")

    def run(self) -> None:
        print(f"Processando Silver: {self.name} para período {getattr(self, 'year_month', 'N/A')}")
        if not hasattr(self, "year_month") or not isinstance(self.year_month, str):
            raise AttributeError("Defina self.year_month (ex.: '202401') antes de .run().")
        df = self.definition()
        self._write_parquet_to_silver(df, self.year_month)
