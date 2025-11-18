import io
import re
import pandas as pd
from typing import List, Tuple
from ..infra.table import Table
from ..infra.storage import silver as silver_store, gold as gold_store

class Gold(Table):
    layer = "gold"
    allowed_layers = ["silver", "gold"]

    def __init__(self, name: str, silver_store=silver_store, gold_store=gold_store):
        super().__init__(name)
        self._silver_fs = silver_store.fs
        self._gold_fs = gold_store.fs

    # ------------------------------
    # Helpers genéricos internos
    # ------------------------------
    def _download_bytes(self, fs_client, path: str) -> bytes:
        return fs_client.get_file_client(path).download_file().readall()

    def _read_single_parquet(self, fs_client, path: str) -> pd.DataFrame:
        data = self._download_bytes(fs_client, path)
        return pd.read_parquet(io.BytesIO(data), engine="pyarrow")

    def _list_parquets(self, fs_client, base_table_path: str) -> List[Tuple[str, str]]:
        """
        Lista (year_month, path) para arquivos Parquet de uma tabela.
        Suporta:
          - <table>/YYYYMM.parquet
          - <table>/year_month=YYYYMM/data.parquet
        """
        out: List[Tuple[str, str]] = []
        for p in fs_client.get_paths(path=base_table_path, recursive=True):
            if not p.is_directory and p.name.endswith(".parquet"):
                ym = None
                m1 = re.search(r"/(\d{6})\.parquet$", p.name)
                if m1:
                    ym = m1.group(1)
                m2 = re.search(r"year_month=(\d{6})/data\.parquet$", p.name)
                if m2:
                    ym = m2.group(1)
                if ym:
                    out.append((ym, p.name))
        out.sort(key=lambda t: t[0])
        return out

    # ------------------------------
    # Leitura da SILVER
    # ------------------------------
    def read_silver_parquet(self, table_name: str, year_month: str | None = None) -> pd.DataFrame:
        files = self._list_parquets(self._silver_fs, table_name)
        if not files:
            raise FileNotFoundError(f"Nenhum Parquet encontrado em silver/{table_name}")

        if year_month:
            sel = [path for ym, path in files if ym == year_month]
            if not sel:
                raise FileNotFoundError(f"Não achei silver/{table_name} para {year_month}")
            return self._read_single_parquet(self._silver_fs, sel[0])

        # todos os períodos
        return pd.concat(
            [self._read_single_parquet(self._silver_fs, path) for _, path in files],
            ignore_index=True
        )

    # ------------------------------
    # Leitura da GOLD (novo)
    # ------------------------------
    def read_gold_parquet(self, table_name: str, year_month: str | None = None) -> pd.DataFrame:
        files = self._list_parquets(self._gold_fs, table_name)
        if not files:
            raise FileNotFoundError(f"Nenhum Parquet encontrado em gold/{table_name}")

        if year_month:
            sel = [path for ym, path in files if ym == year_month]
            if not sel:
                raise FileNotFoundError(f"Não achei gold/{table_name} para {year_month}")
            return self._read_single_parquet(self._gold_fs, sel[0])

        # todos os períodos
        return pd.concat(
            [self._read_single_parquet(self._gold_fs, path) for _, path in files],
            ignore_index=True
        )

    # utilitários de períodos (opcionais)
    def list_silver_periods(self, table_name: str) -> List[str]:
        return [ym for ym, _ in self._list_parquets(self._silver_fs, table_name)]

    def list_gold_periods(self, table_name: str) -> List[str]:
        return [ym for ym, _ in self._list_parquets(self._gold_fs, table_name)]

    def latest_silver_period(self, table_name: str) -> str | None:
        periods = self.list_silver_periods(table_name)
        return periods[-1] if periods else None

    def latest_gold_period(self, table_name: str) -> str | None:
        periods = self.list_gold_periods(table_name)
        return periods[-1] if periods else None

    # ------------------------------
    # Escrita na GOLD
    # ------------------------------
    def _write_parquet_to_gold(self, df: pd.DataFrame) -> None:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("definition() deve retornar um pandas.DataFrame")
        import pyarrow as pa  # mantido local como na Silver
        import pyarrow.parquet as pq
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
        buf.seek(0)
        dest_path = f"{self.name}/data.parquet"
        self._gold_fs.get_file_client(dest_path).upload_data(buf.getvalue(), overwrite=True)
        print(f"  → Gravado em gold: {dest_path} ({len(df)} registros)")

    # ------------------------------
    # Execução
    # ------------------------------
    def run(self) -> None:
        print(f"Processando Gold: {self.name} para período {getattr(self, 'year_month', 'TODOS')}")
        if not hasattr(self, "definition"):
            raise AttributeError("Implemente .definition(self) na subclasse.")
        df = self.definition()
        self._write_parquet_to_gold(df)
