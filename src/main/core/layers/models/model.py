# src/main/core/models/model.py
import io
import pandas as pd
import joblib
from typing import Optional
from main.core.infra.storage import silver, gold, artifacts

class Model:
    """
    Base para modelos. Mantém inputs, helpers de leitura e escrita de artefatos.
    """
    def __init__(self, name: str):
        self.name = name
        self.inputs: dict[str, pd.DataFrame] = {}

    # ---------- helpers de leitura ----------
    def _download_bytes(self, fs_client, path: str) -> bytes:
        return fs_client.get_file_client(path).download_file().readall()

    def _read_parquet_from(self, fs_client, path: str) -> pd.DataFrame:
        data = self._download_bytes(fs_client, path)
        return pd.read_parquet(io.BytesIO(data), engine="pyarrow")

    def read_silver_parquet(self, table_name: str, year_month: Optional[str] = None) -> pd.DataFrame:
        """
        Lê Parquet(s) da silver:
          - se year_month for dado: tenta <table>/<YYYYMM>.parquet e, se não achar, <table>/year_month=YYYYMM/data.parquet
          - se None: concatena todos os períodos (<table>/**/*.parquet)
        """
        # lista todos
        paths = [p for p in silver.fs.get_paths(path=table_name, recursive=True) if not p.is_directory and p.name.endswith(".parquet")]
        if not paths:
            raise FileNotFoundError(f"Nenhum Parquet encontrado em silver/{table_name}")

        if year_month:
            # tenta formatos comuns
            wanted = [
                f"{table_name}/{year_month}.parquet",
                f"{table_name}/year_month={year_month}/data.parquet",
            ]
            for w in wanted:
                if any(p.name == w for p in paths):
                    return self._read_parquet_from(silver.fs, w)
            raise FileNotFoundError(f"Não achei silver/{table_name} para {year_month}")

        # sem filtro: concatena todos
        dfs = [self._read_parquet_from(silver.fs, p.name) for p in sorted(paths, key=lambda x: x.name)]
        return pd.concat(dfs, ignore_index=True)

    def read_gold_parquet(self, table_name: str, year_month: Optional[str] = None, file: str = "data.parquet") -> pd.DataFrame:
        """
        Lê Parquet(s) da gold:
          - se year_month for dado: <table>/<YYYYMM>.parquet OU <table>/year_month=YYYYMM/data.parquet
          - se None: tenta <table>/<file> (snapshot único). Se não houver, concatena todos.
        """
        paths = [p for p in gold.fs.get_paths(path=table_name, recursive=True) if not p.is_directory and p.name.endswith(".parquet")]
        if not paths:
            raise FileNotFoundError(f"Nenhum Parquet encontrado em gold/{table_name}")

        if year_month:
            wanted = [
                f"{table_name}/{year_month}.parquet",
                f"{table_name}/year_month={year_month}/data.parquet",
            ]
            for w in wanted:
                if any(p.name == w for p in paths):
                    return self._read_parquet_from(gold.fs, w)
            raise FileNotFoundError(f"Não achei gold/{table_name} para {year_month}")

        # tenta um snapshot "fixo"
        snap = f"{table_name}/{file}"
        if any(p.name == snap for p in paths):
            return self._read_parquet_from(gold.fs, snap)

        # senão, concatena todos
        dfs = [self._read_parquet_from(gold.fs, p.name) for p in sorted(paths, key=lambda x: x.name)]
        return pd.concat(dfs, ignore_index=True)

    # ---------- escrita do artefato ----------
    def _write_joblib_to_artifacts(self, obj, artifact_name: str) -> str:
        """
        Salva em artifacts/<self.name>/<artifact_name>
        Retorna o path remoto salvo.
        """
        remote_path = f"{self.name}/{artifact_name}"
        buf = io.BytesIO()
        joblib.dump(obj, buf)
        buf.seek(0)
        artifacts.fs.get_file_client(remote_path).upload_data(buf.getvalue(), overwrite=True)
        return remote_path
