# src/main/core/layers/models/model.py
from __future__ import annotations
import io
import re
import joblib
import pandas as pd
from typing import List, Tuple
from sklearn.pipeline import Pipeline

# stores compartilhados do projeto
from ...infra.storage import gold as gold_store, artifacts as artifacts_store

class Model:
    """
    Base para modelos. Mantém inputs, helpers de leitura da GOLD e escrita de artefatos.
    Contrato mínimo:
      - __init__(artifact_name)
      - pipeline() -> Pipeline   (a subclasse implementa e, opcionalmente, roda seu QC)
    """

    def __init__(self, artifact_name: str, *, gold_fs=None, artifacts_fs=None):
        self.artifact_name = artifact_name
        self.inputs: dict[str, pd.DataFrame] = {}

        # permite injetar FS (para testes); por padrão usa os singletons do projeto
        self._gold_fs = gold_fs or gold_store.fs
        self._artifacts_fs = artifacts_fs or artifacts_store.fs

    # ============================================================
    # 1) Contrato que a subclasse deve implementar
    # ============================================================
    def pipeline(self) -> Pipeline:
        """
        Retorna o Pipeline já pronto/treinado (subclasse implementa).
        Você pode chamar execute_quality_check() dentro da sua implementação
        e decidir se retorna o pipeline ou lança erro se reprovar.
        """
        raise NotImplementedError("Método 'pipeline' não implementado.")

    # ============================================================
    # 2) Helpers de IO — leitura da GOLD
    #    Suporta layouts:
    #      - gold/<table>/YYYYMM.parquet
    #      - gold/<table>/year_month=YYYYMM/data.parquet
    # ============================================================
    def _download_bytes(self, fs_client, path: str) -> bytes:
        return fs_client.get_file_client(path).download_file().readall()

    def _read_single_parquet_from_gold(self, path: str) -> pd.DataFrame:
        data = self._download_bytes(self._gold_fs, path)
        return pd.read_parquet(io.BytesIO(data), engine="pyarrow")

    def _list_gold_parquets(self, table_name: str) -> List[Tuple[str, str]]:
        """
        Retorna lista [(YYYYMM, path)] para a tabela na GOLD (ordem crescente de período).
        """
        results: List[Tuple[str, str]] = []
        base = f"{table_name}"
        for p in self._gold_fs.get_paths(path=base, recursive=True):
            if p.is_directory or not p.name.endswith(".parquet"):
                continue
            ym = None
            m1 = re.search(r"/(\d{6})\.parquet$", p.name)  # <table>/YYYYMM.parquet
            if m1:
                ym = m1.group(1)
            m2 = re.search(r"year_month=(\d{6})/data\.parquet$", p.name)  # <table>/year_month=YYYYMM/data.parquet
            if m2:
                ym = m2.group(1)
            if ym:
                results.append((ym, p.name))
        results.sort(key=lambda t: t[0])
        return results

    def read_gold_parquet(self, table_name: str, year_month: str | None = None) -> pd.DataFrame:
        """
        Lê Parquet(s) da GOLD.
          - year_month=None  -> concatena todos os períodos encontrados
          - year_month="202401" -> lê apenas esse período
        """
        files = self._list_gold_parquets(table_name)
        if not files:
            raise FileNotFoundError(f"Nenhum Parquet encontrado em gold/{table_name}")

        if year_month is not None:
            sel = [p for ym, p in files if ym == year_month]
            if not sel:
                raise FileNotFoundError(f"Não achei gold/{table_name} para {year_month}")
            return self._read_single_parquet_from_gold(sel[0])

        # concatena todos
        dfs = [self._read_single_parquet_from_gold(p) for _, p in files]
        return pd.concat(dfs, ignore_index=True)

    # ============================================================
    # 3) Helper de escrita — artifacts
    # ============================================================
    def _save_artifact(self, pipe: Pipeline) -> str:
        """
        Salva em artifacts/<artifact_name> (flat). Se quiser hierarquia, mude aqui.
        """
        dest_path = f"{self.artifact_name}"
        buf = io.BytesIO()
        joblib.dump(pipe, buf)
        buf.seek(0)
        self._artifacts_fs.get_file_client(dest_path).upload_data(buf.getvalue(), overwrite=True)
        return dest_path

    # ============================================================
    # 4) Quality Check (opcional para uso dentro do pipeline)
    # ============================================================
    #modify the method to pass the metrics thresholds as parameters and give error if not met
    def execute_quality_check(self, pipe: Pipeline, x_test: pd.DataFrame, y_test: pd.Series, thresholds: dict) -> dict:
        """
        Executa QC do Pipeline e retorna métricas (MAE, RMSE, R²).
        Use dentro da sua implementação de `pipeline()` para decidir salvar/retornar.
        """
        from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

        y_pred = pipe.predict(x_test)
        mae = float(mean_absolute_error(y_test, y_pred))
        rmse = float(root_mean_squared_error(y_test, y_pred))
        r2 = float(r2_score(y_test, y_pred))

        # Verifica se as métricas atendem aos limites
        for metric, value in zip(["MAE", "RMSE", "R2"], [mae, rmse, r2]):
            if metric in thresholds and value < thresholds[metric]:
                raise ValueError(f"QC falhou: {metric}={value} < {thresholds[metric]}")
        print(f"✅ QC métricas: MAE={mae}, RMSE={rmse}, R2={r2}")
        return {"MAE": mae, "RMSE": rmse, "R2": r2}

    # ============================================================
    # 5) Execução — pega o pipeline e salva o artefato
    #    (QC fica a cargo do pipeline(), se você quiser travar antes)
    # ============================================================
    def run(self) -> None:
        pipe = self.pipeline()  # a subclasse pode ter chamado execute_quality_check internamente
        dest = self._save_artifact(pipe)
        print(f"✅ Artifact salvo em artifacts/{dest}")
