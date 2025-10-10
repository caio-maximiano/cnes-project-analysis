from __future__ import annotations
import io, logging
import pandas as pd
from typing import Dict, List, Set
from cnes_etl.common import paths
from cnes_etl.common.storage import DataLake
from cnes_etl.common.paths import CnesPaths
from cnes_etl.config import AppConfig
from cnes_etl.jobs.base import BaseJob
from cnes_etl.jobs.tables.base import list_tables, make_table


logger = logging.getLogger(__name__)

class TransformJob(BaseJob):
    def __init__(self, yyyymm: str, cfg: AppConfig, dl: DataLake, paths: CnesPaths) -> None:
        self.yyyymm = yyyymm
        self.cfg = cfg
        self.dl = dl
        self.paths = paths

    def _read_bronze_csv(self, name: str) -> pd.DataFrame:
        logger.warning("Lendo CSV bronze: %s", name)
        rel = f"{self.yyyymm}/{name}{self.yyyymm}.csv"
        raw = self.dl.download_bytes(self.cfg.fs_bronze, rel)
        for enc in ("latin-1", "cp1252", "utf-8-sig"):
            try:
                return pd.read_csv(io.BytesIO(raw), sep=";", quotechar='"', dtype=str, encoding=enc, engine="python", on_bad_lines="warn")
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError("all", b"", 0, 1, "Failed to decode CSV")

    def _load_bronze(self) -> Dict[str, pd.DataFrame]:
        return {
        "tbEstabelecimento": self._read_bronze_csv("tbEstabelecimento"),
        "tbMunicipio": self._read_bronze_csv("tbMunicipio"),
        "rlEstabServClass": self._read_bronze_csv("rlEstabServClass"),
        "tbClassificacaoServico": self._read_bronze_csv("tbClassificacaoServico"),
        "tbCargaHorariaSus": self._read_bronze_csv("tbCargaHorariaSus"),
        "tbAtividadeProfissional": self._read_bronze_csv("tbAtividadeProfissional"),
        "tbDadosProfissionalSus": self._read_bronze_csv("tbDadosProfissionalSus"),
        }


    def _write_silver_parquet(self, df: pd.DataFrame, table: str) -> None:
        local_file = self.paths.silver_parquet(table)
        df.to_parquet(local_file, index=False, engine="pyarrow", compression="snappy")
        self.dl.upload_file(self.cfg.fs_silver, self.paths.silver_object(table), str(local_file))


    def _toposort(self, targets: list[str]) -> list[str]:
        from cnes_etl.jobs.tables import make_table, list_tables  # safe import

        ordered: list[str] = []
        temp: set[str] = set()
        perm: set[str] = set()

        # map name -> dependencies
        all_known = set(list_tables())

        def visit(name: str) -> None:
            if name in perm:
                return
            if name in temp:
                raise RuntimeError(f"Cycle detected in table dependencies near '{name}'")
            if name not in all_known:
                raise KeyError(f"Unknown table dependency '{name}'")

            temp.add(name)
            cls = make_table(name, self.yyyymm).__class__
            for dep in getattr(cls, "dependencies", []):
                visit(dep)
            temp.remove(name)
            perm.add(name)
            ordered.append(name)

        for t in targets:
            visit(t)

        return ordered  # <- garante retorno de list, mesmo se targets == []


    def run(self) -> None:
        bronze = self._load_bronze()
        targets = list_tables() # ou filtrar por CLI no futuro
        order = self._toposort(targets)
        logger.warning("Ordem de execução: %s", order)


        cache: Dict[str, pd.DataFrame] = {}
        for name in order:
            table = make_table(name, self.yyyymm)
            logger.warning("Construindo tabela: %s", name)
            df = table.build(bronze, cache)
            cache[name] = df
            self._write_silver_parquet(df, name)