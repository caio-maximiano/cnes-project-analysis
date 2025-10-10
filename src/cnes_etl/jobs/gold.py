from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, List, Set, Iterable
import pandas as pd

from cnes_etl.jobs.base import BaseJob
from cnes_etl.common.storage import DataLake
from cnes_etl.common.paths import CnesPaths
from cnes_etl.config import AppConfig
from cnes_etl.jobs.tables import list_tables, make_table
from cnes_etl.jobs.tables.base import Layer

logger = logging.getLogger(__name__)

class GoldJob(BaseJob):
    """Executa apenas carga FULL de tabelas GOLD (agrega todos os períodos do Silver)."""

    def __init__(self, cfg: AppConfig, dl: DataLake, paths: CnesPaths) -> None:
        self.cfg = cfg
        self.dl = dl
        self.paths = paths

    # ---------- IO helpers (Silver) ----------
    def _read_silver_parquet(self, table: str, yyyymm: str) -> pd.DataFrame:
        dest = f"{table}/year_month={yyyymm}/data.parquet"
        raw = self.dl.download_bytes(self.cfg.fs_silver, dest)
        local_file = self.paths.silver_parquet(f"__tmp_read_{table}_{yyyymm}")
        Path(local_file).parent.mkdir(parents=True, exist_ok=True)
        with open(local_file, "wb") as f:
            f.write(raw)
        return pd.read_parquet(local_file)

    def _list_silver_yearmonths(self, table: str) -> List[str]:
        yms: Set[str] = set()
        for p in self.dl.list_paths(self.cfg.fs_silver, table):
            if "/year_month=" in p:
                try:
                    part = p.split("/year_month=")[1].split("/")[0]
                    if part.isdigit() and len(part) == 6:
                        yms.add(part)
                except IndexError:
                    continue
        return sorted(yms)

    # ---------- IO helper (parquet único externo, ex.: gold/populacao/data.parquet) ----------
    def _read_single_parquet(self, fs_name: str, object_path: str) -> pd.DataFrame:
        raw = self.dl.download_bytes(fs_name, object_path)
        tmp = self.paths.silver_parquet("__tmp_external_read")
        Path(tmp).parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "wb") as f:
            f.write(raw)
        return pd.read_parquet(tmp)

    # ---------- IO helper (escrita GOLD) ----------
    def _write_gold_parquet(self, df: pd.DataFrame, table: str) -> None:
        local_file = self.paths.silver_parquet(f"__gold_{table}_full")
        df.to_parquet(local_file, index=False, engine="pyarrow", compression="snappy")
        dest = f"{table}/data.parquet"  # FULL único, sem particionar por mês
        self.dl.upload_file("gold", dest, str(local_file))

    # ---------- Execução ----------
    def run(self) -> None:
        # Apenas tabelas marcadas como GOLD
        targets = [t for t in list_tables() if make_table(t, "").__class__.layer == Layer.GOLD]
        if not targets:
            logger.warning("Nenhuma tabela GOLD registrada.")
            return

        logger.warning("Tabelas GOLD (full-load): %s", targets)

        # iterador que varre TODAS as partições do silver
        def read_silver(table_silver: str) -> Iterable[pd.DataFrame]:
            months = self._list_silver_yearmonths(table_silver)
            for ym in months:
                yield self._read_silver_parquet(table_silver, ym)

        # leitor de parquet único (ex.: gold/populacao/data.parquet)
        def read_external_parquet(fs_name: str, object_path: str) -> pd.DataFrame:
            return self._read_single_parquet(fs_name, object_path)

        cache: Dict[str, pd.DataFrame] = {}
        for name in targets:
            tbl = make_table(name, "")
            logger.warning("Construindo GOLD (FULL): %s", name)

            if hasattr(tbl, "build_curated_full"):
                # mantemos o nome do método para reaproveitar seu código
                df = tbl.build_curated_full(
                    read_silver=read_silver,
                    cache=cache,
                    read_external_parquet=read_external_parquet,
                )
            else:
                raise NotImplementedError(
                    f"Tabela GOLD '{name}' precisa implementar build_curated_full(read_silver, cache, read_external_parquet)"
                )

            cache[name] = df
            self._write_gold_parquet(df, name)
