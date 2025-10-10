from __future__ import annotations
import logging
from pathlib import Path
# from tabnanny import verbose  # <- desnecessário, pode remover
import typer

from cnes_etl.jobs.gold import GoldJob
from cnes_etl.jobs.tables import make_table, list_tables
from cnes_etl.jobs.tables.base import Layer
from cnes_etl.config import AppConfig
from cnes_etl.logging import setup_logging
from cnes_etl.common.paths import CnesPaths
from cnes_etl.common.storage import DataLake
from cnes_etl.jobs.extract import ExtractJob
from cnes_etl.jobs.transform import TransformJob

app = typer.Typer(add_completion=False, no_args_is_help=True)

@app.callback()
def _main(verbose: bool = typer.Option(False, "--verbose", help="Enable debug logs")):
    setup_logging(logging.INFO if verbose else logging.WARNING)

def _mk_ctx(yyyymm: str | None, cfg: AppConfig) -> tuple[str, DataLake, CnesPaths]:
    ym = yyyymm or cfg.default_yyyymm()
    dl = DataLake(cfg.account_name, cfg.account_key)
    paths = CnesPaths(ym, Path(cfg.local_zip_dir), Path(cfg.local_csv_dir_prefix), Path(cfg.local_curated_dir))
    return ym, dl, paths

@app.command()
def extract(year_month: str | None = typer.Option(None, "--year-month", help="YYYYMM")):
    cfg = AppConfig()
    ym, dl, paths = _mk_ctx(year_month, cfg)
    ExtractJob(ym, cfg, dl, paths).execute()

@app.command()
def transform(year_month: str | None = typer.Option(None, "--year-month", help="YYYYMM")):
    cfg = AppConfig()
    ym, dl, paths = _mk_ctx(year_month, cfg)
    TransformJob(ym, cfg, dl, paths).execute()

@app.command()
def gold():
    """Gera TODAS as tabelas GOLD em FULL-LOAD (escreve em 'gold/<tabela>/data.parquet')."""
    cfg = AppConfig()
    ym, dl, paths = _mk_ctx(None, cfg)
    GoldJob(cfg, dl, paths).execute()

@app.command("gold-table")
def gold_table(name: str = typer.Argument(..., help="Nome da tabela GOLD, ex.: 'estabelecimentos_metricas_sp'")):
    """
    Executa UMA tabela GOLD específica (full-load) sem rodar as demais.
    Exemplo:
        cnes-etl gold-table estabelecimentos_metricas_sp
    """
    cfg = AppConfig()
    ym, dl, paths = _mk_ctx(None, cfg)
    job = GoldJob(cfg, dl, paths)

    # sanity check: precisa existir e ser GOLD
    if name not in list_tables():
        raise typer.BadParameter(f"Tabela '{name}' não encontrada. Disponíveis: {list_tables()}")

    if make_table(name, "").__class__.layer != Layer.GOLD:
        raise typer.BadParameter(f"Tabela '{name}' não é GOLD.")

    # readers finos reaproveitando os helpers do job
    def read_silver(table_silver: str):
        for ym_ in job._list_silver_yearmonths(table_silver):
            yield job._read_silver_parquet(table_silver, ym_)

    def read_external_parquet(fs_name: str, object_path: str):
        return job._read_single_parquet(fs_name, object_path)

    tbl = make_table(name, "")
    logging.warning("Executando GOLD (FULL) apenas: %s", name)
    df = tbl.build_curated_full(
        read_silver=read_silver,
        cache={},
        read_external_parquet=read_external_parquet,
    )
    job._write_gold_parquet(df, name)
    logging.warning("✅ Escrito em gold/%s/data.parquet", name)

@app.command("run-all")
def run_all(
    year_month: str | None = typer.Option(None, "--year-month", help="YYYYMM"),
    from_year: int | None = typer.Option(None, help="Full load start year, e.g., 2020"),
    to_year: int | None = typer.Option(None, help="Full load end year, inclusive, e.g., 2025"),
):
    cfg = AppConfig()
    if year_month:
        ym, dl, paths = _mk_ctx(year_month, cfg)
        ExtractJob(ym, cfg, dl, paths).execute()
        TransformJob(ym, cfg, dl, paths).execute()
        return

    if from_year and to_year:
        for y in range(from_year, to_year + 1):
            for m in range(1, 12 + 1):
                ym = f"{y}{m:02d}"
                ym_, dl, paths = _mk_ctx(ym, cfg)
                ExtractJob(ym_, cfg, dl, paths).execute()
                TransformJob(ym_, cfg, dl, paths).execute()
    else:
        ym, dl, paths = _mk_ctx(None, cfg)
        ExtractJob(ym, cfg, dl, paths).execute()
        TransformJob(ym, cfg, dl, paths).execute()

if __name__ == "__main__":  # pragma: no cover
    app()
