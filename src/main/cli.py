# src/main/cli.py
from __future__ import annotations

import argparse
import inspect
from typing import Any, Dict

from .data_domains.registry import list_jobs, get_job
from .extract.extractor import Extractor


# ------------ helpers ------------
def _build_kwargs_for(JobCls, args) -> Dict[str, Any]:
    """
    Monta kwargs dinamicamente com base na assinatura de __init__ do job.
    Suporta year_month e artifact_name (se existirem na assinatura).
    """
    sig = inspect.signature(JobCls.__init__)
    params = sig.parameters

    kwargs: Dict[str, Any] = {}

    if "year_month" in params and getattr(args, "year_month", None) is not None:
        kwargs["year_month"] = args.year_month

    if "artifact_name" in params and getattr(args, "artifact_name", None) is not None:
        kwargs["artifact_name"] = args.artifact_name

    return kwargs


# ------------ commands ------------
def cmd_list(_args):
    print("Jobs disponíveis:")
    for key in sorted(list_jobs().keys()):
        print(f"  - {key}")


def cmd_run(args):
    JobCls = get_job(args.job)
    kwargs = _build_kwargs_for(JobCls, args)

    print(f"→ Executando job `{args.job}` …")
    job = JobCls(**kwargs)
    job.run()
    print(f"✓ `{args.job}` concluído com sucesso.")


def cmd_run_all(args):
    jobs = list_jobs()
    if not jobs:
        print("Nenhum job registrado.")
        return

    for key, JobCls in jobs.items():
        kwargs = _build_kwargs_for(JobCls, args)
        print(f"→ Executando `{key}` …")
        job = JobCls(**kwargs)
        job.run()
        print(f"✓ `{key}` concluído\n")


def cmd_extract(args):
    ex = Extractor(year_month=args.year_month, months_back=args.months_back)
    print(f"→ Extraindo CNES para {ex.year_month} …")
    ex.download_zip()
    ex.extract_zip()
    ex.upload_to_datalake()
    ex.cleanup()
    print(f"✓ Bronze concluído para {ex.year_month}.")


# ------------ parser ------------
def build_parser():
    p = argparse.ArgumentParser(prog="main", description="Runner de jobs (tables e models)")
    sub = p.add_subparsers(dest="cmd", required=True)

    # main list
    p_list = sub.add_parser("list", help="Lista jobs registrados")
    p_list.set_defaults(func=cmd_list)

    # main run --job X [--year-month YYYYMM] [--artifact-name foo.joblib]
    p_run = sub.add_parser("run", help="Roda um job específico (tabela ou modelo)")
    p_run.add_argument("--job", required=True, help="Nome do job (ex.: cnes_estabelecimentos ou cnes_linear_regression)")
    p_run.add_argument("--year-month", help="Período YYYYMM (usado por tabelas/metrics que aceitam)")
    p_run.add_argument("--artifact-name", help="Nome do artefato (usado por modelos que aceitam)")
    p_run.set_defaults(func=cmd_run)

    # main run-all [--year-month YYYYMM] [--artifact-name foo.joblib]
    p_run_all = sub.add_parser("run-all", help="Roda todos os jobs do registry")
    p_run_all.add_argument("--year-month", help="Período YYYYMM (passado aos jobs que aceitam)")
    p_run_all.add_argument("--artifact-name", help="Artefato (passado aos modelos que aceitam)")
    p_run_all.set_defaults(func=cmd_run_all)

    # main extract [--year-month YYYYMM | --months-back N]
    p_extract = sub.add_parser("extract", help="Baixa ZIP, extrai CSVs e sobe para o bronze")
    p_extract.add_argument("--year-month", help="Período YYYYMM; se omitido, usa hoje - months-back")
    p_extract.add_argument("--months-back", type=int, default=3, help="Meses para trás quando --year-month não for passado (default: 3)")
    p_extract.set_defaults(func=cmd_extract)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
