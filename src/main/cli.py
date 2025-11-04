# src/main/cli.py
import argparse
from .domains.registry import list_tables, get_table
from .extract.extractor import Extractor  # 👈 novo import

def cmd_list(_args):
    print("Tabelas disponíveis:")
    for key in sorted(list_tables().keys()):
        print(f"  - {key}")

def cmd_run(args):
    TableCls = get_table(args.table)
    t = TableCls(year_month=args.year_month)
    print(f"→ Rodando tabela `{args.table}` para {args.year_month} …")
    t.run()
    print(f"✓ Tabela `{args.table}` para {args.year_month} concluída.")

def cmd_run_all(args):
    for key, TableCls in list_tables().items():
        print(f"→ Rodando {key} para {args.year_month} …")
        t = TableCls(year_month=args.year_month)
        t.run()
        print(f"✓ {key} ok\n")

def cmd_extract(args):
    ex = Extractor(year_month=args.year_month, months_back=args.months_back)
    print(f"→ Extraindo CNES para {ex.year_month} …")
    ex.download_zip()
    ex.extract_zip()
    ex.upload_to_datalake()
    ex.cleanup()
    print(f"✓ Bronze concluído para {ex.year_month}.")

def build_parser():
    p = argparse.ArgumentParser(prog="main", description="Runner de tabelas")
    sub = p.add_subparsers(dest="cmd", required=True)

    # main list
    p_list = sub.add_parser("list", help="Lista tabelas registradas")
    p_list.set_defaults(func=cmd_list)

    # main run --table X --year-month YYYYMM
    p_run = sub.add_parser("run", help="Roda uma tabela específica")
    p_run.add_argument("--table", required=True, help="Nome da tabela (ex.: cnes_servicos)")
    p_run.add_argument("--year-month", required=True, help="Período YYYYMM")
    p_run.set_defaults(func=cmd_run)

    # main run-all --year-month YYYYMM
    p_run_all = sub.add_parser("run-all", help="Roda todas as tabelas do registry")
    p_run_all.add_argument("--year-month", required=True, help="Período YYYYMM")
    p_run_all.set_defaults(func=cmd_run_all)

    #  main extract [--year-month YYYYMM | --months-back N]
    p_extract = sub.add_parser("extract", help="Baixa ZIP, extrai CSVs e sobe para o bronze")
    p_extract.add_argument("--year-month", help="Período YYYYMM; se omitido, usa hoje - months-back")
    p_extract.add_argument("--months-back", type=int, default=3, help="Meses para trás quando --year-month não for passado (default: 3)")
    p_extract.set_defaults(func=cmd_extract)

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
