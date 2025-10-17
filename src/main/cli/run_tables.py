from __future__ import annotations
import argparse
from tables import list_tables, get_table, TableContext  # importa e registra

def main():
    p = argparse.ArgumentParser(description="Runner de tabelas CNES (singleton registry).")
    p.add_argument("--year_month", type=str, help="YYYYMM (default: hoje - 3 meses)")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--list", action="store_true", help="Lista tabelas registradas e sai.")
    group.add_argument("--tables", type=str, help="Lista de tabelas separadas por vírgula para rodar.")
    p.add_argument("--dry", action="store_true", help="Só executa definition (não escreve).")
    args = p.parse_args()

    if args.list:
        for t in list_tables():
            print(t)
        return

    ctx = TableContext.for_month(args.year_month)

    # quais rodar?
    selected = list_tables() if not args.tables else [t.strip() for t in args.tables.split(",")]

    for name in selected:
        table = get_table(name)
        print(f"🚀 {name} — target={ctx.year_month}")
        if args.dry:
            df_or_dict = table.definition(ctx)
            shape = (
                {k: v.shape for k, v in df_or_dict.items()}
                if isinstance(df_or_dict, dict) else getattr(df_or_dict, "shape", None)
            )
            print(f"   shapes: {shape}")
        else:
            table.run(ctx)
            print("   ✅ done")

if __name__ == "__main__":
    main()
