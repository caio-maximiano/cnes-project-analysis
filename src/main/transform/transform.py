from __future__ import annotations

import argparse
from .transformer import Transformer


def main():
    parser = argparse.ArgumentParser(description="Curate CNES tables and upload to ADLS (silver)")
    parser.add_argument("--year_month", type=str, help="YYYYMM; se vazio, usa (hoje - 4 meses).")
    args = parser.parse_args()

    Transformer(year_month=args.year_month).run()


if __name__ == "__main__":
    main()
