from __future__ import annotations

import argparse
from .extractor import Extractor


def main():
    parser = argparse.ArgumentParser(description="Download, extract, and upload CNES data to ADLS (bronze)")
    parser.add_argument("--year_month", type=str, help="YYYYMM; se vazio, usa (hoje - 3 meses).")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Processa todos os meses de 2020..2025 (cuidado: operação longa).",
    )
    args = parser.parse_args()

    if args.all:
        years = [str(y) for y in range(2020, 2026)]
        months = [f"{m:02d}" for m in range(1, 13)]
        for y in years:
            for m in months:
                ym = f"{y}{m}"
                e = Extractor(year_month=ym)
                e.download_zip()
                e.extract_zip()
                e.upload_to_datalake()
                e.cleanup()
        return

    # Caso padrão: um único YYYYMM (ou 3 meses atrás)
    e = Extractor(year_month=args.year_month)
    e.download_zip()
    e.extract_zip()
    e.upload_to_datalake()
    e.cleanup()


if __name__ == "__main__":
    main()
