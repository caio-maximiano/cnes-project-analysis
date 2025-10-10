from __future__ import annotations
import argparse
from typing import Iterable

from ..extract.extractor import Extractor


def _months_range(spec: str) -> Iterable[str]:
    """
    Converte "YYYYMM-YYYYMM" em sequência de YYYYMM (inclusive).
    Ex.: "202001-202003" -> ["202001","202002","202003"]
    """
    start, end = spec.split("-")
    ys, ms = int(start[:4]), int(start[4:])
    ye, me = int(end[:4]), int(end[4:])
    y, m = ys, ms
    out = []
    while (y < ye) or (y == ye and m <= me):
        out.append(f"{y}{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def run_one(ym: str, *, skip_upload: bool = False, keep_local: bool = False) -> None:
    e = Extractor(year_month=ym)
    e.download_zip()
    e.extract_zip()
    if not skip_upload:
        e.upload_to_datalake()
    if not keep_local:
        e.cleanup()


def main():
    p = argparse.ArgumentParser(description="Extractor CNES → ADLS (bronze)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--year_month", type=str, help="YYYYMM único")
    g.add_argument("--range", type=str, help="Intervalo YYYYMM-YYYYMM (inclusive), ex.: 202001-202312")
    g.add_argument("--all", action="store_true", help="2020..2025, todos os meses (cuidado)")
    p.add_argument("--skip-upload", action="store_true", help="Não envia para ADLS (somente download+extract)")
    p.add_argument("--keep-local", action="store_true", help="Não deleta ZIP/pasta local ao final")
    args = p.parse_args()

    if args.year_month:
        run_one(args.year_month, skip_upload=args.skip_upload, keep_local=args.keep_local)
        return

    if args.range:
        for ym in _months_range(args.range):
            run_one(ym, skip_upload=args.skip_upload, keep_local=args.keep_local)
        return

    if args.all:
        for y in range(2020, 2026):
            for m in range(1, 13):
                ym = f"{y}{m:02d}"
                run_one(ym, skip_upload=args.skip_upload, keep_local=args.keep_local)
        return

    # padrão: se nada for passado, usa (hoje - 3 meses) do próprio Extractor
    run_one(ym=None, skip_upload=args.skip_upload, keep_local=args.keep_local)  # type: ignore[arg-type]


if __name__ == "__main__":
    main()
