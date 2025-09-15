import argparse
from transformer import Transformer


def main():
    parser = argparse.ArgumentParser(description="Curate CNES tables and upload to Azure Data Lake")
    parser.add_argument(
        "--year_month",
        type=str,
        help="Optional year and month in YYYYMM format. If not provided, defaults to 3 months ago.",
    )
    args = parser.parse_args()

    transformer = Transformer(year_month=args.year_month)
    transformer.run()


if __name__ == "__main__":
    main()