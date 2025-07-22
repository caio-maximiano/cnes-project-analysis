import argparse
from extractor import Extractor

def main():
    parser = argparse.ArgumentParser(description="Download, extract, and upload CNES data")
    parser.add_argument(
        "--year_month",
        type=str,
        help="Optional year and month in YYYYMM format. If not provided, defaults to 3 months ago."
    )

    args = parser.parse_args()

    extractor = Extractor(year_month=args.year_month)
    extractor.download_zip()
    extractor.extract_zip()
    extractor.upload_to_datalake()

if __name__ == "__main__":
    main()
