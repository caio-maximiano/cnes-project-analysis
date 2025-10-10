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
    
    #i did this for a full load in the past
    list_year = ["2020","2021", "2022", "2023", "2024", "2025"]
    list_month = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    if not args.year_month:
        for year in list_year:
            for month in list_month:
                extractor = Extractor(year_month=f"{year}{month}")
                extractor.download_zip()
                extractor.extract_zip()
                extractor.upload_to_datalake()
        extractor.cleanup()
    else:
        extractor = Extractor(year_month=args.year_month)
        extractor.download_zip()
        extractor.extract_zip()
        extractor.upload_to_datalake()
        extractor.cleanup()

    # extractor = Extractor(year_month=args.year_month)
    # extractor.download_zip()
    # extractor.extract_zip()
    # extractor.upload_to_datalake()
    # extractor.cleanup()

if __name__ == "__main__":
    main()
