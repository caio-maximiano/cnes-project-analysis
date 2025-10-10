from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from dateutil.relativedelta import relativedelta


@dataclass(frozen=True)
class AppConfig:
    account_name: str = "cnesstorageaccount"
    account_key: str = (
    "cP1htVg+Qtmzi+4dJKz0qEDb1c7uHu3f5VuDWK8/RV2FP/6Qa5GJzT7q2jcGLVvUfwpC3UaFbTEY+ASt38FW+A=="
    )
    fs_bronze: str = "bronze"
    fs_silver: str = "silver"


    base_url: str = "https://cnes.datasus.gov.br"
    download_path: str = "/EstatisticasServlet?path=BASE_DE_DADOS_CNES_{YYYYMM}.ZIP"


    local_zip_dir: str = "./local_storage/zip"
    local_csv_dir_prefix: str = "./local_storage/csv/cnes_extract_" # + YYYYMM
    local_curated_dir: str = "./local_storage/curated"


    @staticmethod
    def default_yyyymm(months_back: int = 3) -> str:
        return (datetime.today() - relativedelta(months=months_back)).strftime("%Y%m")