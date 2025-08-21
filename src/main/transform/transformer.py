import os
import io
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient


class Transformer:
    """Curates CNES tables and uploads them to Azure Data Lake."""

    def __init__(self, year_month: str | None = None, months_back: int = 4):
        if year_month:
            self.year_month = year_month
        else:
            self.year_month = (
                datetime.today() - relativedelta(months=months_back)
            ).strftime("%Y%m")

        self.account_name = "cnesstorage"
        self.account_key = "/ae47eZuE0NGPopxVHEkxOKsQwtEm3qQM0vBRPBRbB5nAW1zO6FPkEO9gwNQwkqExaVhOyHWgb68+AStIau+Uw=="#os.environ["AZURE_STORAGE_KEY"]

        self.datalake_client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=self.account_key,
        )
        self.bronze_fs_client = self.datalake_client.get_file_system_client("bronze")
        self.silver_fs_client = self.datalake_client.get_file_system_client("silver")

        self.datalake_target_path = f"/{self.year_month}"

    # ------------------------------------------------------------------
    def _current_date_str(self) -> str:
        return date.today().isoformat()

    # def _read_cnes_csv(self, name: str) -> pd.DataFrame:
    #     file_path = f"{self.year_month}/{name}{self.year_month}.csv"
    #     print(f"Reading {file_path} from Data Lake")
    #     file_client = self.bronze_fs_client.get_file_client(file_path)
    #     print("loaded")
    #     download = file_client.download_file()
    #     data = download.readall()
    #     return pd.read_csv(
    #         io.BytesIO(data), sep=";", quotechar="\"", dtype=str, low_memory=False
    #     )
    def _read_cnes_csv(self, name: str) -> pd.DataFrame:
        file_path = f"{self.year_month}/{name}{self.year_month}.csv"
        print(f"Reading bronze/{file_path} from Data Lake")
        file_client = self.bronze_fs_client.get_file_client(file_path)
        data = file_client.download_file().readall()

        # CNES: usually semicolon + latin-1 (cp1252). Use the python engine (more tolerant).
        try:
            return pd.read_csv(
                io.BytesIO(data),
                sep=";",
                quotechar='"',
                dtype=str,
                # low_memory=False,
                encoding="latin-1",
                engine="python",
                on_bad_lines="warn",
            )
        except UnicodeDecodeError:
            # Rarely some tables come as UTF-8-SIG or cp1252 – try a fallback.
            for enc in ("cp1252", "utf-8-sig"):
                try:
                    return pd.read_csv(
                        io.BytesIO(data),
                        sep=";",
                        quotechar='"',
                        dtype=str,
                        low_memory=False,
                        encoding=enc,
                        # engine="python",
                        on_bad_lines="warn",
                    )
                except UnicodeDecodeError:
                    continue
            raise  # rethrow if none worked


    # ------------------------------------------------------------------
    def _load_tables(self) -> dict[str, pd.DataFrame]:
        tables = {
            "tbEstabelecimento": self._read_cnes_csv("tbEstabelecimento"),
            "tbMunicipio": self._read_cnes_csv("tbMunicipio"),
            "rlEstabServClass": self._read_cnes_csv("rlEstabServClass"),
            "tbClassificacaoServico": self._read_cnes_csv("tbClassificacaoServico"),
            "tbCargaHorariaSus": self._read_cnes_csv("tbCargaHorariaSus"),
            "tbAtividadeProfissional": self._read_cnes_csv("tbAtividadeProfissional"),
            "tbDadosProfissionalSus": self._read_cnes_csv("tbDadosProfissionalSus"),
        }
        return tables

    # ------------------------------------------------------------------
    # def _estab_municipio(self, tbEstabelecimento: pd.DataFrame, tbMunicipio: pd.DataFrame) -> pd.DataFrame:
    #     tbEstabelecimento["CO_ESTADO_GESTOR"] = pd.to_numeric(
    #         tbEstabelecimento["CO_ESTADO_GESTOR"], errors="coerce"
    #     )
    #     estab_filtered = tbEstabelecimento[
    #         (tbEstabelecimento["CO_ESTADO_GESTOR"] == 35)#| (tbEstabelecimento["CO_SIGLA_ESTADO"] == "SP")
    #     ]
    #     estab_munic = estab_filtered.merge(
    #         tbMunicipio,
    #         left_on="CO_MUNICIPIO_GESTOR",
    #         right_on="CO_MUNICIPIO",
    #         how="inner",
    #         suffixes=("", "_mun"),
    #     )
    #     rename_map = {}
    #     for col in ["NO_MUNICIPIO", "CO_MUNICIPIO", "CO_SIGLA_ESTADO"]:
    #         right = f"{col}_mun"
    #         if right in estab_munic.columns:
    #             rename_map[right] = col
    #     estab_munic = estab_munic.rename(columns=rename_map)
    #     return estab_munic
    def _estab_municipio(self, tbEstabelecimento: pd.DataFrame, tbMunicipio: pd.DataFrame) -> pd.DataFrame:
        # coerce state code to numeric, then filter for 35 (São Paulo)
        tbEstabelecimento = tbEstabelecimento.copy()
        tbEstabelecimento["CO_ESTADO_GESTOR"] = pd.to_numeric(
            tbEstabelecimento["CO_ESTADO_GESTOR"], errors="coerce"
        )
        estab_sp = tbEstabelecimento[tbEstabelecimento["CO_ESTADO_GESTOR"] == 35]

        # inner join on municipio
        estab_munic = estab_sp.merge(
            tbMunicipio,
            left_on="CO_MUNICIPIO_GESTOR",
            right_on="CO_MUNICIPIO",
            how="inner",
            suffixes=("", "_mun"),
        )
        return estab_munic

    def _transform_servicos(self, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        estab_munic = self._estab_municipio(tables["tbEstabelecimento"], tables["tbMunicipio"])
        serv_join = (
            tables["rlEstabServClass"]
            .merge(
                tables["tbClassificacaoServico"],
                left_on=["CO_SERVICO", "CO_CLASSIFICACAO"],
                right_on=["CO_SERVICO_ESPECIALIZADO", "CO_CLASSIFICACAO_SERVICO"],
                how="inner",
            )
            .merge(estab_munic, on="CO_UNIDADE", how="inner")
        )
        curated_servicos = serv_join[
            [
                "CO_UNIDADE",
                "NO_MUNICIPIO",
                "CO_MUNICIPIO",
                "CO_SERVICO",
                "CO_CLASSIFICACAO",
                "DS_CLASSIFICACAO_SERVICO",
            ]
        ].copy()
        curated_servicos["SK_REGISTRO"] = (
            curated_servicos["CO_UNIDADE"].astype(str)
            + "_"
            + curated_servicos["CO_SERVICO"].astype(str)
            + "_"
            + curated_servicos["CO_CLASSIFICACAO"].astype(str)
        )
        curated_servicos["DATA_INGESTAO"] = self._current_date_str()
        curated_servicos = curated_servicos.drop_duplicates(subset=["SK_REGISTRO"])
        return curated_servicos

    def _transform_estabelecimentos(self, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        estab_munic = self._estab_municipio(tables["tbEstabelecimento"], tables["tbMunicipio"])
        joined = (
            tables["tbCargaHorariaSus"]
            .merge(tables["tbAtividadeProfissional"], on="CO_CBO", how="inner")
            .merge(estab_munic, on="CO_UNIDADE", how="inner")
            .merge(tables["tbDadosProfissionalSus"], on="CO_PROFISSIONAL_SUS", how="inner")
        )
        curated_estab = joined[
            [
                "CO_UNIDADE",
                "CO_PROFISSIONAL_SUS",
                "NO_PROFISSIONAL",
                "CO_CBO",
                "TP_SUS_NAO_SUS",
                "DS_ATIVIDADE_PROFISSIONAL",
                "NO_FANTASIA",
                "NO_BAIRRO",
                "NO_MUNICIPIO",
                "CO_MUNICIPIO",
                "CO_SIGLA_ESTADO",
                "CO_CEP",
            ]
        ].copy()
        curated_estab["ds_localidade"] = (
            curated_estab["CO_CEP"].astype(str)
            + ","
            + curated_estab["NO_MUNICIPIO"].astype(str)
            + ","
            + curated_estab["CO_SIGLA_ESTADO"].astype(str)
            + ",Brasil"
        )
        curated_estab["SK_REGISTRO"] = (
            curated_estab["CO_UNIDADE"].astype(str)
            + "_"
            + curated_estab["CO_PROFISSIONAL_SUS"].astype(str)
            + "_"
            + curated_estab["CO_CBO"].astype(str)
        )
        curated_estab["DATA_INGESTAO"] = self._current_date_str()
        curated_estab = curated_estab.drop_duplicates(subset=["SK_REGISTRO"])
        return curated_estab

    # ------------------------------------------------------------------
    def _write_to_datalake(self, df: pd.DataFrame, name: str):
        local_dir = Path("./local_storage/curated")
        local_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_dir / f"{name}_{self.year_month}.csv"
        df.to_csv(local_file, index=False)

        dest_path = f"{self.datalake_target_path}/{name}_{self.year_month}.csv"
        file_client = self.silver_fs_client.get_file_client(dest_path)
        with open(local_file, "rb") as data:
            file_client.upload_data(data, overwrite=True)

    # ------------------------------------------------------------------
    def run(self):
        print("starting")
        tables = self._load_tables()
        print("loaded")
        servicos = self._transform_servicos(tables)
        estabelecimentos = self._transform_estabelecimentos(tables)
        self._write_to_datalake(servicos, "servicos")
        self._write_to_datalake(estabelecimentos, "estabelecimentos")


if __name__ == "__main__":
    Transformer().run()