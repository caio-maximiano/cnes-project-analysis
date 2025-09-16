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
        # Define o período alvo (YYYYMM)
        if year_month:
            self.year_month = year_month
        else:
            self.year_month = (
                datetime.today() - relativedelta(months=months_back)
            ).strftime("%Y%m")

        # ---- Configuração do Data Lake (use variável de ambiente para a chave) ----
        self.account_name = "cnesstorage"
        # export AZURE_STORAGE_KEY='...'
        self.account_key = "/ae47eZuE0NGPopxVHEkxOKsQwtEm3qQM0vBRPBRbB5nAW1zO6FPkEO9gwNQwkqExaVhOyHWgb68+AStIau+Uw=="  # os.environ.get("AZURE_STORAGE_KEY")
        if not self.account_key:
            raise RuntimeError(
                "AZURE_STORAGE_KEY não definida no ambiente. "
                "Execute: export AZURE_STORAGE_KEY='sua_chave_azure'"
            )

        self.datalake_client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=self.account_key,
        )
        self.bronze_fs_client = self.datalake_client.get_file_system_client("bronze")
        self.silver_fs_client = self.datalake_client.get_file_system_client("silver")

        # Diretório local para staging (debug)
        self.local_dir = Path("./local_storage/curated")
        self.local_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    def _current_date_str(self) -> str:
        return date.today().isoformat()

    # ------------------------------------------------------------------
    # Lê CSV do Bronze (tipicamente ; + latin-1). Mantemos a leitura tolerante.
    def _read_cnes_csv(self, name: str) -> pd.DataFrame:
        file_path = f"{self.year_month}/{name}{self.year_month}.csv"
        print(f"Reading bronze/{file_path} from Data Lake")
        file_client = self.bronze_fs_client.get_file_client(file_path)
        data = file_client.download_file().readall()

        try:
            return pd.read_csv(
                io.BytesIO(data),
                sep=";",
                quotechar='"',
                dtype=str,
                encoding="latin-1",
                engine="python",
                on_bad_lines="warn",
            )
        except UnicodeDecodeError:
            for enc in ("cp1252", "utf-8-sig"):
                try:
                    return pd.read_csv(
                        io.BytesIO(data),
                        sep=";",
                        quotechar='"',
                        dtype=str,
                        encoding=enc,
                        engine="python",
                        on_bad_lines="warn",
                    )
                except UnicodeDecodeError:
                    continue
            raise  # rethrow se nada funcionar

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
    def _estab_municipio(
        self, tbEstabelecimento: pd.DataFrame, tbMunicipio: pd.DataFrame
    ) -> pd.DataFrame:
        # Coerce state code para numérico e filtra SP (35)
        tbEstabelecimento = tbEstabelecimento.copy()
        tbEstabelecimento["CO_ESTADO_GESTOR"] = pd.to_numeric(
            tbEstabelecimento.get("CO_ESTADO_GESTOR"), errors="coerce"
        )
        estab_sp = tbEstabelecimento[tbEstabelecimento["CO_ESTADO_GESTOR"] == 35]

        # Join de município
        estab_munic = estab_sp.merge(
            tbMunicipio,
            left_on="CO_MUNICIPIO_GESTOR",
            right_on="CO_MUNICIPIO",
            how="inner",
            suffixes=("", "_mun"),
        )
        return estab_munic

    # ------------------------------------------------------------------
    def _transform_servicos(self, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        estab_munic = self._estab_municipio(
            tables["tbEstabelecimento"], tables["tbMunicipio"]
        )
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
        curated_servicos["YYYYMM"] = self.year_month
        curated_servicos = curated_servicos.drop_duplicates(subset=["SK_REGISTRO"])

        # Opcional: garantir UTF-8 nos textos antes do Parquet (normalmente já está ok)
        for col in ["NO_MUNICIPIO", "DS_CLASSIFICACAO_SERVICO"]:
            if col in curated_servicos.columns:
                curated_servicos[col] = curated_servicos[col].astype(str)

        return curated_servicos

    # ------------------------------------------------------------------
    def _transform_estabelecimentos(self, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
        estab_munic = self._estab_municipio(
            tables["tbEstabelecimento"], tables["tbMunicipio"]
        )
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

        # Campos string normalizados (evita perder zeros à esquerda)
        string_cols = [
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
        for c in string_cols:
            if c in curated_estab.columns:
                curated_estab[c] = curated_estab[c].astype(str)

        # Localidade composta (CEP, Município, UF, Brasil)
        curated_estab["ds_localidade"] = (
            curated_estab["CO_CEP"].astype(str)
            + ","
            + curated_estab["NO_MUNICIPIO"].astype(str)
            + ","
            + curated_estab["CO_SIGLA_ESTADO"].astype(str)
            + ",Brasil"
        )

        # Chave técnica + data de ingestão
        curated_estab["SK_REGISTRO"] = (
            curated_estab["CO_UNIDADE"].astype(str)
            + "_"
            + curated_estab["CO_PROFISSIONAL_SUS"].astype(str)
            + "_"
            + curated_estab["CO_CBO"].astype(str)
        )
        curated_estab["DATA_INGESTAO"] = self._current_date_str()
        curated_estab["YYYYMM"] = self.year_month
        curated_estab = curated_estab.drop_duplicates(subset=["SK_REGISTRO"])

        return curated_estab

    # ------------------------------------------------------------------
    def _write_parquet_to_silver(self, df: pd.DataFrame, table_name: str):
        """
        Escreve em Parquet (snappy) no Silver com layout:
        silver/<table_name>/year_month=<YYYYMM>/data.parquet
        """
        # Staging local (útil para debug e para upload)
        local_file = self.local_dir / f"{table_name}_{self.year_month}.parquet"
        df.to_parquet(local_file, index=False, engine="pyarrow", compression="snappy")

        dest_path = f"{table_name}/year_month={self.year_month}/data.parquet"  # sem barra inicial
        file_client = self.silver_fs_client.get_file_client(dest_path)
        with open(local_file, "rb") as data:
            file_client.upload_data(data, overwrite=True)

    # ------------------------------------------------------------------
    # (Opcional) manter escrita CSV para troubleshooting rápido
    # def _write_csv_to_silver(self, df: pd.DataFrame, table_name: str):
    #     """
    #     Escreve CSV UTF-8 no Silver (apenas se precisar compatibilidade temporária).
    #     silver/<table_name>/year_month=<YYYYMM>/data.csv
    #     """
    #     local_file = self.local_dir / f"{table_name}_{self.year_month}.csv"
    #     df.to_csv(local_file, index=False, encoding="utf-8")

    #     dest_path = f"{table_name}/year_month={self.year_month}/data.csv"
    #     file_client = self.silver_fs_client.get_file_client(dest_path)
    #     with open(local_file, "rb") as data:
    #         file_client.upload_data(data, overwrite=True)

    # ------------------------------------------------------------------
    def run(self):
        print(f"Iniciando curadoria para {self.year_month}…")
        tables = self._load_tables()
        print("Tabelas carregadas do Bronze.")

        print("Transformando serviços…")
        servicos = self._transform_servicos(tables)

        print("Transformando estabelecimentos…")
        estabelecimentos = self._transform_estabelecimentos(tables)

        print("Gravando no Silver (Parquet particionado)…")
        self._write_parquet_to_silver(servicos, "servicos")
        self._write_parquet_to_silver(estabelecimentos, "estabelecimentos")

        print("Concluído!")

        # Se quiser manter também um CSV para inspeção pontual, descomente:
        # self._write_csv_to_silver(servicos, "servicos")
        # self._write_csv_to_silver(estabelecimentos, "estabelecimentos")


if __name__ == "__main__":
    Transformer().run()
