from __future__ import annotations

import io
from pathlib import Path
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

import pandas as pd

from core.storage import Storage


class Transformer:
    """
    Curadoria de tabelas CNES (foco: SP) e escrita em Parquet particionado no Silver.
    Lê CSVs do Bronze em: bronze/<YYYYMM>/<tbXxxYYYYMM>.csv
    Escreve Parquet em: silver/<tabela>/year_month=<YYYYMM>/data.parquet
    """

    def __init__(self, year_month: str | None = None, months_back: int = 4):
        if year_month:
            self.year_month = year_month
        else:
            self.year_month = (datetime.today() - relativedelta(months=months_back)).strftime("%Y%m")

        self.bronze = Storage(file_system="bronze")
        self.silver = Storage(file_system="silver")

        self.local_dir = Path("./local_storage/curated")
        self.local_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------
    def _today(self) -> str:
        return date.today().isoformat()

    # ------------------------------
    def _read_cnes_csv(self, base: str) -> pd.DataFrame:
        # bronze/<YYYYMM>/<base><YYYYMM>.csv
        remote = f"{self.year_month}/{base}{self.year_month}.csv"
        print(f"📥 Reading bronze/{remote}")
        raw = self.bronze.download_file(remote)

        # Leitura tolerante a encoding
        for enc in ("latin-1", "cp1252", "utf-8-sig"):
            try:
                return pd.read_csv(
                    io.BytesIO(raw),
                    sep=";",
                    quotechar='"',
                    dtype=str,
                    encoding=enc,
                    engine="python",
                    on_bad_lines="warn",
                )
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError("csv-decode", b"", 0, 1, "Falha ao decodificar com encodings comuns")

    # ------------------------------
    def _load_tables(self) -> dict[str, pd.DataFrame]:
        return {
            "tbEstabelecimento": self._read_cnes_csv("tbEstabelecimento"),
            "tbMunicipio": self._read_cnes_csv("tbMunicipio"),
            "rlEstabServClass": self._read_cnes_csv("rlEstabServClass"),
            "tbClassificacaoServico": self._read_cnes_csv("tbClassificacaoServico"),
            "tbCargaHorariaSus": self._read_cnes_csv("tbCargaHorariaSus"),
            "tbAtividadeProfissional": self._read_cnes_csv("tbAtividadeProfissional"),
            "tbDadosProfissionalSus": self._read_cnes_csv("tbDadosProfissionalSus"),
        }

    # ------------------------------
    def _estab_municipio(self, tbEstab: pd.DataFrame, tbMun: pd.DataFrame) -> pd.DataFrame:
        t = tbEstab.copy()
        t["CO_ESTADO_GESTOR"] = pd.to_numeric(t.get("CO_ESTADO_GESTOR"), errors="coerce")
        sp = t[t["CO_ESTADO_GESTOR"] == 35]
        return sp.merge(
            tbMun,
            left_on="CO_MUNICIPIO_GESTOR",
            right_on="CO_MUNICIPIO",
            how="inner",
            suffixes=("", "_mun"),
        )

    # ------------------------------
    def _transform_servicos(self, tbl: dict[str, pd.DataFrame]) -> pd.DataFrame:
        estab_munic = self._estab_municipio(tbl["tbEstabelecimento"], tbl["tbMunicipio"])
        serv_join = (
            tbl["rlEstabServClass"]
            .merge(
                tbl["tbClassificacaoServico"],
                left_on=["CO_SERVICO", "CO_CLASSIFICACAO"],
                right_on=["CO_SERVICO_ESPECIALIZADO", "CO_CLASSIFICACAO_SERVICO"],
                how="inner",
            )
            .merge(estab_munic, on="CO_UNIDADE", how="inner")
        )

        cols = [
            "CO_UNIDADE",
            "NO_MUNICIPIO",
            "CO_MUNICIPIO",
            "CO_SERVICO",
            "CO_CLASSIFICACAO",
            "DS_CLASSIFICACAO_SERVICO",
        ]
        df = serv_join[cols].copy()

        df["SK_REGISTRO"] = (
            df["CO_UNIDADE"].astype(str)
            + "_"
            + df["CO_SERVICO"].astype(str)
            + "_"
            + df["CO_CLASSIFICACAO"].astype(str)
        )
        df["DATA_INGESTAO"] = self._today()
        df["YYYYMM"] = self.year_month
        df = df.drop_duplicates(subset=["SK_REGISTRO"])

        for c in ("NO_MUNICIPIO", "DS_CLASSIFICACAO_SERVICO"):
            if c in df.columns:
                df[c] = df[c].astype(str)

        return df

    # ------------------------------
    def _transform_estabelecimentos(self, tbl: dict[str, pd.DataFrame]) -> pd.DataFrame:
        estab_munic = self._estab_municipio(tbl["tbEstabelecimento"], tbl["tbMunicipio"])
        joined = (
            tbl["tbCargaHorariaSus"]
            .merge(tbl["tbAtividadeProfissional"], on="CO_CBO", how="inner")
            .merge(estab_munic, on="CO_UNIDADE", how="inner")
            .merge(tbl["tbDadosProfissionalSus"], on="CO_PROFISSIONAL_SUS", how="inner")
        )

        cols = [
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
        df = joined[cols].copy()

        for c in cols:
            if c in df.columns:
                df[c] = df[c].astype(str)

        df["ds_localidade"] = (
            df["CO_CEP"].astype(str)
            + ","
            + df["NO_MUNICIPIO"].astype(str)
            + ","
            + df["CO_SIGLA_ESTADO"].astype(str)
            + ",Brasil"
        )

        df["SK_REGISTRO"] = (
            df["CO_UNIDADE"].astype(str)
            + "_"
            + df["CO_PROFISSIONAL_SUS"].astype(str)
            + "_"
            + df["CO_CBO"].astype(str)
        )
        df["DATA_INGESTAO"] = self._today()
        df["YYYYMM"] = self.year_month
        df = df.drop_duplicates(subset=["SK_REGISTRO"])

        return df

    # ------------------------------
    def _write_parquet_to_silver(self, df: pd.DataFrame, table_name: str) -> None:
        # Local para debug/log
        local_file = self.local_dir / f"{table_name}_{self.year_month}.parquet"
        df.to_parquet(local_file, index=False, engine="pyarrow", compression="snappy")

        remote = f"{table_name}/year_month={self.year_month}/data.parquet"
        self.silver.upload_file(local_file, remote)

    # ------------------------------
    def run(self) -> None:
        print(f"Iniciando curadoria para {self.year_month}…")
        tables = self._load_tables()
        print("✅ Tabelas lidas do Bronze")

        servicos = self._transform_servicos(tables)
        estabelecimentos = self._transform_estabelecimentos(tables)

        print("💾 Gravando no Silver (Parquet particionado)…")
        self._write_parquet_to_silver(servicos, "servicos")
        self._write_parquet_to_silver(estabelecimentos, "estabelecimentos")

        print("Concluído!")
