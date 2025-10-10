from __future__ import annotations
import pandas as pd
from cnes_etl.jobs.tables.base import BaseTable, register_table


@register_table
class EstabelecimentosTable(BaseTable):
    name = "estabelecimentos"
    dependencies: list[str] = [] # poderia depender de "servicos" se fosse reusar algo


    def build(self, bronze: dict[str, pd.DataFrame], cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
        tbe = bronze["tbEstabelecimento"].copy()
        tbm = bronze["tbMunicipio"].copy()
        tchs = bronze["tbCargaHorariaSus"].copy()
        tap = bronze["tbAtividadeProfissional"].copy()
        tdps = bronze["tbDadosProfissionalSus"].copy()

        tbe["CO_ESTADO_GESTOR"] = pd.to_numeric(tbe.get("CO_ESTADO_GESTOR"), errors="coerce")
        estab_sp = tbe[tbe["CO_ESTADO_GESTOR"] == 35]

        estab_munic = estab_sp.merge(
            tbm,
            left_on="CO_MUNICIPIO_GESTOR",
            right_on="CO_MUNICIPIO",
            how="inner",
            suffixes=("", "_mun"),
        )

        joined = (
            tchs.merge(tap, on="CO_CBO", how="inner")
            .merge(estab_munic, on="CO_UNIDADE", how="inner")
            .merge(tdps, on="CO_PROFISSIONAL_SUS", how="inner")
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

        df = joined[cols].astype(str)

        df["ds_localidade"] = (
            df["CO_CEP"] + "," + df["NO_MUNICIPIO"] + "," + df["CO_SIGLA_ESTADO"] + ",Brasil"
        )

        df["SK_REGISTRO"] = (
            df["CO_UNIDADE"] + "_" + df["CO_PROFISSIONAL_SUS"] + "_" + df["CO_CBO"]
        )

        df["YYYYMM"] = self.yyyymm
        
        return df.drop_duplicates(subset=["SK_REGISTRO"]).reset_index(drop=True)