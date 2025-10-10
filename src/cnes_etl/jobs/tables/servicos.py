from __future__ import annotations
import pandas as pd
from cnes_etl.jobs.tables.base import BaseTable, register_table


@register_table
class ServicosTable(BaseTable):
    name = "servicos"
    dependencies: list[str] = []


    def build(self, bronze: dict[str, pd.DataFrame], cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
        tbe = bronze["tbEstabelecimento"].copy()
        tbm = bronze["tbMunicipio"].copy()
        tbe["CO_ESTADO_GESTOR"] = pd.to_numeric(tbe.get("CO_ESTADO_GESTOR"), errors="coerce")
        estab_sp = tbe[tbe["CO_ESTADO_GESTOR"] == 35]

        estab_munic = estab_sp.merge(
            tbm,
            left_on="CO_MUNICIPIO_GESTOR",
            right_on="CO_MUNICIPIO",
            how="inner",
            suffixes=("", "_mun"),
        )

        serv_join = (
            bronze["rlEstabServClass"]
            .merge(
                bronze["tbClassificacaoServico"],
                left_on=["CO_SERVICO", "CO_CLASSIFICACAO"],
                right_on=["CO_SERVICO_ESPECIALIZADO", "CO_CLASSIFICACAO_SERVICO"],
                how="inner",
            )
            .merge(estab_munic, on="CO_UNIDADE", how="inner")
        )

        df = serv_join[[
            "CO_UNIDADE",
            "NO_MUNICIPIO",
            "CO_MUNICIPIO",
            "CO_SERVICO",
            "CO_CLASSIFICACAO",
            "DS_CLASSIFICACAO_SERVICO",
            ]].copy()

        df["SK_REGISTRO"] = (
            df["CO_UNIDADE"].astype(str)
            + "_"
            + df["CO_SERVICO"].astype(str)
            + "_"
            + df["CO_CLASSIFICACAO"].astype(str)
        )

        df["YYYYMM"] = self.yyyymm
        
        return df.drop_duplicates(subset=["SK_REGISTRO"]).reset_index(drop=True)