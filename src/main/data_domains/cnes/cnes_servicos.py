from datetime import date
import pandas as pd
from src.main.core.layers.silver import Silver

class CnesServicos(Silver):
    job_type = "table"
    def __init__(self, year_month: str):
        super().__init__(name="cnes_servicos")
        self.year_month = year_month
        ym = self.year_month
        self.inputs = {
            "tbEstabelecimento":      self.read_csv_from_bronze(f"{ym}/tbEstabelecimento{ym}.csv"),
            "tbMunicipio":            self.read_csv_from_bronze(f"{ym}/tbMunicipio{ym}.csv"),
            "rlEstabServClass":       self.read_csv_from_bronze(f"{ym}/rlEstabServClass{ym}.csv"),
            "tbClassificacaoServico": self.read_csv_from_bronze(f"{ym}/tbClassificacaoServico{ym}.csv"),
        }

    def definition(self) -> pd.DataFrame:
        tbEstabelecimento = self.inputs["tbEstabelecimento"].copy()
        tbMunicipio = self.inputs["tbMunicipio"].copy()
        rlEstabServClass = self.inputs["rlEstabServClass"].copy()
        tbClassificacaoServico = self.inputs["tbClassificacaoServico"].copy()

        tbEstabelecimento["CO_ESTADO_GESTOR"] = pd.to_numeric(
            tbEstabelecimento.get("CO_ESTADO_GESTOR"), errors="coerce"
        )
        estab_sp = tbEstabelecimento[tbEstabelecimento["CO_ESTADO_GESTOR"] == 35]

        estab_munic = estab_sp.merge(
            tbMunicipio,
            left_on="CO_MUNICIPIO_GESTOR",
            right_on="CO_MUNICIPIO",
            how="inner",
            suffixes=("", "_mun"),
        )

        serv_join = (
            rlEstabServClass
            .merge(
                tbClassificacaoServico,
                left_on=["CO_SERVICO", "CO_CLASSIFICACAO"],
                right_on=["CO_SERVICO_ESPECIALIZADO", "CO_CLASSIFICACAO_SERVICO"],
                how="inner",
            )
            .merge(estab_munic, on="CO_UNIDADE", how="inner")
        )

        servicos = serv_join[
            ["CO_UNIDADE","NO_MUNICIPIO","CO_MUNICIPIO","CO_SERVICO","CO_CLASSIFICACAO","DS_CLASSIFICACAO_SERVICO"]
        ].copy()

        today_str = date.today().isoformat()
        ym = self.year_month
        servicos["SK_REGISTRO"] = (
            servicos["CO_UNIDADE"].astype(str) + "_"
            + servicos["CO_SERVICO"].astype(str) + "_"
            + servicos["CO_CLASSIFICACAO"].astype(str)
        )
        servicos["DATA_INGESTAO"] = today_str
        servicos["YYYYMM"] = ym
        servicos = servicos.drop_duplicates(subset=["SK_REGISTRO"])
        for col in ["NO_MUNICIPIO", "DS_CLASSIFICACAO_SERVICO"]:
            if col in servicos.columns:
                servicos[col] = servicos[col].astype(str)
        return servicos
