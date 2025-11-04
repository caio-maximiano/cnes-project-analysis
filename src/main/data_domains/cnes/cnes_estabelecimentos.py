from datetime import date
import pandas as pd
from main.core.layers.silver import Silver  # ajuste o import conforme seu projeto

class CnesEstabelecimentos(Silver):
    def __init__(self, year_month: str):
        super().__init__(name="cnes_estabelecimentos")
        self.year_month = year_month

        ym = self.year_month
        # inputs necessários para ESTABELECIMENTOS (padrão: nome{YYYYMM}.csv)
        self.inputs = {
            "tbEstabelecimento":      self.read_csv_from_bronze(f"{ym}/tbEstabelecimento{ym}.csv"),
            "tbMunicipio":            self.read_csv_from_bronze(f"{ym}/tbMunicipio{ym}.csv"),
            "tbCargaHorariaSus":      self.read_csv_from_bronze(f"{ym}/tbCargaHorariaSus{ym}.csv"),
            "tbAtividadeProfissional":self.read_csv_from_bronze(f"{ym}/tbAtividadeProfissional{ym}.csv"),
            "tbDadosProfissionalSus": self.read_csv_from_bronze(f"{ym}/tbDadosProfissionalSus{ym}.csv"),
        }

    def definition(self) -> pd.DataFrame:
        tbEstabelecimento      = self.inputs["tbEstabelecimento"].copy()
        tbMunicipio            = self.inputs["tbMunicipio"].copy()
        tbCargaHorariaSus      = self.inputs["tbCargaHorariaSus"].copy()
        tbAtividadeProfissional= self.inputs["tbAtividadeProfissional"].copy()
        tbDadosProfissionalSus = self.inputs["tbDadosProfissionalSus"].copy()

        # ---- estab + município (SP = 35)
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

        # ---- joins para estabelecimentos
        joined = (
            tbCargaHorariaSus
            .merge(tbAtividadeProfissional, on="CO_CBO", how="inner")
            .merge(estab_munic, on="CO_UNIDADE", how="inner")
            .merge(tbDadosProfissionalSus, on="CO_PROFISSIONAL_SUS", how="inner")
        )

        # ---- seleção e normalização
        cols = [
            "CO_UNIDADE","CO_PROFISSIONAL_SUS","NO_PROFISSIONAL","CO_CBO",
            "TP_SUS_NAO_SUS","DS_ATIVIDADE_PROFISSIONAL","NO_FANTASIA","NO_BAIRRO",
            "NO_MUNICIPIO","CO_MUNICIPIO","CO_SIGLA_ESTADO","CO_CEP",
        ]
        curated = joined[cols].copy()

        # força string (evita perder zeros à esquerda)
        for c in cols:
            if c in curated.columns:
                curated[c] = curated[c].astype(str)

        # campo de localidade
        curated["ds_localidade"] = (
            curated["CO_CEP"] + "," + curated["NO_MUNICIPIO"] + "," + curated["CO_SIGLA_ESTADO"] + ",Brasil"
        )

        # metadados e chave técnica
        today_str = date.today().isoformat()
        ym = self.year_month
        curated["SK_REGISTRO"] = (
            curated["CO_UNIDADE"] + "_" + curated["CO_PROFISSIONAL_SUS"] + "_" + curated["CO_CBO"]
        )
        curated["DATA_INGESTAO"] = today_str
        curated["YYYYMM"] = ym
        curated = curated.drop_duplicates(subset=["SK_REGISTRO"])

        return curated
