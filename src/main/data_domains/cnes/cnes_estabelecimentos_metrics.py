from main.core.layers.gold import Gold
from datetime import date
import pandas as pd
import pandasql as ps


class CnesEstabelecimentosMetrics(Gold):
    def __init__(self, year_month: str = "all"):
        super().__init__(name="cnes_estabelecimentos_metrics")
        self.year_month = year_month

        self.inputs = {
            "estabelecimentos": self.read_silver_parquet("cnes_estabelecimentos"), # year_month None = "all" (carga full)
            "populacao": self._read_single_parquet(fs_client=self._gold_fs, path="populacao/data.parquet"),
        }

    def definition(self) -> pd.DataFrame:
        estab = self.inputs["estabelecimentos"].copy()
        pop = self.inputs["populacao"].copy()

        # ============================================================
        # filtros
        # ============================================================
        mask_sus = estab.get("TP_SUS_NAO_SUS", "").eq("S")
        mask_med = estab.get("DS_ATIVIDADE_PROFISSIONAL", "").astype(str).str.startswith("MEDICO", na=False)
        estab = estab[mask_sus & mask_med]

        keep_cols = [
            "CO_PROFISSIONAL_SUS",
            "NO_MUNICIPIO",
            "DS_ATIVIDADE_PROFISSIONAL",
            "TP_SUS_NAO_SUS",
            "CO_MUNICIPIO",
            "YYYYMM",
        ]
        estab = estab[[c for c in keep_cols if c in estab.columns]].copy()

        # tipos e chaves
        estab["CO_MUNICIPIO_SEM_DIGITO"] = pd.to_numeric(estab["CO_MUNICIPIO"], errors="coerce").astype("Int64")
        estab["YYYY"] = estab["YYYYMM"].astype(str).str[:4].astype("Int16")
        estab["MM"] = estab["YYYYMM"].astype(str).str[4:6]
        estab["MM"] = pd.Categorical(estab["MM"], categories=[f"{m:02d}" for m in range(1, 13)], ordered=True)

        # agrega profissionais únicos
        query = """
        SELECT
            CO_MUNICIPIO_SEM_DIGITO,
            NO_MUNICIPIO,
            DS_ATIVIDADE_PROFISSIONAL,
            TP_SUS_NAO_SUS,
            YYYY,
            MM,
            COUNT(DISTINCT CO_PROFISSIONAL_SUS) AS TOTAL_PROFISSIONAIS
        FROM estab
        WHERE TP_SUS_NAO_SUS = 'S'
        GROUP BY
            CO_MUNICIPIO_SEM_DIGITO,
            NO_MUNICIPIO,
            DS_ATIVIDADE_PROFISSIONAL,
            TP_SUS_NAO_SUS,
            YYYY,
            MM
        """
        g = ps.sqldf(query, locals())

        # normaliza população
        for c in ["CO_MUNICIPIO_SEM_DIGITO", "YYYY", "MM"]:
            if c not in pop.columns:
                raise KeyError(f"população: coluna obrigatória ausente: {c}")

        pop = pop.copy()
        pop["CO_MUNICIPIO_SEM_DIGITO"] = pd.to_numeric(pop["CO_MUNICIPIO_SEM_DIGITO"], errors="coerce").astype("Int64")
        pop["YYYY"] = pd.to_numeric(pop["YYYY"], errors="coerce").astype("Int16")
        pop["MM"] = pop["MM"].astype(str).str.zfill(2)

        # join e métrica
        join_keys = ["CO_MUNICIPIO_SEM_DIGITO", "YYYY", "MM"]
        cols_pop = join_keys + [
            "CO_UF", "NO_UF", "NO_REGIAO", "NO_MUNICIPIO_IBGE",
            "POPULACAO_MENSAL", "POPULACAO", "GROWTH_ABS", "GROWTH_PCT"
        ]
        cols_pop = [c for c in cols_pop if c in pop.columns]

        df = g.merge(pop[cols_pop], on=join_keys, how="left")
        df["PROFISSIONAIS_POR_1000"] = (
            (df["TOTAL_PROFISSIONAIS"] / df["POPULACAO_MENSAL"].replace({0: pd.NA})) * 1000
        )

        df["DATA_INGESTAO"] = pd.Timestamp.today().strftime("%Y-%m-%d")

        return df