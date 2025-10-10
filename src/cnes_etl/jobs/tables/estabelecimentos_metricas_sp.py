from __future__ import annotations
import pandas as pd
from typing import Iterable, Callable
from cnes_etl.jobs.tables.base import BaseTable, register_table, Layer

@register_table
class EstabelecimentosMetricasSp(BaseTable):
    """
    Métricas mensais para SP:
    - profissionais SUS (distinct CO_PROFISSIONAL_SUS) por município × ano × mês,
    - filtrando apenas atividades que começam com "MEDICO",
    - join com população mensal (parquet único no FS gold),
    - métrica final: PROFISSIONAIS_POR_1000.
    """
    name = "estabelecimentos_metricas_sp"
    layer = Layer.GOLD
    dependencies: list[str] = []

    POP_FS = "gold"
    POP_PATH = "populacao/data.parquet"

    def build(self, bronze, cache):
    # Esta tabela é GOLD (full-load) e não usa 'build' (apenas 'build_curated_full')
        raise NotImplementedError("Tabela GOLD: use 'build_curated_full'.")

    def _coerce_types_estab(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "YYYYMM" in out.columns:
            out["YYYY"] = out["YYYYMM"].astype(str).str[:4]
            out["MM"] = out["YYYYMM"].astype(str).str[-2:]

        for c in [
            "CO_UNIDADE","CO_PROFISSIONAL_SUS","NO_PROFISSIONAL","NO_MUNICIPIO",
            "NO_FANTASIA","NO_BAIRRO","CO_SIGLA_ESTADO","CO_CEP","DS_ATIVIDADE_PROFISSIONAL",
        ]:
            if c in out.columns:
                out[c] = out[c].astype("string")

        if "CO_MUNICIPIO" in out.columns:
            out["CO_MUNICIPIO"] = pd.to_numeric(out["CO_MUNICIPIO"], errors="coerce").astype("Int64")
            out["CO_MUNICIPIO_SEM_DIGITO"] = out["CO_MUNICIPIO"].astype("Int64")

        if "TP_SUS_NAO_SUS" in out.columns:
            out["TP_SUS_NAO_SUS"] = pd.Categorical(out["TP_SUS_NAO_SUS"], categories=["N","S"], ordered=True)

        if "YYYY" in out.columns:
            out["YYYY"] = pd.to_numeric(out["YYYY"], errors="coerce").astype("Int16")
        if "MM" in out.columns:
            out["MM"] = pd.to_numeric(out["MM"], errors="coerce").astype("Int16")
        return out

    def _load_populacao(self, read_external_parquet: Callable[[str, str], pd.DataFrame]) -> pd.DataFrame:
        pop = read_external_parquet(self.POP_FS, self.POP_PATH).copy()
        rename_map = {}
        if "CO_MUNICIPIO" in pop.columns and "CO_MUNICIPIO_SEM_DIGITO" not in pop.columns:
            rename_map["CO_MUNICIPIO"] = "CO_MUNICIPIO_SEM_DIGITO"
        if rename_map:
            pop = pop.rename(columns=rename_map)

        for c in ["CO_MUNICIPIO_SEM_DIGITO","YYYY","MM"]:
            if c in pop.columns:
                pop[c] = pd.to_numeric(pop[c], errors="coerce")
        pop["CO_MUNICIPIO_SEM_DIGITO"] = pop["CO_MUNICIPIO_SEM_DIGITO"].astype("Int64")
        pop["YYYY"] = pop["YYYY"].astype("Int16")
        pop["MM"] = pop["MM"].astype("Int16")

        if "POPULACAO_MENSAL" not in pop.columns:
            if "POPULACAO" in pop.columns:
                pop["POPULACAO_MENSAL"] = pop["POPULACAO"]
            else:
                raise KeyError("Parquet de população precisa de POPULACAO_MENSAL ou POPULACAO")

        keep = ["CO_MUNICIPIO_SEM_DIGITO","YYYY","MM","POPULACAO_MENSAL"]
        for extra in ["CO_UF","NO_UF","NO_REGIAO","NO_MUNICIPIO_IBGE","POPULACAO","GROWTH_ABS","GROWTH_PCT"]:
            if extra in pop.columns:
                keep.append(extra)
        return pop[keep].drop_duplicates()

    def build_curated_full(
        self,
        read_silver: Callable[[str], Iterable[pd.DataFrame]],
        cache,
        read_external_parquet: Callable[[str, str], pd.DataFrame],
    ) -> pd.DataFrame:
        agg: pd.DataFrame | None = None

        for chunk in read_silver("estabelecimentos"):
            chunk = self._coerce_types_estab(chunk)
            is_sp = chunk.get("CO_SIGLA_ESTADO").astype("string") == "SP"
            is_sus = chunk.get("TP_SUS_NAO_SUS").astype("string") == "S"
            is_med = chunk.get("DS_ATIVIDADE_PROFISSIONAL").astype("string").str.startswith("MEDICO", na=False)
            filt = chunk[is_sp & is_sus & is_med]

            need = {"CO_MUNICIPIO_SEM_DIGITO","YYYY","MM","CO_PROFISSIONAL_SUS"}
            if not need <= set(filt.columns):
                raise KeyError(f"Colunas esperadas não encontradas em estabelecimentos: {sorted(need - set(filt.columns))}")

            g = (filt
                 .groupby(["CO_MUNICIPIO_SEM_DIGITO","YYYY","MM"], as_index=False)["CO_PROFISSIONAL_SUS"]
                 .nunique()
                 .rename(columns={"CO_PROFISSIONAL_SUS":"TOTAL_PROFISSIONAIS"}))

            agg = g if agg is None else (
                pd.concat([agg, g], ignore_index=True)
                  .groupby(["CO_MUNICIPIO_SEM_DIGITO","YYYY","MM"], as_index=False)
                  .agg({"TOTAL_PROFISSIONAIS":"sum"})
            )

        if agg is None:
            agg = pd.DataFrame(columns=["CO_MUNICIPIO_SEM_DIGITO","YYYY","MM","TOTAL_PROFISSIONAIS"]).astype({
                "CO_MUNICIPIO_SEM_DIGITO":"Int64","YYYY":"Int16","MM":"Int16"
            })

        pop = self._load_populacao(read_external_parquet)
        out = agg.merge(
            pop, on=["CO_MUNICIPIO_SEM_DIGITO","YYYY","MM"], how="left", validate="m:1"
        )

        out["PROFISSIONAIS_POR_1000"] = (out["TOTAL_PROFISSIONAIS"] / out["POPULACAO_MENSAL"]) * 1000

        out["sk"] = (
            out["CO_MUNICIPIO_SEM_DIGITO"].astype("Int64").astype("string") + "_" +
            out["YYYY"].astype("Int16").astype("string") + "_" +
            out["MM"].astype("Int16").astype("string")
        )
        return out.sort_values(["CO_MUNICIPIO_SEM_DIGITO","YYYY","MM"]).reset_index(drop=True)
