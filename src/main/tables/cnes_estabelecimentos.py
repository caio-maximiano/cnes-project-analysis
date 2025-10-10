from __future__ import annotations
import io
import pandas as pd
from .base import TableContext, table

@table(name="cnes_estabelecimentos_sp")
class CNESEstabelecimentosSP:
    def _read(self, ctx: TableContext, base: str) -> pd.DataFrame:
        remote = f"{ctx.year_month}/{base}{ctx.year_month}.csv"
        raw = ctx.bronze.download_file(remote)
        for enc in ("latin-1", "cp1252", "utf-8-sig"):
            try:
                return pd.read_csv(io.BytesIO(raw), sep=";", quotechar='"', dtype=str,
                                   encoding=enc, engine="python", on_bad_lines="warn")
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError("csv-decode", b"", 0, 1, "Falha ao decodificar")

    def _estab_municipio(self, tbEstab, tbMun):
        t = tbEstab.copy()
        t["CO_ESTADO_GESTOR"] = pd.to_numeric(t.get("CO_ESTADO_GESTOR"), errors="coerce")
        sp = t[t["CO_ESTADO_GESTOR"] == 35]
        return sp.merge(tbMun, left_on="CO_MUNICIPIO_GESTOR", right_on="CO_MUNICIPIO",
                        how="inner", suffixes=("", "_mun"))

    def definition(self, ctx: TableContext) -> pd.DataFrame:
        tbEstab  = self._read(ctx, "tbEstabelecimento")
        tbMun    = self._read(ctx, "tbMunicipio")
        tbCarga  = self._read(ctx, "tbCargaHorariaSus")
        tbAtiv   = self._read(ctx, "tbAtividadeProfissional")
        tbProf   = self._read(ctx, "tbDadosProfissionalSus")

        estab_munic = self._estab_municipio(tbEstab, tbMun)
        joined = (
            tbCarga.merge(tbAtiv, on="CO_CBO", how="inner")
                   .merge(estab_munic, on="CO_UNIDADE", how="inner")
                   .merge(tbProf, on="CO_PROFISSIONAL_SUS", how="inner")
        )

        cols = [
            "CO_UNIDADE","CO_PROFISSIONAL_SUS","NO_PROFISSIONAL","CO_CBO","TP_SUS_NAO_SUS",
            "DS_ATIVIDADE_PROFISSIONAL","NO_FANTASIA","NO_BAIRRO","NO_MUNICIPIO",
            "CO_MUNICIPIO","CO_SIGLA_ESTADO","CO_CEP"
        ]
        df = joined[cols].copy()
        df["ds_localidade"] = (
            df["CO_CEP"].astype(str) + "," + df["NO_MUNICIPIO"].astype(str) + "," +
            df["CO_SIGLA_ESTADO"].astype(str) + ",Brasil"
        )
        df["SK_REGISTRO"] = (
            df["CO_UNIDADE"].astype(str) + "_" +
            df["CO_PROFISSIONAL_SUS"].astype(str) + "_" +
            df["CO_CBO"].astype(str)
        )
        df["YYYYMM"] = ctx.year_month
        df = df.drop_duplicates(subset=["SK_REGISTRO"])
        return df

    def run(self, ctx: TableContext) -> None:
        df = self.definition(ctx)
        local = ctx.local_dir / f"cnes_estabelecimentos_sp_{ctx.year_month}.parquet"
        df.to_parquet(local, index=False, engine="pyarrow", compression="snappy")
        remote = f"estabelecimentos/year_month={ctx.year_month}/data.parquet"
        ctx.silver.upload_file(local, remote)
