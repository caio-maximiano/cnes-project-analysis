from __future__ import annotations
import io
import pandas as pd
from .base import TableContext, table

@table(name="cnes_servicos_sp")
class CNESServicosSP:
    def _read_cnes_csv(self, ctx: TableContext, base: str) -> pd.DataFrame:
        remote = f"{ctx.year_month}/{base}{ctx.year_month}.csv"
        raw = ctx.bronze.download_file(remote)
        for enc in ("latin-1", "cp1252", "utf-8-sig"):
            try:
                return pd.read_csv(io.BytesIO(raw), sep=";", quotechar='"', dtype=str,
                                   encoding=enc, engine="python", on_bad_lines="warn")
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError("csv-decode", b"", 0, 1, "Falha ao decodificar")

    def _estab_municipio(self, tbEstab: pd.DataFrame, tbMun: pd.DataFrame) -> pd.DataFrame:
        t = tbEstab.copy()
        t["CO_ESTADO_GESTOR"] = pd.to_numeric(t.get("CO_ESTADO_GESTOR"), errors="coerce")
        sp = t[t["CO_ESTADO_GESTOR"] == 35]
        return sp.merge(tbMun, left_on="CO_MUNICIPIO_GESTOR", right_on="CO_MUNICIPIO",
                        how="inner", suffixes=("", "_mun"))

    def definition(self, ctx: TableContext) -> pd.DataFrame:
        tbEstab  = self._read_cnes_csv(ctx, "tbEstabelecimento")
        tbMun    = self._read_cnes_csv(ctx, "tbMunicipio")
        rlServ   = self._read_cnes_csv(ctx, "rlEstabServClass")
        tbClass  = self._read_cnes_csv(ctx, "tbClassificacaoServico")

        estab_munic = self._estab_municipio(tbEstab, tbMun)
        serv_join = (
            rlServ.merge(tbClass,
                         left_on=["CO_SERVICO", "CO_CLASSIFICACAO"],
                         right_on=["CO_SERVICO_ESPECIALIZADO", "CO_CLASSIFICACAO_SERVICO"],
                         how="inner")
                  .merge(estab_munic, on="CO_UNIDADE", how="inner")
        )
        cols = ["CO_UNIDADE","NO_MUNICIPIO","CO_MUNICIPIO","CO_SERVICO","CO_CLASSIFICACAO","DS_CLASSIFICACAO_SERVICO"]
        df = serv_join[cols].copy()
        df["SK_REGISTRO"] = df["CO_UNIDADE"].astype(str) + "_" + df["CO_SERVICO"].astype(str) + "_" + df["CO_CLASSIFICACAO"].astype(str)
        df["YYYYMM"] = ctx.year_month
        df = df.drop_duplicates(subset=["SK_REGISTRO"])
        return df

    def run(self, ctx: TableContext) -> None:
        df = self.definition(ctx)
        local = ctx.local_dir / f"cnes_servicos_sp_{ctx.year_month}.parquet"
        df.to_parquet(local, index=False, engine="pyarrow", compression="snappy")
        remote = f"servicos/year_month={ctx.year_month}/data.parquet"
        ctx.silver.upload_file(local, remote)
