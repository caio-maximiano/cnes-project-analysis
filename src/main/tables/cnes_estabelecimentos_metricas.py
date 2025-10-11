from __future__ import annotations
from curses import raw
import io
import re
from typing import Optional, List, Tuple, Iterable
from azure.core.exceptions import ResourceNotFoundError  # opcional
import pyarrow
import pandas as pd
import pandasql as ps
from .base import TableContext, table

def _read_parquet(storage, remote_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Lê um único parquet do ADLS. Se o arquivo estiver corrompido/0 bytes, retorna DataFrame vazio.
    """
    print(f"   lendo {remote_path} ... ", end="", flush=True)
    raw = storage.download_file(remote_path)
    if not raw or len(raw) == 0:
        # arquivo vazio: retorna df vazio
        return pd.DataFrame()
    try:
        return pd.read_parquet(io.BytesIO(raw), engine="pyarrow", columns=columns)
    except (pyarrow.lib.ArrowInvalid, OSError, ValueError):
        # arquivo corrompido ou inválido: retorna df vazio
        return pd.DataFrame()

def _ym_ok(ym: str) -> bool:
    return isinstance(ym, str) and re.fullmatch(r"\d{6}", ym) is not None


def _list_partitions(storage, prefix: str, partition_key: str = "year_month", filename: str = "data.parquet") -> List[Tuple[str, str]]:
    """
    Descobre arquivos no layout: <prefix>/<partition_key>=YYYYMM/<filename>
    Retorna lista [(remote_path, YYYYMM)]
    """
    pat = re.compile(rf"^{re.escape(prefix)}/{re.escape(partition_key)}=(\d{{6}})/{re.escape(filename)}$")
    paths = storage.list_paths(prefix=prefix)
    return [(p, pat.match(p).group(1)) for p in paths if pat.match(p)]


def _cast_estab_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # campos textuais (mantém zeros à esquerda)
    text_as_string = [
        "CO_UNIDADE", "CO_PROFISSIONAL_SUS", "NO_PROFISSIONAL",
        "NO_FANTASIA", "NO_BAIRRO", "NO_MUNICIPIO",
        "CO_SIGLA_ESTADO", "CO_CEP", "ds_localidade",
        "SK_REGISTRO", "DS_ATIVIDADE_PROFISSIONAL",
    ]
    for c in text_as_string:
        if c in out.columns:
            out[c] = out[c].astype("string")

    # códigos numéricos
    for c in ["CO_MUNICIPIO", "CO_MUNICIPIO_COMPLETO"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

    if "CO_CBO" in out.columns:
        out["CO_CBO"] = out["CO_CBO"].astype("string")

    if "TP_SUS_NAO_SUS" in out.columns:
        out["TP_SUS_NAO_SUS"] = pd.Categorical(out["TP_SUS_NAO_SUS"], categories=["N", "S"], ordered=True)

    if "DATA_INGESTAO" in out.columns:
        out["DATA_INGESTAO"] = pd.to_datetime(out["DATA_INGESTAO"], errors="coerce")

    return out


def _add_time_keys(df: pd.DataFrame, ym_col: str = "year_month") -> pd.DataFrame:
    df = df.copy()
    if ym_col not in df.columns:
        raise KeyError(f"esperado coluna '{ym_col}' com YYYYMM")
    df["YYYY"] = df[ym_col].astype(str).str.slice(0, 4).astype("Int16")
    df["MM"]   = df[ym_col].astype(str).str.slice(4, 6)
    df["MM"]   = pd.Categorical(df["MM"], categories=[f"{m:02d}" for m in range(1, 13)], ordered=True)
    return df


def _add_municipio_sem_digito(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera CO_MUNICIPIO_SEM_DIGITO removendo um dígito final quando houver (divide por 10),
    preservando nulos.
    """
    df = df.copy()
    if "CO_MUNICIPIO" not in df.columns:
        raise KeyError("esperado 'CO_MUNICIPIO' em estabelecimentos")
    s = pd.to_numeric(df["CO_MUNICIPIO"], errors="coerce")
    df["CO_MUNICIPIO_SEM_DIGITO"] = (s // 10).astype("Int64")
    return df


def _read_parquet(storage, remote_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    raw = storage.download_file(remote_path)
    return pd.read_parquet(io.BytesIO(raw), engine="pyarrow", columns=columns)


@table(name="cnes_estabelecimentos_sp_metricas")
class CNESEstabelecimentosSPMetricas:
    """
    Métricas mensais por município/atividade para SP:
      - entrada: silver/estabelecimentos/year_month=YYYYMM/data.parquet
      - população: gold/populacao/data.parquet
      - saída: gold/metricas/estabelecimentos_sp/year_month=YYYYMM/data.parquet
    """

    # ---------- leitura ----------
    def _load_estabelecimentos_all(self, ctx: TableContext) -> pd.DataFrame:
        parts = _list_partitions(ctx.silver, prefix="estabelecimentos")
        if not parts:
            raise FileNotFoundError("Nenhuma partição em silver/estabelecimentos/year_month=*/data.parquet")
        dfs = []
        skipped = []
        for remote_path, ym in sorted(parts, key=lambda x: x[1]):
            print(f"   lendo {remote_path} ... ", end="", flush=True)
            df = _read_parquet(ctx.silver, remote_path)
            if df.empty:
                skipped.append((remote_path, ym))
                continue
            columns=["CO_PROFISSIONAL_SUS","NO_MUNICIPIO", "DS_ATIVIDADE_PROFISSIONAL", "TP_SUS_NAO_SUS", "CO_MUNICIPIO","YYYYMM"]
            df = df[
                (df["TP_SUS_NAO_SUS"] == "S") &
                (df["DS_ATIVIDADE_PROFISSIONAL"].str.startswith("MEDICO", na=False))
            ]
            df = df.loc[:, columns]
            df["year_month"] = ym
            dfs.append(df)

        if not dfs:
            raise RuntimeError("Todas as partições estavam vazias/ilegíveis. Verifique os arquivos no Silver.")

        if skipped:
            print(f"[warn] pulando {len(skipped)} partições vazias/corrompidas (ex.: {skipped[:3]})")

        out = pd.concat(dfs, ignore_index=True)
        out = _cast_estab_types(out)
        out = _add_time_keys(out, ym_col="year_month")
        out = _add_municipio_sem_digito(out)
        return out


    def _load_populacao(self, ctx: TableContext) -> pd.DataFrame:
        # caminho declarado pequeno e único
        df = _read_parquet(ctx.gold, "populacao/data.parquet")
        # saneia tipos esperados
        # chaves de junção: CO_MUNICIPIO_SEM_DIGITO (Int64), YYYY (Int16), MM (string/"01".."12")
        for c in ["CO_MUNICIPIO_SEM_DIGITO", "YYYY", "MM"]:
            if c not in df.columns:
                raise KeyError(f"coluna esperada em população: {c}")
        df["CO_MUNICIPIO_SEM_DIGITO"] = pd.to_numeric(df["CO_MUNICIPIO_SEM_DIGITO"], errors="coerce").astype("Int64")
        df["YYYY"] = pd.to_numeric(df["YYYY"], errors="coerce").astype("Int16")
        df["MM"] = df["MM"].astype(str).str.zfill(2)
        return df

    # ---------- transformação principal ----------
    def definition(self, ctx: TableContext) -> pd.DataFrame:
        estab = self._load_estabelecimentos_all(ctx)
        pop   = self._load_populacao(ctx)

        # # agrega profissionais distintos por município/atividade/tempo
        # group_cols = [
        #     "CO_MUNICIPIO_SEM_DIGITO", "NO_MUNICIPIO",
        #     "DS_ATIVIDADE_PROFISSIONAL", "YYYY", "MM"
        # ]
        # g = (
        #     estab.groupby(group_cols, dropna=False)["CO_PROFISSIONAL_SUS"]
        #     .nunique()
        #     .reset_index(name="TOTAL_PROFISSIONAIS")
        # )

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
        schema = g.columns.to_series().map(type)
        print("schema de g (após groupby):")
        print(schema)
        schema = pop.columns.to_series().map(type)
        print("schema de pop (população):")
        print(schema)
        # join com população
        join_keys = ["CO_MUNICIPIO_SEM_DIGITO", "YYYY", "MM"]
        df = g.merge(
            pop[join_keys + ["CO_UF", "NO_UF", "NO_REGIAO", "NO_MUNICIPIO_IBGE",
                             "POPULACAO_MENSAL", "POPULACAO", "GROWTH_ABS", "GROWTH_PCT"]],
            on=join_keys,
            how="left",
        )

        # métrica final
        df["PROFISSIONAIS_POR_1000"] = (df["TOTAL_PROFISSIONAIS"] / df["POPULACAO_MENSAL"]) * 1000

        # recria partição YYYYMM para escrita
        df["year_month"] = df["YYYY"].astype(str) + df["MM"].astype(str)

        # ordena e tipos
        df = df.sort_values(["year_month", "CO_MUNICIPIO_SEM_DIGITO", "DS_ATIVIDADE_PROFISSIONAL"]).reset_index(drop=True)

        return df

    # ---------- escrita ----------
    def run(self, ctx: TableContext) -> None:
        df = self.definition(ctx)

        # escreve uma partição por YYYYMM
        out_prefix = "metricas/estabelecimentos_sp"
        for ym, part in df.groupby("year_month", sort=True):
            local = ctx.local_dir / f"metricas_estabelecimentos_sp_{ym}.parquet"
            part.to_parquet(local, index=False, engine="pyarrow", compression="snappy")
            remote = f"{out_prefix}/year_month={ym}/data.parquet"
            ctx.gold.upload_file(local, remote)

        gold = ctx.gold
        raw = gold.download_file("metricas/estabelecimentos_sp/year_month=202212/data.parquet")
        df = pd.read_parquet(io.BytesIO(raw), engine="pyarrow")
        df.head(3)
