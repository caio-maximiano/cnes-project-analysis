# src/main/domains/registry.py
from typing import Dict, Type
from .cnes.cnes_servicos import CnesServicos
from .cnes.cnes_estabelecimentos import CnesEstabelecimentos
from .cnes.cnes_estabelecimentos_metrics import CnesEstabelecimentosMetrics

TABLES: dict[str, type] = {
    "cnes_servicos": CnesServicos,
    "cnes_estabelecimentos": CnesEstabelecimentos,
    "cnes_estabelecimentos_metrics": CnesEstabelecimentosMetrics,
}


def list_tables() -> Dict[str, Type]:
    return dict(TABLES)

def get_table(name: str):
    try:
        return TABLES[name]
    except KeyError:
        raise SystemExit(f"Tabela desconhecida: {name}. Use `python -m main list`.")
