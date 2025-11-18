# src/main/data_domains/registry.py
from typing import Dict, Type

# Tabelas
from .cnes.cnes_servicos import CnesServicos
from .cnes.cnes_estabelecimentos import CnesEstabelecimentos
from .cnes.cnes_estabelecimentos_metrics import CnesEstabelecimentosMetrics

# Modelos
from .cnes.models.cnes_linear_regression import CnesLinearRegression

JOBS: Dict[str, Type] = {
    # Tables
    "cnes_servicos": CnesServicos,
    "cnes_estabelecimentos": CnesEstabelecimentos,
    "cnes_estabelecimentos_metrics": CnesEstabelecimentosMetrics,
    # Models
    "cnes_linear_regression": CnesLinearRegression,
}

def list_jobs() -> Dict[str, Type]:
    return dict(JOBS)

def get_job(name: str):
    try:
        return JOBS[name]
    except KeyError:
        raise SystemExit(f"Job desconhecido: {name}. Use `python -m src.main list`.")
