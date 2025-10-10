# Reexporta utilitários
from .base import BaseTable, register_table, list_tables, make_table  # noqa: F401

# IMPORTANTE: importar as tabelas concretas para que @register_table rode
from .servicos import ServicosTable  # noqa: F401
from .estabelecimentos import EstabelecimentosTable  # noqa: F401

from .estabelecimentos_metricas_sp import EstabelecimentosMetricasSp  # noqa: F401

