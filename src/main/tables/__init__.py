# importa os módulos para acionar os decorators e registrar as tabelas
from . import cnes_servicos  # noqa: F401
from . import cnes_estabelecimentos  # noqa: F401
from . import cnes_estabelecimentos_metricas  # <--- novo

from .base import list_tables, get_table, TableContext  # reexport
