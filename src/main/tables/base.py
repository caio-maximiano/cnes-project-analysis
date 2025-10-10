from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Dict, Callable, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta
import inspect  # <-- add

from core.storage import Storage

@dataclass(frozen=True)
class TableContext:
    year_month: str
    bronze: Storage
    silver: Storage
    gold: Storage
    local_dir: Path

    @staticmethod
    def for_month(year_month: Optional[str], months_back: int = 3) -> "TableContext":
        ym = year_month or (datetime.today() - relativedelta(months=months_back)).strftime("%Y%m")
        bronze = Storage(file_system="bronze")
        silver = Storage(file_system="silver")
        gold   = Storage(file_system="gold")
        local = Path("./local_storage/curated")
        local.mkdir(parents=True, exist_ok=True)
        return TableContext(ym, bronze, silver, gold, local)

class TableDefinition(Protocol):
    name: str
    def definition(self, ctx: TableContext): ...
    def run(self, ctx: TableContext) -> None: ...

_REGISTRY: Dict[str, TableDefinition] = {}

def table(name: str) -> Callable[[TableDefinition], TableDefinition]:
    """
    Decorator que aceita CLASSE ou INSTÂNCIA.
    - Se for classe, instancia automaticamente.
    - Registra no _REGISTRY e retorna a instância registrada.
    """
    def _wrap(obj: TableDefinition) -> TableDefinition:
        instance = obj() if inspect.isclass(obj) else obj  # <- aceita classe ou instância
        if hasattr(instance, "__dict__"):
            instance.__dict__["name"] = name  # evita __setattr__ especial de dataclass frozen etc.
        if name in _REGISTRY:
            raise ValueError(f"Tabela '{name}' já registrada.")
        _REGISTRY[name] = instance
        return instance  # <- o símbolo decorado passa a ser a instância!
    return _wrap

def get_table(name: str) -> TableDefinition:
    return _REGISTRY[name]

def list_tables() -> list[str]:
    return sorted(_REGISTRY.keys())
