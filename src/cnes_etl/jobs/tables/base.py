from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Type, List
import pandas as pd
from enum import Enum
# Registry simples para descoberta automática
_TABLE_REGISTRY: Dict[str, Type["BaseTable"]] = {}
_instance_cache: dict[tuple[str, str], "BaseTable"] = {}

class Layer(str, Enum):
    SILVER = "silver"
    GOLD = "gold"

class BaseTable(ABC):
    """Cada tabela é um objeto com nome, dependências e método build()."""
    # nome lógico da tabela (e.g., "servicos")
    name: str
    # nomes de tabelas das quais depende (para ordem de execução)
    dependencies: List[str] = []

    layer: Layer = Layer.SILVER  # camada padrão é "silver", pode ser sobrescrito

    def __init__(self, yyyymm: str):
        self.yyyymm = yyyymm

    @abstractmethod
    def build(self, bronze: Dict[str, pd.DataFrame], cache: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Retorna o DataFrame final da tabela. Pode ler 'cache' com dependências já construídas."""
        ...

def register_table(cls: Type["BaseTable"]) -> Type["BaseTable"]:
    """Decorator para auto-registrar a tabela pelo atributo 'name'."""
    if not getattr(cls, "name", None): # sanity check
        raise ValueError("Table class must define a unique 'name' attribute")
    if cls.name in _TABLE_REGISTRY:
        raise ValueError(f"Table '{cls.name}' already registered")
    _TABLE_REGISTRY[cls.name] = cls
    return cls

def list_tables() -> List[str]:
        return list(_TABLE_REGISTRY.keys())

def make_table(name: str, yyyymm: str) -> BaseTable:
        key = (name, yyyymm)
        if key not in _instance_cache:
            _instance_cache[key] = _TABLE_REGISTRY[name](yyyymm)
        return _instance_cache[key]