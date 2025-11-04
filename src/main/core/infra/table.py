from typing import Dict
import pandas as pd
from .singleton import SingletonMeta

class Table(metaclass=SingletonMeta):
    layer: str
    allowed_layers: list[str]

    def __init__(self, name: str):
        self.name = name
        self.inputs: Dict[str, pd.DataFrame] = {}
