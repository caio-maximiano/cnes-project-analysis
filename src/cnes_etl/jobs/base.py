from __future__ import annotations
from abc import ABC, abstractmethod
import logging


logger = logging.getLogger(__name__)


class BaseJob(ABC):
    # @abstractmethod
    def run(self) -> None: # unidade de trabalho
        ...

    def before(self) -> None:
        logger.info("Starting job: %s", self.__class__.__name__)


    def after(self) -> None:
        logger.info("Finished job: %s", self.__class__.__name__)


    def execute(self) -> None:
        self.before()
        try:
            self.run()
        finally:
            self.after()