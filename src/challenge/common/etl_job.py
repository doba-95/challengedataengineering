from abc import ABC, abstractmethod

from pandas.core.interchange.dataframe_protocol import DataFrame


class ETLJob(ABC):
    @abstractmethod
    def extract(self) -> DataFrame |tuple[DataFrame, ...]:
        pass

    @abstractmethod
    def transform(self, *inputs: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def load(self, output: DataFrame) -> None:
        pass

    @abstractmethod
    def run(self):
        pass
