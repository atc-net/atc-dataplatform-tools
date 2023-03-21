from abc import ABC, abstractmethod


class OtherBaseClass(ABC):
    @classmethod
    @abstractmethod
    def task(cls) -> None:
        pass


class A(OtherBaseClass):
    @classmethod
    def task(cls) -> None:
        pass


class B(OtherBaseClass):
    @classmethod
    def task(cls) -> None:
        pass
