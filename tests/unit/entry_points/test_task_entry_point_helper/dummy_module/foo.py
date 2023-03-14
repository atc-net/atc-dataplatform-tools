from atc.entry_points import TaskEntryPoint


class A(TaskEntryPoint):
    @classmethod
    def task(cls) -> None:
        pass
