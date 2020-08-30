from dataclasses import dataclass


@dataclass
class HashInfo:
    name: str
    value: str

    def __bool__(self):
        return bool(self.value)
