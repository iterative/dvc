from dataclasses import dataclass


@dataclass
class HashInfo:
    name: str
    value: str

    def __bool__(self):
        return bool(self.value)

    @classmethod
    def from_dict(cls, d):
        if not d:
            return cls(None, None)
        ((name, value),) = d.items()
        return cls(name, value)

    def to_dict(self):
        return {self.name: self.value} if self else {}
