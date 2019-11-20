import abc
import json
import cattr


class JSONMixin(abc.ABC):
    @classmethod
    def from_file(cls, path):
        with open(path, "r") as fobj:
            data = json.load(fobj)

        return cattr.structure(data, cls)

    def to_file(self, path):
        with open(path, "w+") as fobj:
            json.dump(cattr.unstructure(self), fobj)

    @property
    def asdict(self):
        return cattr.unstructure(self)


def json_serializer(cls):
    cls.from_file = classmethod(JSONMixin.from_file.__func__)
    cls.to_file = JSONMixin.to_file
    cls.asdict = JSONMixin.asdict
    return cls
