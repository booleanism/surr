from dataclasses import dataclass, field
import json
import typing
from . import handlers

SiblingName = str

@dataclass
class Sibling:
    commands: list[handlers.Command] = field(default_factory=list)

    def to_json(self) -> bytes:
        return json.dumps({"commands": list(self.commands)}).encode()

    @classmethod
    def from_json(cls, data: bytes) -> "Sibling":
        obj = json.loads(data)
        return cls(commands=list(obj.get("commands", [])))

class Siblings:
    def __init__(self) -> None:
        self._d: dict[SiblingName, Sibling] = {}
 
    def get(self, name: SiblingName) -> typing.Optional[Sibling]:
        return self._d.get(name)

    def set(self, name: SiblingName, sibling: Sibling) -> None:
        self._d[name] = sibling

    def get_name(self, command: handlers.Command) -> typing.Optional[list[SiblingName]]:
        names: list[SiblingName] = []
        for k, v in self._d.items():
            for c in v.commands:
                if c != command:
                    continue

                names.append(k)

        if len(names) == 0:
            return None

        return names
