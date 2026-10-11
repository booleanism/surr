from dataclasses import dataclass, field
import json
import typing
from . import handlers

SiblingName = str

# Wire revision of the sibling RPC protocol. Bump it for a change that stops two
# peers talking to each other: a renamed command, a changed request or reply
# payload shape, a new required field. Adding an optional field is not a break,
# because an old peer ignores what it does not know, so it needs no bump.
#
# Every introduce advertises it and a peer reporting a different revision is
# logged as a warning. Without that a mixed deployment is silent: the peers stay
# connected and fail later, at the first command whose payload they disagree
# about. The Go services mirror this number in internal/protocols/siblings.go,
# and the two must be changed together.
PROTOCOL_REVISION = 1

@dataclass
class Sibling:
    commands: list[handlers.Command] = field(default_factory=list)

    # The peer's wire revision, compared against PROTOCOL_REVISION when its
    # introduce arrives. A peer predating this field sends nothing and decodes
    # as 0, which is correct: it really does not speak revision 1.
    protocol_revision: int = 0

    # The peer's build id, carried so a mismatch can name the build it is
    # talking to. Nothing branches on it: two peers running different builds is
    # the normal case.
    version: str = ""

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "commands": list(self.commands),
                "protocol_revision": self.protocol_revision,
                "version": self.version,
            }
        ).encode()

    @classmethod
    def from_json(cls, data: bytes) -> "Sibling":
        obj = json.loads(data)
        return cls(
            commands=list(obj.get("commands", [])),
            protocol_revision=int(obj.get("protocol_revision", 0)),
            version=str(obj.get("version", "")),
        )

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
