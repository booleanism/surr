from dataclasses import dataclass, field
import json
import time
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

# How long a sibling stays resolvable after its last introduce. Mirrors
# SiblingTTL in internal/protocols/siblings.go: every service broadcasts one per
# second, so a peer silent this long has missed fifteen in a row and is gone.
# Keep the two numbers equal -- they are the same behaviour on both sides.
SIBLING_TTL_SECONDS = 15.0

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
    """Resolves a command to the peer(s) that advertise it.

    An entry expires SIBLING_TTL_SECONDS after the last introduce that carried
    it. Every service broadcasts one per second, so a peer silent for this long
    has missed fifteen in a row -- it is not slow, it is gone. Without expiry a
    node killed without a graceful ``nodes.remove`` stays resolvable for the
    life of the process, which makes ``rpc._call_once`` try a dead address
    first, and makes a Go service spend a whole command timeout on it before
    treating the sink as unavailable. See TODO.md F5.

    Expiry is lazy: an entry is checked, and dropped, when it is looked up. That
    is where it matters -- the question is always "is this peer still there?" at
    the moment of dispatch -- and it needs no background task or shutdown path.
    The cost is that an entry nobody asks about lingers in the map, which is one
    dead key per peer.
    """

    def __init__(self, clock: typing.Callable[[], float] = time.monotonic) -> None:
        self._d: dict[SiblingName, Sibling] = {}
        self._seen: dict[SiblingName, float] = {}
        # Injected so a test can age an entry instead of sleeping for the TTL.
        self._clock = clock

    def _fresh(self, name: SiblingName) -> bool:
        """Whether ``name`` may still be used, forgetting it when it may not."""
        seen = self._seen.get(name)
        if seen is not None and self._clock() - seen <= SIBLING_TTL_SECONDS:
            return True

        self._d.pop(name, None)
        self._seen.pop(name, None)

        return False

    def get(self, name: SiblingName) -> typing.Optional[Sibling]:
        if not self._fresh(name):
            return None

        return self._d.get(name)

    def set(self, name: SiblingName, sibling: Sibling) -> None:
        # Every introduce lands here, so this is also the refresh point: a peer
        # that keeps introducing itself never expires.
        self._d[name] = sibling
        self._seen[name] = self._clock()

    def get_name(self, command: handlers.Command) -> typing.Optional[list[SiblingName]]:
        names: list[SiblingName] = []
        for k, v in list(self._d.items()):
            if not self._fresh(k):
                continue

            for c in v.commands:
                if c != command:
                    continue

                names.append(k)

        if len(names) == 0:
            return None

        return names
