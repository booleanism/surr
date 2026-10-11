from dataclasses import dataclass
import json
import typing
import uuid
import base64

Command = str

def json_serializer(obj: typing.Any) -> str:
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode('utf-8')
    return str(obj)

@dataclass
class RouterRequest:
    payload: bytes


@dataclass
class RouterResponse:
    code: int
    result: typing.Any

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "code": self.code,
                "result": self.result,
            },
            default=json_serializer
        ).encode()

    @classmethod
    def from_json(cls, data: bytes) -> "RouterResponse":
        obj = json.loads(data)
        code = obj.get("code", 0)
        return cls(code=code, result=obj.get("result"))

# A ReplyFunc returns a Coroutine, not a bare Awaitable, so a handler that
# calls rep(...) without awaiting it is visible as a never-awaited coroutine
# rather than silently producing no reply.
ReplyFunc = typing.Callable[[RouterResponse], typing.Coroutine[typing.Any, typing.Any, None]]
HandlerFunc = typing.Callable[[uuid.UUID, RouterRequest, ReplyFunc], typing.Coroutine[typing.Any, typing.Any, None]]

class Handlers:
    def __init__(self) -> None:
        self._d: dict[Command, HandlerFunc] = {}

    def get(self, command: Command) -> HandlerFunc:
        handler = self._d.get(command)
        if handler is None:
            raise ValueError("unknown command")

        return handler

    def available_commands(self) -> list[Command]:
        return list(self._d.keys())

    def set(self, command: Command, handler: HandlerFunc):
        self._d[command] = handler
