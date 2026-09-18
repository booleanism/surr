from dataclasses import dataclass
import json
import typing
import uuid

Command = str

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
            }
        ).encode()

    @classmethod
    def from_json(cls, data: bytes) -> "RouterResponse":
        obj = json.loads(data)
        code = obj.get("code", 1)
        return cls(code=code, result=obj.get("result"))

ReplyFunc = typing.Callable[[RouterResponse], typing.Awaitable[None]]
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
