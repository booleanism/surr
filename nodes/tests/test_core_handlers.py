"""``core.handlers``: the wire shape of a RouterResponse.

This is the Python half of the protocol the Go services speak
(``internal/protocols/handlers.go``). ``RouterResponse.GetBytes`` on the Go
side base64-decodes a JSON *string* when the result parses as one, which is
why bytes have to leave this side base64-encoded rather than as a JSON array
of numbers.
"""

from __future__ import annotations

import base64
import json
import unittest
import uuid

from core import handlers

class JsonSerializerTests(unittest.TestCase):
    def test_bytes_become_base64_strings(self):
        self.assertEqual(handlers.json_serializer(b"\x00\x01\xfe"), "AAH+")

    def test_empty_bytes_are_not_confused_with_absent(self):
        self.assertEqual(handlers.json_serializer(b""), "")

    def test_other_types_fall_back_to_str(self):
        self.assertEqual(handlers.json_serializer(uuid.UUID(int=0)), "00000000-0000-0000-0000-000000000000")

class RouterResponseTests(unittest.TestCase):
    def test_wire_shape_is_exactly_code_and_result(self):
        wire = handlers.RouterResponse(0, [1, 2]).to_json()
        self.assertEqual(json.loads(wire), {"code": 0, "result": [1, 2]})

    def test_the_serialized_form_is_frozen(self):
        # A golden assertion on the exact bytes rather than on the parsed
        # object: this is the contract Go's RouterResponse.GetBytes reads, so a
        # change in the key set, the separators or the bytes convention has to
        # fail here and not only in a live cross-language run.
        cases = [
            (handlers.RouterResponse(0, [1, 2]), b'{"code": 0, "result": [1, 2]}'),
            (handlers.RouterResponse(-2, "not found"), b'{"code": -2, "result": "not found"}'),
            (handlers.RouterResponse(0, b"\x00\x01\xfe"), b'{"code": 0, "result": "AAH+"}'),
        ]
        for response, expected in cases:
            with self.subTest(response=response):
                self.assertEqual(response.to_json(), expected)

    def test_code_and_result_survive_a_round_trip(self):
        resp = handlers.RouterResponse(-2, "subscription not found")
        back = handlers.RouterResponse.from_json(resp.to_json())
        self.assertEqual(back.code, -2)
        self.assertEqual(back.result, "subscription not found")

    def test_bytes_round_trip_as_base64(self):
        # Go decodes this back to the original bytes; Python does not, because
        # a Python-side reply never carries raw bytes in the other direction.
        resp = handlers.RouterResponse(0, b"\x00\x01\xfe")
        result = handlers.RouterResponse.from_json(resp.to_json()).result
        self.assertEqual(result, "AAH+")
        self.assertEqual(base64.b64decode(result), b"\x00\x01\xfe")

    def test_a_missing_code_defaults_to_success(self):
        resp = handlers.RouterResponse.from_json(b'{"result":null}')
        self.assertEqual(resp.code, 0)

    def test_a_missing_result_is_none(self):
        resp = handlers.RouterResponse.from_json(b'{"code":-1}')
        self.assertIsNone(resp.result)

    def test_undecodable_body_raises(self):
        with self.assertRaises(json.JSONDecodeError):
            handlers.RouterResponse.from_json(b"not json")

class HandlersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.handlers = handlers.Handlers()

    def test_get_returns_a_registered_handler(self):
        async def handler(track_id, req, rep):  # pragma: no cover - never called
            pass

        self.handlers.set("nodes.add", handler)
        self.assertIs(self.handlers.get("nodes.add"), handler)

    def test_get_raises_for_an_unknown_command(self):
        # router._handle_inbound_request relies on this to nack instead of
        # dispatching an unknown message type.
        with self.assertRaises(ValueError):
            self.handlers.get("nodes.nope")

    def test_set_overwrites(self):
        async def first(track_id, req, rep):  # pragma: no cover - never called
            pass

        async def second(track_id, req, rep):  # pragma: no cover - never called
            pass

        self.handlers.set("c", first)
        self.handlers.set("c", second)
        self.assertIs(self.handlers.get("c"), second)
        self.assertEqual(self.handlers.available_commands(), ["c"])

    def test_available_commands_is_advertised_by_introduce(self):
        async def handler(track_id, req, rep):  # pragma: no cover - never called
            pass

        self.handlers.set("b.sink.start", handler)
        self.handlers.set("b.sink.stop", handler)
        self.assertEqual(sorted(self.handlers.available_commands()), ["b.sink.start", "b.sink.stop"])

    def test_available_commands_is_a_snapshot(self):
        async def handler(track_id, req, rep):  # pragma: no cover - never called
            pass

        self.handlers.set("a", handler)
        listed = self.handlers.available_commands()
        self.handlers.set("b", handler)
        self.assertEqual(listed, ["a"])

if __name__ == "__main__":
    unittest.main()
