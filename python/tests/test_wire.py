import base64
import shutil
from pathlib import Path

import pytest

from p2plab.identity import load_or_create
from p2plab.wire import Message, decode_frame, encode_frame

GOLDEN = Path(__file__).parent / "golden"
GO_ID = bytes(range(32))                   # 0, 1, ... 31
TARGET = bytes(255 - i for i in range(32))  # 255, 254, ... 224
NAMES = ["handshake", "heartbeat", "gossip", "find_node",
         "find_reply", "store", "value_reply"]


def load(name: str) -> Message:
    return decode_frame((GOLDEN / f"{name}.bin").read_bytes())


def test_handshake():
    m = load("handshake")
    assert (m.type, m.from_id, m.id, m.payload) == ("handshake", GO_ID, "handshake", None)


def test_heartbeat_has_zero_id():
    m = load("heartbeat")
    assert m.type == "heartbeat" and m.from_id == bytes(32)


def test_gossip_data_is_base64():
    p = load("gossip").payload
    assert p["ttl"] == 5
    assert base64.b64decode(p["data"]) == b"hello"
    assert p["sent_at"] == 1700000000000000000


def test_find_node_target_is_int_array():
    assert bytes(load("find_node").payload["target"]) == TARGET


def test_dht_entries_have_capitalized_keys():
    entry = load("find_reply").payload["nodes"][0]
    assert bytes(entry["ID"]) == TARGET
    assert entry["Addr"] == "node2:7000"


def test_value_reply_values_are_base64():
    values = load("value_reply").payload["values"]
    assert [base64.b64decode(v) for v in values] == [b"v1", b"v2"]


@pytest.mark.parametrize("name", NAMES)
def test_reencoding_is_byte_identical_to_go(name):
    raw = (GOLDEN / f"{name}.bin").read_bytes()
    assert encode_frame(decode_frame(raw)) == raw


def test_identity_matches_go(tmp_path):
    key_file = tmp_path / "node.key"
    shutil.copy(GOLDEN / "go_identity.bin", key_file)
    _, nid = load_or_create(key_file)
    assert nid.hex() == (GOLDEN / "node_id.txt").read_text().strip()