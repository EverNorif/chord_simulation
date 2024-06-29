from __future__ import annotations

import thriftpy2
import os

M = 16

current_dir = os.path.dirname(os.path.abspath(__file__))
thrift_path = os.path.join(current_dir, '../idl/chord.thrift')
chord_thrift = thriftpy2.load(thrift_path, module_name='chord_thrift')


class KVStatus(chord_thrift.KVStatus):
    VALID = chord_thrift.KVStatus.VALID
    NOT_FOUND = chord_thrift.KVStatus.NOT_FOUND


class KVStoreType(chord_thrift.KVStoreType):
    BASE = chord_thrift.KVStoreType.BASE
    PRE_BACKUP = chord_thrift.KVStoreType.PRE_BACKUP
    PRE_PRE_BACKUP = chord_thrift.KVStoreType.PRE_PRE_BACKUP


class KeyValueResult(chord_thrift.KeyValueResult):
    def __init__(self, key: str, value: str, node_id: int, status: KVStatus = KVStatus.VALID):
        super().__init__(key, value, node_id, status)


class Node(chord_thrift.Node):
    def __init__(self, node_id: int, address: str, port: int, valid: bool = True):
        super().__init__(node_id, address, port, valid)
