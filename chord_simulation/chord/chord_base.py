import hashlib
import thriftpy2
import os
import threading
import traceback
from thriftpy2.rpc import make_client
from .struct_class import KeyValueResult, Node, M
from loguru import logger

current_dir = os.path.dirname(os.path.abspath(__file__))
thrift_path = os.path.join(current_dir, '../idl/chord.thrift')
chord_thrift = thriftpy2.load(thrift_path, module_name='chord_thrift')


class BaseChordNode:
    """
     Base Interface of Chord Node
    """

    def __init__(self):
        self.logger = logger
        self._interval = 1
        self.__timer = threading.Timer(self._interval, self.run_periodically)
        self.__timer.start()
        self.predecessor = None

    def lookup(self, key: str) -> KeyValueResult:
        raise NotImplementedError

    def _lookup_local(self, key: str) -> KeyValueResult:
        raise NotImplementedError

    def find_successor(self, key_id: int) -> Node:
        raise NotImplementedError

    def _closet_preceding_node(self, key_id: int) -> Node:
        raise NotImplementedError

    def put(self, key: str, value: str) -> KeyValueResult:
        raise NotImplementedError

    def join(self, node: Node):
        raise NotImplementedError

    def _stabilize(self):
        raise NotImplementedError

    def notify(self, node: Node):
        raise NotImplementedError

    def _fix_fingers(self):
        raise NotImplementedError

    def _check_predecessor(self):
        raise NotImplementedError

    def get_predecessor(self) -> Node:
        return self.predecessor

    def _log_self(self):
        raise NotImplementedError

    def run_periodically(self):
        try:
            self._stabilize()
            self._fix_fingers()
            self._check_predecessor()
            self._log_self()
        except Exception as e:
            self.logger.warning(e)
            self.logger.warning(traceback.format_exc())

        self.__timer = threading.Timer(self._interval, self.run_periodically)
        self.__timer.start()


def hash_func(intput_str) -> int:
    """
     sha1 hash function
    """
    sha1 = hashlib.sha1()
    sha1.update(str(intput_str).encode('utf-8'))
    hash_hex = sha1.hexdigest()
    hash_int = int(hash_hex, 16)
    hash_int = hash_int % (2 ** M)
    return hash_int


def connect_address(address, port):
    """
     connect address:port if it is online, else None
    """
    try:
        node = make_client(chord_thrift.ChordNode, address, port)
        return node
    except Exception as e:
        logger.warning(e)
        logger.warning(traceback.format_exc())
        return None


def connect_node(node: Node):
    """
     connect node if node is online, else None
    """
    return connect_address(node.address, node.port)


def is_between(node: Node, node1: Node, node2: Node):
    """
     judge if node is on the clockwise arc(node1->node2), not include the endpoints
    """
    start_node_id, end_node_id = node1.node_id, node2.node_id
    if start_node_id < end_node_id:
        return start_node_id < node.node_id < end_node_id
    elif start_node_id == end_node_id:
        return node.node_id != start_node_id
    else:
        return node.node_id > start_node_id or node.node_id < end_node_id
