import sys
import time

from ..chord.chord_base import BaseChordNode
from ..chord.chord_base import connect_node, hash_func, is_between, is_alive_node
from ..chord.struct_class import KeyValueResult, Node, KVStatus, KVStoreType
from thriftpy2.thrift import TClient
import threading


class ChordNode(BaseChordNode):
    def __init__(self, address, port):
        super().__init__()
        # set logger level
        self.logger.remove()
        self.logger.add(sys.stdout, level='DEBUG')

        self.node_id = hash_func(f'{address}:{port}')
        self.kv_store = dict()
        self.backup_kv_store = {
            'predecessor': dict(),
            'pre_predecessor': dict(),
        }

        self.self_node = Node(self.node_id, address, port)
        self.successor = self.self_node
        self.suc_successor = self.self_node
        self.predecessor = Node(self.node_id, address, port, valid=False)

        self.successor_rpc_pool_idx = 0
        self.rpc_pool.append(None)
        self.suc_successor_rpc_pool_idx = 1
        self.backup_rpc_pool.append(None)
        self.backup_rpc_pool.append(None)

        self.init_finish = False

        self.logger.info(f'node {self.node_id} listening at {address}:{port}')

    def _log_self(self):
        msg = 'now content: '
        for k, v in self.kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        self.logger.debug(msg)
        msg = 'predecessor backup: '
        for k, v in self.backup_kv_store['predecessor'].items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        self.logger.debug(msg)
        msg = 'pre_predecessor backup: '
        for k, v in self.backup_kv_store['pre_predecessor'].items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        self.logger.debug(msg)

        pre_node_id = self.predecessor.node_id if self.predecessor.valid else "null"
        self.logger.debug(f"{pre_node_id} - {self.node_id} - {self.successor.node_id}")

        self.logger.debug(f"backup list: [{self.successor.node_id}, {self.suc_successor.node_id}]")

    def lookup(self, key: str) -> KeyValueResult:
        h = hash_func(key)
        tmp_key_node = Node(h, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return self._lookup_local(key)
        else:
            conn_next_node = self._closest_preceding_node(h)
            return conn_next_node.lookup(key)

    def _lookup_local(self, key: str) -> KeyValueResult:
        result = self.kv_store.get(key, None)
        status = KVStatus.VALID if result is not None else KVStatus.NOT_FOUND
        return KeyValueResult(key, result, self.node_id, status)

    def find_successor(self, key_id: int) -> Node:
        key_id_node = Node(key_id, "", 0)
        if is_between(key_id_node, self.self_node, self.successor):
            return self.self_node
        else:
            conn_next_node = self._closest_preceding_node(key_id)
            return conn_next_node.find_successor(key_id)

    def _closest_preceding_node(self, key_id: int) -> TClient:
        return self.rpc_pool[self.successor_rpc_pool_idx]

    def put(self, key: str, value: str, backup: bool = True) -> KeyValueResult:
        h = hash_func(key)
        tmp_key_node = Node(h, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return self.__do_put(key, value, backup)
        else:
            conn_next_node = self._closest_preceding_node(h)
            return conn_next_node.put(key, value, backup)

    def __do_put(self, key: str, value: str, backup: bool) -> KeyValueResult:
        self.kv_store[key] = value
        # self node will store kv from predecessor and pre_predecessor as backup
        if backup:
            self.backup_rpc_pool[self.successor_rpc_pool_idx].backup('predecessor', key, value)
            self.backup_rpc_pool[self.suc_successor_rpc_pool_idx].backup('pre_predecessor', key, value)
        return KeyValueResult(key, value, self.node_id)

    def backup(self, backup_kv_store_key: str, key: str, value: str):
        self.backup_kv_store[backup_kv_store_key][key] = value

    def replay(self, kv_store_type: KVStoreType):
        replay_dict, replay_dict_name = None, ''
        if kv_store_type == KVStoreType.BASE:
            replay_dict = self.kv_store.copy()
            replay_dict_name = 'base kv store'
        elif kv_store_type == KVStoreType.PRE_BACKUP:
            replay_dict = self.backup_kv_store['predecessor'].copy()
            replay_dict_name = 'predecessor backup kv store'
        elif kv_store_type == KVStoreType.PRE_PRE_BACKUP:
            replay_dict = self.backup_kv_store['pre_predecessor'].copy()
            replay_dict_name = 'pre_predecessor backup kv store'

        self.logger.info(f"replay data in {replay_dict_name}")

        def put_all_data():
            time.sleep(3)  # waiting for the chord ring to stabilize
            for k, v in replay_dict.items():
                self.put(k, v, backup=True)
            self.logger.info("data replay finished.")

        # new thread to recovery data
        recover_thread = threading.Thread(target=put_all_data)
        recover_thread.start()

    def join(self, node: Node):
        conn_node = connect_node(node)
        self.successor = conn_node.find_successor(self.node_id)
        self.rpc_pool[self.successor_rpc_pool_idx] = connect_node(self.successor)
        self._fix_backup_rpc_pool()

    def notify(self, node: Node):
        # repair chord ring
        if not is_alive_node(self.predecessor, self.logger):
            self.predecessor = node
            self.replay(KVStoreType.PRE_BACKUP)
            connect_node(self.predecessor).replay(KVStoreType.BASE)
        elif not self.predecessor.valid or is_between(node, self.predecessor, self.self_node):
            self.predecessor = node

    def _stabilize(self):
        if not self.init_finish:
            self.rpc_pool[self.successor_rpc_pool_idx] = connect_node(self.successor)
            self._fix_backup_rpc_pool()
            self.init_finish = True

        conn_successor = self.rpc_pool[self.successor_rpc_pool_idx]
        x = conn_successor.get_predecessor()
        if is_between(x, self.self_node, self.successor) and is_alive_node(x, self.logger):
            self.successor = x
            self.rpc_pool[self.successor_rpc_pool_idx] = connect_node(self.successor)

        self._fix_backup_rpc_pool()
        conn_successor = self.rpc_pool[self.successor_rpc_pool_idx]
        conn_successor.notify(self.self_node)

    def _fault_detect(self):
        return self.rpc_pool[self.successor_rpc_pool_idx].heart_beat()

    def _fault_recovery(self):
        self.successor = self.suc_successor
        self.rpc_pool[self.successor_rpc_pool_idx] = connect_node(self.successor)
        self._fix_backup_rpc_pool()

    def _fix_fingers(self):
        pass

    def _check_predecessor(self):
        pass

    def get_successor(self) -> Node:
        return self.successor

    def _fix_backup_rpc_pool(self):
        """
         fix backup rpc pool when successor change.
         backup rpc pool includes successor, successor.successor
        """
        self.suc_successor = self.rpc_pool[self.successor_rpc_pool_idx].get_successor()
        self.backup_rpc_pool[self.successor_rpc_pool_idx] = self.rpc_pool[self.successor_rpc_pool_idx]
        self.backup_rpc_pool[self.suc_successor_rpc_pool_idx] = connect_node(self.suc_successor)
