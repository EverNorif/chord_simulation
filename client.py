from chord_simulation.chord.struct_class import KeyValueResult, KVStatus
from chord_simulation.chord.chord_base import connect_address


class Client:
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.node = connect_address(address, port)

    def put(self, key: str, value: str):
        """
         return put_status: bool and put_node_position: int
        """
        put_res: KeyValueResult = connect_address(self.address, self.port).put(key, value)
        put_status = True if put_res.status == KVStatus.VALID else False
        return put_status, put_res.node_id

    def get(self, key: str):
        """
         return get_status: str, get_result: k-v, get_node_position: int
        """
        get_res: KeyValueResult = connect_address(self.address, self.port).lookup(key)
        status = get_res.status
        if status == KVStatus.VALID:
            status = 'valid'
        elif status == KVStatus.NOT_FOUND:
            status = 'not_found'
        else:
            status = 'else status'
        return status, get_res.key, get_res.value, get_res.node_id
