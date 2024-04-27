import traceback
from loguru import logger

from chord_simulation.chord.struct_class import KeyValueResult, KVStatus
from chord_simulation.chord.chord_base import connect_address
from thriftpy2.transport import TTransportException


class Client:
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.node = connect_address(address, port)

    def __do_with_reconnect(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TTransportException as e:
            if e.type == TTransportException.END_OF_FILE:
                self.node = connect_address(self.address, self.port)
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(e)
                    logger.warning(traceback.format_exc())
        except BrokenPipeError:
            self.node = connect_address(self.address, self.port)
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(e)
            logger.warning(traceback.format_exc())

    def put(self, key: str, value: str):
        """
         return put_status: bool and put_node_position: int
        """
        return self.__do_with_reconnect(self.__do_put, key, value)

    def __do_put(self, key: str, value: str):
        put_res: KeyValueResult = self.node.put(key, value)
        put_status = True if put_res.status == KVStatus.VALID else False
        return put_status, put_res.node_id

    def get(self, key: str):
        """
         return get_status: str, get_result: k-v, get_node_position: int
        """
        return self.__do_with_reconnect(self.__do_get, key)

    def __do_get(self, key: str):
        get_res: KeyValueResult = self.node.lookup(key)
        status = get_res.status
        if status == KVStatus.VALID:
            status = 'valid'
        elif status == KVStatus.NOT_FOUND:
            status = 'not_found'
        else:
            status = 'else status'
        return status, get_res.key, get_res.value, get_res.node_id
