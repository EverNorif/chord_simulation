import time

from client import Client
from loguru import logger
from chord_simulation.chord.chord_base import connect_node, hash_func
from chord_simulation.chord.struct_class import Node


def build_chord_ring():
    node1 = Node(hash_func('localhost:50001'), 'localhost', 50001)
    node2 = Node(hash_func('localhost:50002'), 'localhost', 50002)
    node3 = Node(hash_func('localhost:50003'), 'localhost', 50003)

    conn_node2 = connect_node(node2)
    conn_node3 = connect_node(node3)

    logger.info("build chord ring...")
    conn_node3.join(node1)
    conn_node2.join(node1)
    time.sleep(5)


def init_data_content(client):
    logger.info("init data content...")
    for i in range(50):
        client.put(f"test-key-{i}", f"test-value-{i}")
        time.sleep(0.5)


def main():
    client = Client("localhost", 50001)
    init_data_content(client)
    print("operation format：[ put <key> <value> | get <key>]")
    while True:
        cmd = input()
        params = cmd.split(' ')

        if len(params[0]) == 0:
            continue

        if params[0] not in ['put', 'get']:
            print("> only support two operation: put/get")
            continue

        if params[0] == 'put' and len(params) == 3:
            key, value = params[1], params[2]
            status, node_id = client.put(key, value)
            h = hash_func(key)
            print(f'> hash func({key}) = {h}, put status is {status}, this value will be stored in server-{node_id}')
        elif params[0] == 'get' and len(params) == 2:
            key = params[1]
            status, key, value, node_id = client.get(key)
            h = hash_func(key)
            print(f'> hash func({key}) == {h}, find key in server-{node_id}, get status is {status}.')
            print(f'> get result: key: {key}, value: {value}')


if __name__ == '__main__':
    build_chord_ring()
    main()
