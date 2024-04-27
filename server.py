import thriftpy2
import argparse
from thriftpy2.rpc import make_server
from chord_simulation.implement.chord_basic_query import ChordNode as ChordNodeBasicQuery
from chord_simulation.implement.chord_finger_table import ChordNode as ChordNodeFingerTable

chord_thrift = thriftpy2.load('chord_simulation/idl/chord.thrift', module_name='chord_thrift')

parser = argparse.ArgumentParser(description='server node for chord simulation.')
parser.add_argument('-t', '--task_type', type=str, default='basic_query',
                    choices=['basic_query', 'finger_table'],
                    help='simulation type:[basic_query|finger_table]')
parser.add_argument('-a', '--address', type=str, default='localhost', help='server address')
parser.add_argument('-p', '--port', type=int, help='server port')
parser.add_argument('--timeout', type=int, default=10, help='client timeout[min]')

if __name__ == '__main__':
    args = parser.parse_args()
    node = None
    if args.task_type == 'basic_query':
        node = ChordNodeBasicQuery(args.address, args.port)
    elif args.task_type == 'finger_table':
        node = ChordNodeFingerTable(args.address, args.port)

    timeout_ms = args.timeout * 60 * 1000
    server = make_server(chord_thrift.ChordNode, node, args.address, args.port, client_timeout=timeout_ms)
    server.serve()
