from ..chord.chord_base import BaseChordNode


class ChordNode(BaseChordNode):
    def __init__(self, address, port):
        super().__init__()
        self.address = address
        self.port = port
