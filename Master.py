import rpyc
import uuid
import math
import random
import socket
import os
import pickle
import sys
import signal

from rpyc.utils.server import ThreadedServer


def int_handler(signal, frame):
    pickle.dump((MasterService.exposed_Master.file_table),
                open('fs.img', 'wb'))
    sys.exit(0)


class MasterService(rpyc.Service):
    class exposed_Master():
        file_table = {}
        workers = {}

        block_size = 0
        number_of_workers = 0

        def exposed_read(self, fname):
            mapping = self.__class__.file_table[fname]
            return mapping

        def exposed_write(self, dest, size):
            self.__class__.file_table[dest] = []

            self.set_block_size(size)
            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            return blocks

        def exposed_get_file_table_entry(self, fname):
            if fname in self.__class__.file_table:
                return self.__class__.file_table[fname]
            else:
                return None

        def exposed_get_block_size(self):
            return self.__class__.block_size

        def exposed_get_workers(self):
            return self.__class__.workers

        def exposed_set_number_of_workers(self, W):
            self.__class__.number_of_workers = W
            host = socket.gethostbyname(socket.gethostname())
            for i in range(1, W+1):
                self.__class__.workers[i] = (host, 8887 + i)

        def set_block_size(self, size):
            W = self.__class__.number_of_workers
            s = int(math.ceil(float(size)/W))
            self.__class__.block_size = s

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size)/self.__class__.block_size))

        def alloc_blocks(self, dest, num):
            blocks = []
            nodes_list = list(self.__class__.workers.keys())
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                nodes_id = nodes_list[i]
                blocks.append((block_uuid, nodes_id))

                self.__class__.file_table[dest].append((block_uuid, nodes_id))

            return blocks

if __name__ == "__main__":
    if os.path.isfile('fs.img'):
        MasterService.exposed_Master.file_table = pickle.load(
            open('fs.img', 'rb'))
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(MasterService, port=2100)
    t.start()
