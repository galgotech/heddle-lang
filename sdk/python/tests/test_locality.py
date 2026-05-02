import os
import socket
import array
import mmap
import pyarrow as pa
import unittest
import threading
import time
import sys

# Add the sdk path to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from heddle.core.locality import resolve_ticket
from heddle.proto.locality_pb2 import FlightTicket, RouteType

class TestLocality(unittest.TestCase):
    def test_resolve_ticket_local(self):
        # 1. Create a dummy Arrow file in /dev/shm
        shm_path = "/dev/shm/heddle-py-test"
        schema = pa.schema([('f1', pa.int32())])
        data = pa.record_batch([pa.array([1, 2, 3])], schema=schema)
        
        with open(shm_path, 'wb') as f:
            with pa.ipc.new_file(f, schema) as writer:
                writer.write_batch(data)
        
        # 2. Setup a mock UDS server that sends the FD
        socket_path = "/tmp/heddle-py-test.sock"
        if os.path.exists(socket_path):
            os.remove(socket_path)
            
        def server():
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.bind(socket_path)
            s.listen(1)
            conn, _ = s.accept()
            
            # Wait for request
            conn.recv(1024)
            
            # Send FD
            with open(shm_path, 'rb') as f:
                fd = f.fileno()
                # Send 'OK' with FD in ancdata
                conn.sendmsg([b"OK"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))])
            conn.close()
            s.close()

        t = threading.Thread(target=server)
        t.start()
        time.sleep(0.1)

        # 3. Use resolve_ticket to get the data
        ticket = FlightTicket(
            route_type=RouteType.LOCAL,
            address=f"unix://{socket_path}",
            resource_id="test-res"
        )
        
        try:
            table = resolve_ticket(ticket)
            self.assertEqual(table.num_rows, 3)
            self.assertEqual(table.to_pydict()['f1'], [1, 2, 3])
        finally:
            if os.path.exists(socket_path):
                os.remove(socket_path)
            if os.path.exists(shm_path):
                os.remove(shm_path)

if __name__ == '__main__':
    unittest.main()
