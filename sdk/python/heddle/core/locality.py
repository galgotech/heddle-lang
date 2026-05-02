import socket
import array
import mmap
import os
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc
from heddle.proto.locality_pb2 import FlightTicket, RouteType

def resolve_ticket(ticket: FlightTicket) -> pa.Table:
    """
    Resolves a FlightTicket to a PyArrow Table using either the Fast-Path (LOCAL)
    or the Network-Path (REMOTE).
    """
    if ticket.route_type == RouteType.LOCAL:
        return _resolve_local(ticket)
    elif ticket.route_type == RouteType.REMOTE:
        return _resolve_remote(ticket)
    else:
        raise ValueError(f"Unknown route type: {ticket.route_type}")

def _resolve_local(ticket: FlightTicket) -> pa.Table:
    # 1. Connect to UDS
    addr = ticket.address
    if addr.startswith("unix://"):
        addr = addr[len("unix://"):]
    
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(addr)
        
        # 2. Request resource
        sock.sendall(ticket.resource_id.encode('utf-8'))
        
        # 3. Receive FD via SCM_RIGHTS
        # We expect 'OK' or an error message in the data part
        msg, ancdata, flags, addr = sock.recvmsg(1024, socket.CMSG_LEN(array.array('i').itemsize))
        
        if not msg.startswith(b"OK"):
            raise Exception(f"Failed to receive FD: {msg.decode('utf-8')}")
            
        fd = None
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if (cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS):
                fd = array.array('i', cmsg_data)[0]
                break
        
        if fd is None:
            raise Exception("No FD received from UDS server")
            
        # 4. mmap the FD and open as Arrow stream/file
        # Note: We use the fd to create a memory-mapped buffer
        size = os.fstat(fd).st_size
        mm = mmap.mmap(fd, size, access=mmap.ACCESS_READ)
        
        # Arrow can open from a buffer
        reader = ipc.open_file(mm)
        table = reader.read_all()
        
        # Cleanup
        os.close(fd)
        return table
        
    finally:
        sock.close()

def _resolve_remote(ticket: FlightTicket) -> pa.Table:
    # 1. Connect to peer
    addr = ticket.address
    if addr.startswith("grpc://"):
        addr = addr[len("grpc://"):]
        
    client = flight.connect(f"grpc://{addr}")
    
    # 2. Issue DoGet
    # The ticket ID is the resource_id
    reader = client.do_get(flight.Ticket(ticket.resource_id.encode('utf-8')))
    
    # 3. Read all batches into a table
    return reader.read_all()
