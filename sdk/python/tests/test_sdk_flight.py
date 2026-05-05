import sys
from unittest.mock import MagicMock

# --- HACK TO AVOID GRPC/PYARROW CONFLICT IN THIS ENVIRONMENT ---
# We mock the 'grpc' package before it can load its C-extension,
# as it conflicts with pyarrow.flight's internal gRPC flags.
if 'grpc' not in sys.modules:
    mock_grpc = MagicMock()
    mock_grpc.__version__ = "9.9.9"
    mock_grpc.StatusCode.UNIMPLEMENTED = 12
    # Define StatusCode values used in servicer
    mock_grpc.StatusCode.SUCCESS = 0
    mock_grpc._utilities.first_version_is_lower.return_value = False
    sys.modules['grpc'] = mock_grpc
    sys.modules['grpc._utilities'] = mock_grpc._utilities

import json
import pytest
import threading
import pyarrow as pa
import pyarrow.flight as flight
from unittest.mock import MagicMock

from heddle.sdk.plugin import Plugin, PluginServicer, HeddleBusinessException
from heddle.core.table import Table, HeddleTable
from heddle.proto import worker_pb2
from heddle.proto.locality_pb2 import FlightTicket, RouteType

# 1. Mock Flight Server
class MockFlightServer(flight.FlightServerBase):
    def __init__(self, location, **kwargs):
        super(MockFlightServer, self).__init__(location, **kwargs)
        self.tables = {}

    def do_get(self, context, ticket):
        resource_id = ticket.ticket.decode('utf-8')
        if resource_id not in self.tables:
            raise flight.FlightResourceNotFound(resource_id)
        
        table = self.tables[resource_id]
        return flight.RecordBatchStream(table)

@pytest.fixture
def flight_server():
    location = "grpc://127.0.0.1:0"
    server = MockFlightServer(location)
    
    # Pre-populate with dummy data
    schema = pa.schema([('id', pa.int32()), ('val', pa.string())])
    table = pa.Table.from_batches([
        pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3]),
            pa.array(["a", "b", "c"])
        ], schema=schema)
    ])
    server.tables["test-resource"] = table
    
    # Run in thread
    def serve():
        server.serve()
    
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    
    # Give it a moment to start and get port
    # The server starts listening when serve() is called, but we need to wait for it to be ready.
    # flight.connect will fail if it's not ready.
    # Usually a small sleep is enough in tests.
    import time
    time.sleep(0.5)
    
    port = server.port
    yield f"127.0.0.1:{port}", server
    
    server.shutdown()
    thread.join(timeout=1)

@pytest.fixture
def test_plugin():
    return Plugin(namespace="test-ns")

@pytest.fixture
def servicer(test_plugin):
    return PluginServicer(test_plugin.registry)

# 2. Test Cases

def test_flight_remote_ticket_resolution(servicer, flight_server, test_plugin):
    addr, server = flight_server
    
    # Define a step that expects a Table
    @test_plugin.step(name="process_data")
    def process_data(input: HeddleTable) -> Table:
        assert isinstance(input, Table)
        assert input.num_rows == 3
        # Return something to test output path
        return input

    # Create the request with a REMOTE ticket
    ticket = FlightTicket(
        route_type=RouteType.REMOTE,
        address=f"grpc://{addr}",
        resource_id="test-resource"
    )
    
    request = worker_pb2.ExecuteStepRequest(
        step_name="process_data",
        input_ticket=ticket,
        config_json="{}"
    )
    
    context = MagicMock()
    response = servicer.ExecuteStep(request, context)
    
    assert response.status == worker_pb2.StatusCode.SUCCESS
    
    # Verify we got back valid Arrow data in output_table
    reader = pa.ipc.open_stream(response.output_table)
    out_table = reader.read_all()
    assert out_table.num_rows == 3
    assert out_table.column('val').to_pylist() == ["a", "b", "c"]

def test_strict_type_enforcement_input(test_plugin):
    # This should fail at definition time because of the TypeError in PluginRegistry.step
    with pytest.raises(TypeError) as excinfo:
        @test_plugin.step(name="invalid_input")
        def invalid_input(input: dict) -> Table:
            return None
    
    assert "input must be heddle.core.HeddleTable" in str(excinfo.value)

def test_strict_type_enforcement_output(servicer, test_plugin):
    # This bypasses the static check (no return annotation) but fails at runtime
    @test_plugin.step(name="invalid_output")
    def invalid_output(input: None):
        return {"not": "a table"}

    request = worker_pb2.ExecuteStepRequest(
        step_name="invalid_output",
        config_json="{}"
    )
    
    context = MagicMock()
    response = servicer.ExecuteStep(request, context)
    
    assert response.status == worker_pb2.StatusCode.BUSINESS_ERROR
    assert "must return a Table object" in response.error_message
