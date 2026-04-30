import json
import pytest
from unittest.mock import MagicMock

from heddle.sdk.plugin import plugin, PluginServicer, HeddleBusinessException
from heddle.core.resource import ResourceConfig, Resource
from heddle.core.step import StepConfig
from heddle.core.table import Table
from heddle.proto import worker_pb2

class ResourceConfigHttp(ResourceConfig):
    host: str = "127.0.0.1"
    port: int = 8080

class HttpResource(Resource):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.is_started = False

    def start(self):
        self.is_started = True

class StepConfigRoute(StepConfig[ResourceConfigHttp]):
    path: str
    method: str

class HttpRequest(Table):
    method: str
    path: str

@plugin.resource(name="http_server")
def server(config: ResourceConfigHttp) -> HttpResource:
    res = HttpResource(config.host, config.port)
    res.start()
    return res

@plugin.step(name="http_route", resource="http_server")
def route(config: StepConfigRoute, input: None) -> HttpRequest:
    if not hasattr(config, 'resource'):
        raise HeddleBusinessException("Missing injected resource")
    if not getattr(config, 'resource').is_started:
        raise HeddleBusinessException("Resource not started")

    # Standard python exception to simulate a crash
    if config.path == "/crash":
        raise ValueError("Oops, a crash!")

    return HttpRequest(method=config.method, path=config.path)

@pytest.fixture
def servicer():
    return PluginServicer(plugin)

def test_init_resource(servicer):
    request = worker_pb2.InitResourceRequest(
        resource_name="http_server",
        config_json=json.dumps({"host": "0.0.0.0", "port": 9090})
    )
    context = MagicMock()

    response = servicer.InitResource(request, context)

    assert response.status == worker_pb2.StatusCode.SUCCESS
    assert response.resource_id != ""
    assert response.resource_id in plugin.resource_instances

    instance = plugin.resource_instances[response.resource_id]
    assert isinstance(instance, HttpResource)
    assert instance.host == "0.0.0.0"
    assert instance.port == 9090
    assert instance.is_started == True

    return response.resource_id

def test_execute_step(servicer):
    # First init the resource
    resource_id = test_init_resource(servicer)

    request = worker_pb2.ExecuteStepRequest(
        step_name="http_route",
        resource_id=resource_id,
        config_json=json.dumps({"path": "/hello", "method": "GET"}),
        input_table=b""
    )
    context = MagicMock()

    response = servicer.ExecuteStep(request, context)
    assert response.status == worker_pb2.StatusCode.SUCCESS

def test_execute_step_business_error(servicer):
    resource_id = test_init_resource(servicer)

    request = worker_pb2.ExecuteStepRequest(
        step_name="nonexistent_step",
        resource_id=resource_id,
        config_json=json.dumps({"path": "/hello", "method": "GET"}),
        input_table=b""
    )
    context = MagicMock()

    response = servicer.ExecuteStep(request, context)
    assert response.status == worker_pb2.StatusCode.BUSINESS_ERROR
    assert "not found in registry" in response.error_message

def test_execute_step_fatal_error(servicer):
    resource_id = test_init_resource(servicer)

    request = worker_pb2.ExecuteStepRequest(
        step_name="http_route",
        resource_id=resource_id,
        config_json=json.dumps({"path": "/crash", "method": "GET"}),
        input_table=b""
    )
    context = MagicMock()

    response = servicer.ExecuteStep(request, context)
    assert response.status == worker_pb2.StatusCode.FATAL_ERROR
    assert "Oops, a crash!" in response.error_message
