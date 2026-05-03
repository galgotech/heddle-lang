import uuid
import json
import logging
import traceback
from typing import Callable, Dict, Any, Optional

import grpc

from heddle.proto import worker_pb2, worker_pb2_grpc
from heddle.core.table import Table
from heddle.core.locality import resolve_ticket

class HeddleBusinessException(Exception):
    pass

class PluginRegistry:
    def __init__(self):
        self.resources: Dict[str, Callable] = {}
        self.steps: Dict[str, Dict[str, Any]] = {}
        self.resource_instances: Dict[str, Any] = {}

    def resource(self, name: str):
        def decorator(func: Callable):
            self.resources[name] = func
            return func
        return decorator

    def step(self, name: str, resource: Optional[str] = None):
        def decorator(func: Callable):
            # Semantic enforcement: Inputs and Outputs MUST be Table
            annotations = func.__annotations__
            if 'input' in annotations and annotations['input'] != Table and annotations['input'] != type(None):
                 raise TypeError(f"Step '{name}' input must be heddle.core.Table, got {annotations['input']}")
            
            # Note: Python's return type annotation is 'return'
            if 'return' in annotations and annotations['return'] != Table and annotations['return'] != type(None):
                 raise TypeError(f"Step '{name}' return type must be heddle.core.Table, got {annotations['return']}")

            self.steps[name] = {
                "func": func,
                "resource": resource
            }
            return func
        return decorator

plugin = PluginRegistry()

class PluginServicer(worker_pb2_grpc.PluginServiceServicer):
    def __init__(self, registry: PluginRegistry):
        self.registry = registry

    def InitResource(self, request, context):
        try:
            if request.resource_name not in self.registry.resources:
                raise HeddleBusinessException(f"Resource '{request.resource_name}' not found in registry")

            func = self.registry.resources[request.resource_name]
            config_class = func.__annotations__.get('config')
            if not config_class:
                raise HeddleBusinessException("Resource function must have a 'config' annotation")

            config_dict = json.loads(request.config_json or "{}")
            config_instance = config_class(**config_dict)

            instance = func(config=config_instance)
            resource_id = str(uuid.uuid4())
            self.registry.resource_instances[resource_id] = instance

            return worker_pb2.InitResourceResponse(
                status=worker_pb2.StatusCode.SUCCESS,
                resource_id=resource_id
            )
        except HeddleBusinessException as e:
            return worker_pb2.InitResourceResponse(
                status=worker_pb2.StatusCode.BUSINESS_ERROR,
                error_message=str(e)
            )
        except Exception as e:
            logging.error(f"Fatal error initializing resource: {e}\n{traceback.format_exc()}")
            return worker_pb2.InitResourceResponse(
                status=worker_pb2.StatusCode.FATAL_ERROR,
                error_message=f"{str(e)}\n{traceback.format_exc()}"
            )

    def ExecuteStep(self, request, context):
        try:
            if request.step_name not in self.registry.steps:
                raise HeddleBusinessException(f"Step '{request.step_name}' not found in registry")

            step_info = self.registry.steps[request.step_name]
            func = step_info["func"]
            resource_name = step_info["resource"]

            config_class = func.__annotations__.get('config')
            input_class = func.__annotations__.get('input')

            config_dict = json.loads(request.config_json or "{}")

            kwargs = {}
            if config_class:
                if resource_name:
                    if not request.resource_id:
                        raise HeddleBusinessException(f"Step requires resource '{resource_name}' but no resource_id provided")
                    if request.resource_id not in self.registry.resource_instances:
                        raise HeddleBusinessException(f"Resource instance '{request.resource_id}' not found")

                    resource_instance = self.registry.resource_instances[request.resource_id]
                    # We inject the resource into the config, but config is supposed to be StepConfig,
                    # let's assume the user doesn't expect the resource instance in the config instantiation
                    # Actually wait, the problem says "retrieve the live resource instance from memory, attach it to the config parameter, and invoke the step function."
                    # Let's instantiate config, then attach resource.
                    config_instance = config_class(**config_dict)
                    setattr(config_instance, 'resource', resource_instance) # Attaching it directly
                    kwargs['config'] = config_instance
                else:
                    kwargs['config'] = config_class(**config_dict)

            if input_class and input_class != type(None):
                if request.HasField("input_ticket"):
                    # Fast-Path or Network-Path resolution
                    pa_table = resolve_ticket(request.input_ticket)
                    kwargs['input'] = Table(pa_table)
                elif request.input_table:
                    # Legacy byte-buffer fallback
                    import pyarrow as pa
                    reader = pa.ipc.open_stream(request.input_table)
                    kwargs['input'] = Table(reader.read_all())
                else:
                    kwargs['input'] = None
            else:
                kwargs['input'] = None

            # If function expects Table as input, we would normally construct it from bytes.
            # Here we just pass None or an empty instance for now.

            result = func(**kwargs)

            # Strict Output Enforcement
            output_bytes = b""
            if result is not None:
                if not isinstance(result, Table):
                    raise HeddleBusinessException(f"Step '{request.step_name}' must return a Table object, got {type(result)}")
                output_bytes = result.to_bytes()

            return worker_pb2.ExecuteStepResponse(
                status=worker_pb2.StatusCode.SUCCESS,
                output_table=output_bytes
            )
        except HeddleBusinessException as e:
            return worker_pb2.ExecuteStepResponse(
                status=worker_pb2.StatusCode.BUSINESS_ERROR,
                error_message=str(e)
            )
        except Exception as e:
            logging.error(f"Fatal error executing step: {e}\n{traceback.format_exc()}")
            return worker_pb2.ExecuteStepResponse(
                status=worker_pb2.StatusCode.FATAL_ERROR,
                error_message=f"{str(e)}\n{traceback.format_exc()}"
            )
