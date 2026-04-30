from typing import Generic, TypeVar, Optional, Any
from pydantic import BaseModel, Field

from heddle.core.resource import ResourceConfig

R = TypeVar("R", bound=ResourceConfig)

class StepConfig(BaseModel, Generic[R]):
    resource: Optional[Any] = Field(default=None, exclude=True)
