import sys
import os

# Add SDK to path
sys.path.append(os.path.abspath("."))

from heddle.sdk import plugin
from heddle.core.table import HeddleTable

class MyTable(HeddleTable):
    def __init__(self, record=None):
        self._record = record
    def native(self):
        return self._record
    def release(self):
        pass

@plugin.step(name="test")
def my_step(config, input_table: MyTable) -> MyTable:
    return input_table

print("Checking valid step...")
# This should NOT raise TypeError if used with valid types
# Note: the decorator checks types during function call in some implementations, 
# but my implementation checks them at decoration time for return/input types.

try:
    @plugin.step(name="invalid")
    def invalid_step(config, input_table: int) -> int:
        return input_table
    print("FAILED: Invalid step decoration did NOT raise TypeError")
except TypeError as e:
    print(f"SUCCESS: Caught expected TypeError: {e}")

try:
    @plugin.step(name="invalid_return")
    def invalid_return_step(config, input_table: MyTable) -> int:
        return 0
    print("FAILED: Invalid return type decoration did NOT raise TypeError")
except TypeError as e:
    print(f"SUCCESS: Caught expected TypeError: {e}")
