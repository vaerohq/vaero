from __future__ import annotations # enable using class type in the class
import json
from typing import Any, List, Mapping, Optional

class VaeroStream():
    """"
    Python class used in pipeline specification files to generate a task graph
    """

    task_graph = []

    def __init__(self, arr: Optional(List[Any]) = None, loc: int = 0):
        self._arr = arr if arr != None else VaeroStream.task_graph # list where next node will be added
        self._loc = loc # index in self._arr where next node will be added

    def source(self, source_type: str, interval: int = 0) -> VaeroStream:
        node = {"type" : "source", "op" : source_type, "interval" : interval}

        return self._addToTaskGraph(node)
    
    def sink(self, sink_type: str) -> VaeroStream:
        node = {"type" : "sink", "op" : sink_type}

        return self._addToTaskGraph(node)

    def add(self, path: str, value: Any) -> VaeroStream:
        node = {"type" : "tn", "op" : "add", "path" : path, "value" : value}

        return self._addToTaskGraph(node)

    def delete(self, path: str) -> VaeroStream:
        node = {"type" : "tn", "op" : "delete", "path" : path}

        return self._addToTaskGraph(node)

    def rename(self, path: str, new_path: str) -> VaeroStream:
        node = {"type" : "tn", "op" : "rename", "path" : path, "new_path" : new_path}

        return self._addToTaskGraph(node)

    def _addToTaskGraph(self, node : Mapping[str, Any]) -> VaeroStream:

        # if at the end of the array, then append
        if self._loc == len(self._arr):
            self._arr.append(node)
            return VaeroStream(self._arr, self._loc + 1)
        # not at end of array, so have to split next node
        else:
            next_op = self._arr[self._loc]

            # if the next op is not a branch op, create the branch op
            if next_op.get("type") != "branch":
                # put the tail of the current array into a branch
                next_op = {"type" : "branch", "branches" : [self._arr[self._loc:]]}
                self._arr[self._loc] = next_op
                del self._arr[self._loc + 1:] # trims the array

            # add new branch
            next_op.get("branches").append([node])

            return VaeroStream(next_op.get("branches")[-1], 1)

    @classmethod
    def start(cls):
        json_graph = json.dumps(VaeroStream.task_graph)
        print(f"{json_graph}")