from __future__ import annotations # enable using class type in the class
import json
from typing import Any, List, Mapping, Optional

class VaeroStream():
    """"
    Python class used in pipeline specification files to generate a task graph
    """

    tg_start = None # pointer to first node of global task graph

    def __init__(self, ptr: Mapping[str, Any] = None):
        self._ptr = ptr # self._node is a pointer to the node at the current place of this instance

    def source(self, source_type: str, interval: int = 0) -> VaeroStream:
        node = {"type" : "source", "op" : source_type, "args" : {"interval" : interval}}

        return self._addToTaskGraph(node)
    
    def sink(self, sink_type: str) -> VaeroStream:
        node = {"type" : "sink", "op" : sink_type}

        return self._addToTaskGraph(node)

    def add(self, path: str, value: Any) -> VaeroStream:
        node = {"type" : "tn", "op" : "add", "args" : {"path" : path, "value" : value}}

        return self._addToTaskGraph(node)

    def delete(self, path: str) -> VaeroStream:
        node = {"type" : "tn", "op" : "delete", "args" : {"path" : path}}

        return self._addToTaskGraph(node)

    def rename(self, path: str, new_path: str) -> VaeroStream:
        node = {"type" : "tn", "op" : "rename", "args" : {"path" : path, "new_path" : new_path}}

        return self._addToTaskGraph(node)

    def _addToTaskGraph(self, node : Mapping[str, Any]) -> VaeroStream:
        node["next"] = []

        # first node
        if self._ptr == None:
            self._ptr = VaeroStream.tg_start = node
            return VaeroStream(node)
        # add to list at ptr location
        else:
            self._ptr["next"].append(node)
            return VaeroStream(node)

    # Convert the task graph into json and print to stdout
    @classmethod
    def start(cls):
        task_graph = VaeroStream.linkedListToArr(VaeroStream.tg_start)

        json_graph = json.dumps(task_graph)
        print(f"{json_graph}")

    # Print out the linked list for debugging purposes
    @staticmethod
    def printLinkedList(start_node : Mapping[str, Any]):
        nested_graph = json.dumps(start_node)
        print(f"{nested_graph}")

    # Convert from a linked list representation to an array that can be converted to json
    @staticmethod
    def linkedListToArr(start_node : Mapping[str, Any]) -> List[Any]:
        result = []

        node = start_node
        while node != None:
            result.append(node)
            next_list = node.pop("next", None) # remove the next field (the array keeps the order)

            if len(next_list) == 0:
                break
            elif len(next_list) == 1:
                node = next_list[0]
            else:
                post = []
                for next_node in next_list:
                    sub = VaeroStream.linkedListToArr(next_node)
                    post.append(sub)
                result.append(post)
                break
        
        return result
