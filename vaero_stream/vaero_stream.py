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

        # first node
        if self._ptr == None:
            self._ptr = node
            VaeroStream.tg_start = node
            node["next"] = []
            return VaeroStream(node)
        # if at the end of the list, then append
        elif len(self._ptr.get("next")) == 0:
            self._ptr["next"].append(node)
            node["next"] = []
            return VaeroStream(node)
        # not at end of array, so have to add branch
        else:
            self._ptr["next"].append(node)
            node["next"] = []

            #next_op = self._arr[self._loc]

            # if the next op is not a branch op, create the branch op
            #if next_op.get("type") != "branch":
                # put the tail of the current array into a branch
            #    next_op = {"type" : "branch", "branches" : [self._arr[self._loc:]]}
            #    self._arr[self._loc] = next_op
            #    del self._arr[self._loc + 1:] # trims the array

            # add new branch
            #next_op.get("branches").append([node])

            return VaeroStream(node)

    # Convert the task graph into json and print to stdout
    @classmethod
    def start(cls):
        #VaeroStream.printLinkedList(VaeroStream.tg_start)

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

            if len(node["next"]) == 0:
                node.pop("next", None) # remove the next field (the array keeps the order)
                break
            elif len(node["next"]) == 1:
                next_list = node.pop("next", None) # remove the next field (the array keeps the order)
                node = next_list[0]
                #node = node["next"][0]
            else:
                post = []
                for next_node in node["next"]:
                    sub = VaeroStream.linkedListToArr(next_node)
                    post.append(sub)
                result.append(post)
                next_list = node.pop("next", None) # remove the next field (the array keeps the order)
                break
        
        return result
