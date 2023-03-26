#
# Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
#
from __future__ import annotations # enable using class type in the class
import json
import tomli
from typing import Any, List, Mapping, Optional

class Vaero():
    """"
    Python class used in pipeline specification files to generate a task graph
    """

    tg_start = None # pointer to first node of global task graph

    def __init__(self, ptr: Mapping[str, Any] = None):
        self._ptr = ptr # self._ptr is a pointer to the node at the current place of this instance

    def source(self, source_type: str, interval: int = 10, host: str = "",
                token: str = "", name: str = "", max_calls_per_period: int = 60, limit_period : int = 60,
                max_retries: int = 6, endpoint: str = "/logevent", port: int = 8080, event_breaker: str = "jsonarray",
                bucket: str = "", prefix: str = "", region: str = "") -> Vaero:

        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint

        node = {"type" : "source", "op" : source_type,
                "args" : {"interval" : interval, "host" : host, "token" : token, "name" : name,
                "max_calls_per_period" : max_calls_per_period, "limit_period" : limit_period,
                "max_retries" : max_retries, "endpoint" : endpoint, "port" : port,
                "event_breaker" : event_breaker, "bucket" : bucket, "prefix" : prefix, "region" : region}}

        return self._addToTaskGraph(node)
    
    def sink(self, sink_type: str, timestamp_key : str = "timestamp", timestamp_format : str = "RFC3339",
                filename_prefix : str = '%Y/%m/%d', filename_format : str = '%s.log',
                batch_max_bytes : int = 1_000_000, batch_max_time: int = 60 * 5,
                bucket : str = "", region : str = "") -> Vaero:
        node = {"type" : "sink", "op" : sink_type,
                "args" : {"timestamp_key" : timestamp_key, "timestamp_format" : timestamp_format,
                "filename_prefix" : filename_prefix, "filename_format" : filename_format,
                "batch_max_bytes" : batch_max_bytes, "batch_max_time" : batch_max_time,
                "bucket" : bucket, "region" : region}}

        return self._addToTaskGraph(node)

    def add(self, path: str, value: Any) -> Vaero:
        node = {"type" : "tn", "op" : "add", "args" : {"path" : path, "value" : value}}

        return self._addToTaskGraph(node)

    def delete(self, path: str) -> Vaero:
        node = {"type" : "tn", "op" : "delete", "args" : {"path" : path}}

        return self._addToTaskGraph(node)
    
    def filter_regexp(self, path: str, regexp: str) -> Vaero:
        node = {"type" : "tn", "op" :"filter_regexp", "args" : {"path" : path, "regex" : regexp}}

        return self._addToTaskGraph(node)
    
    def mask(self, path: str, regexp: str, replace_expr: str) -> Vaero:
        node = {"type" : "tn", "op" :"mask", "args" : {"path" : path, "regex" : regexp, "replace_expr" : replace_expr}}

        return self._addToTaskGraph(node)

    def parse_regexp(self, path: str, regexp: str) -> Vaero:
        node = {"type" : "tn", "op" :"parse_regexp", "args" : {"path" : path, "regex" : regexp}}

        return self._addToTaskGraph(node)

    def rename(self, path: str, new_path: str) -> Vaero:
        node = {"type" : "tn", "op" : "rename", "args" : {"path" : path, "new_path" : new_path}}

        return self._addToTaskGraph(node)
    
    def select(self, path: str) -> Vaero:
        node = {"type" : "tn", "op" : "select", "args" : {"path" : path}}

        return self._addToTaskGraph(node)

    # Apply any option to the node. Options are set in the args map.
    def option(self, arg_name: str, value: Any) -> Vaero:
        self._ptr.get("args")[arg_name] = value

        return self
    
    # Read a toml options file with key = value pairs. Set the key, value  pairs in the args map.
    # The toml file should be only key = value pairs with no tables / headers.
    def option_file(self, file_name : str) -> Vaero:
        with open(file_name, 'rb') as toml_file:
            parsed = tomli.load(toml_file)
            self._ptr.get("args").update(parsed)

        return self

    # Apply special option to run a command to get a secret
    # command will be run
    # secrets is an array of maps of {secret_name : target_argument} that will be passed on stdin to the command
    # The output of command should generate a map in format {"arg_name1" : value1, "arg_name2" : value2}
    def secret(self, command : str = "", secrets : List[str] = [], cache_time_seconds : int = 86400 * 30, timeout_seconds : int = 30) -> Vaero:
        self._ptr["secret"] = {
            "command" : command,
            "secrets" : secrets,
            "cache_time_seconds" : cache_time_seconds,
            "timeout_seconds" : timeout_seconds
        }

        return self

    def _addToTaskGraph(self, node : Mapping[str, Any]) -> Vaero:
        node["next"] = []

        if node.get("args") == None:
            node["args"] = {}

        # first node
        if self._ptr == None:
            self._ptr = Vaero.tg_start = node
            return Vaero(node)
        # add to list at ptr location
        else:
            self._ptr["next"].append(node)
            return Vaero(node)

    # Convert the task graph into json and print to stdout
    @classmethod
    def start(cls):
        task_graph = Vaero.linkedListToArr(Vaero.tg_start)

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
                    sub = Vaero.linkedListToArr(next_node)
                    post.append(sub)
                result.append(post)
                break
        
        return result
