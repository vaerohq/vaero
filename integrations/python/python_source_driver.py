#
# Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
#
import getopt, json, sys
from integrations.python.source_okta import OktaSource

def main(argv):
    opts, args = getopt.getopt(argv, "", ["op=", "host=", "token=",
        "name=", "max_calls_per_period=", "limit_period=", "max_retries="])

    source = None
    flags = {}

    # Set flags
    for opt, arg in opts:
        flags[opt] = arg
    
    interval = flags.get("--interval")
    host = flags.get("--host")
    token = flags.get("--token")
    name = flags.get("--name")
    max_calls_per_period = int(flags.get("--max_calls_per_period", "30"))
    limit_period = int(flags.get("--limit_period", "60"))
    max_retries = int(flags.get("--max_retries", "5"))

    # Set source
    if(flags["--op"] == "okta"):
        source = OktaSource(interval, host, token, name, max_calls_per_period, limit_period, max_retries)

    # Read from source and dump events as json
    event_list = source.read()
    json_list = json.dumps(event_list)
    print("__Python Source Driver Output__")
    print(f"{json_list}")
    print("__End Python Source Driver Output__")

if __name__ == "__main__":
    main(sys.argv[1:])