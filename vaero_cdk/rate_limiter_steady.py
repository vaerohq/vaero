#
# Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
#
import time

def rate_limit(increment: float, last_call: float) -> float:
    """
    Sleeps until current time >= increment + last_call
    Returns current time to be stored as new last_call value
    """
    current_time = time.time()

    while current_time < increment + last_call:
        delta = last_call + increment - current_time
        print(f"Sleep for {delta}: {last_call} + {increment} - {current_time}", flush=True)
        time.sleep(delta)
        print(f"Sleep over for {delta}: {last_call} + {increment} - {current_time}", flush=True)
        print(f"Time slept: {time.time() - current_time}", flush=True)
        current_time = time.time()

    return current_time
    


