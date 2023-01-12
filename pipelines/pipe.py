from vaero.stream import Vaero

Vaero().source("random", 3) \
        .rename("hostname", "host") \
        .add("newfield", "Hello, world!") \
        .sink("stdout", batch_max_time = 3)

Vaero.start()