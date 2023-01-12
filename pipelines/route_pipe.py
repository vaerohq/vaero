from vaero.stream import Vaero

result = Vaero().source("random", 3) \
        .rename("hostname", "host")

result.sink("stdout", batch_max_time = 5)

result.add("newfield", "Hello, world!") \
        .sink("stdout", batch_max_time = 3)

Vaero.start()