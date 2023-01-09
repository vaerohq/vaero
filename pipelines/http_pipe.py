from vaero_stream import Vaero

vs = Vaero()

result = vs.source("http_server", port = 1000, endpoint = "myevent") \
        .add("newfield", 42) \
        .sink("stdout") \
        .option("batch_max_time", 2)

Vaero.start()