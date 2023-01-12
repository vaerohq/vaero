from vaero.stream import Vaero

vs = Vaero()

result = vs.source("http_server", port = 8080, endpoint = "/log") \
        .add("newfield", "Hello, world!") \
        .sink("stdout") \
        .option("batch_max_time", 2)

Vaero.start()