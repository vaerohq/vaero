from vaero.stream import Vaero

vs = Vaero()

result = vs.source("random", 3) \
        .add("newfield", 42)

result2 = result.delete("severity") \
            .rename("hostname", "myhost")

result.sink("stdout", batch_max_time = 3)

result2.sink("stdout", batch_max_time = 3)

result2.rename("msg", "mymessage") \
        .sink("datadog", batch_max_time = 3)

result3 = result2.add("author", "Douglas Adams") \
                    .add("title", "Hitchhiker's Guide to the Galaxy")

result3.rename("severity", "myseverity") \
        .sink("elastic", batch_max_time = 3)

result.add("author", "Stan Lee") \
        .add("title", "Spiderman") \
        .sink("splunk", batch_max_time = 3)

Vaero.start()