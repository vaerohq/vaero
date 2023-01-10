from vaero_stream import Vaero

vs = Vaero()

result = vs.source("random", 3) \
        .add("newfield", 42)

result2 = result.delete("severity") \
            .rename("hostname", "myhost")

result.sink("stdout", timestamp_key = "time", batch_max_time = 3)

result2.sink("s3", timestamp_key = "time", bucket = "vaero-go-test", batch_max_time = 3)

result2.rename("msg", "mymessage") \
        .sink("datadog", timestamp_key = "time", batch_max_time = 3)

result3 = result2.add("author", "Douglas Adams") \
                    .add("title", "Hitchhiker's Guide to the Galaxy")

result3.rename("severity", "myseverity") \
        .sink("elastic", timestamp_key = "time", batch_max_time = 3)

result.add("author", "Stan Lee") \
        .add("title", "Spiderman") \
        .sink("splunk", timestamp_key = "time", batch_max_time = 3)

Vaero.start()