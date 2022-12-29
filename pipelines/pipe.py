from vaero_stream.vaero_stream import VaeroStream

vs = VaeroStream()

result = vs.source("okta", 10) \
        .add("newfield", 42)

result2 = result.delete("legacyEvent") \
            .rename("actor", "user")

result.sink("stdout")

result2.sink("s3")

result2.rename("client", "device") \
        .sink("datadog")

result3 = result2.add("author", "Douglas Adams") \
                    .add("title", "Hitchhiker's Guide to the Galaxy")

result3.rename("location", "geo") \
        .sink("elastic")

result.add("author", "Stan Lee") \
        .add("title", "Spiderman") \
        .sink("splunk")

VaeroStream.start()