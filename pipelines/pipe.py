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

VaeroStream.start()