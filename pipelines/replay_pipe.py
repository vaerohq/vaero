from vaero.stream import Vaero

Vaero().source("s3", 1200, bucket = "vaero-go-test", prefix = "2023/01/02") \
        .add("replay", True) \
        .sink("s3", timestamp_key = "published", bucket = "vaero-go-test", filename_prefix = "replay/%Y/%m/%d") \
        .option("batch_max_bytes", 50_000) \
        .option("batch_max_time", 10)

Vaero.start()