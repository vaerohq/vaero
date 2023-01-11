from vaero.stream import Vaero

vs = Vaero()

result = vs.source("okta") \
        .option("interval", 10) \
        .option_file("pipelines/config/okta.toml") \
        .secret("./scripts/aws_secrets.py", [{"okta_token" : "token"}, {"okta_host" : "host"}],
                cache_time_seconds = 2) \
        .add("newfield", 42) \
        .sink("s3", timestamp_key = "published", bucket = "vaero-go-test") \
        .option("batch_max_bytes", 10_000) \
        .option("batch_max_time", 3)

Vaero.start()