from vaero_stream import Vaero

vs = Vaero()

result = vs.source("okta") \
        .option("interval", 10) \
        .option_file("pipelines/config/okta.toml") \
        .secret("./scripts/aws_secrets.py", [{"okta_token" : "token"}, {"okta_host" : "host"}],
                cache_time_seconds = 86400 * 30) \
        .add("newfield", 42) \
        .sink("s3", timestamp_key = "published", bucket = "vaero-go-test") \
        .option("batch_max_bytes", 2_500) \
        .option("batch_max_time", 2)

Vaero.start()