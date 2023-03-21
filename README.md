<div align="center">

<h1 align="center">
  <img width="600" src="docs/images/logo-no-background.svg" alt="Vaero Logo">
</h1>

[![Go](https://img.shields.io/badge/built%20with-go-lightblue)](https://go.dev/) [![Contributions](https://img.shields.io/badge/contributions-welcome-orange)](https://github.com/vaerohq/vaero/issues)

</div>

View the [documentation][docs.intro] and [quickstart][docs.quickstart] guide online for usage information.

## What is Vaero?

Vaero is a _programmable_ high-performance data pipeline for log data. Collect, transform, and route your log data from any log source to any destination. Vaero helps you save on log storage costs, send data to different systems of analysis, normalize data to different formats, enrich data with more information, and control logs from all your vendors in one place.

### Principles
- **Easy to program** \- Code your log pipelines in Python. No need to learn a config file format or new language.
- **Fast** \- Log pipelines are executed entirely in Go, with performance in mind.
- **Building blocks** \- Built-in transforms and source and destination integrations, so you don't have to reinvent the wheel.

### Use Cases
- Reduce log storage costs by filtering and routing logs
- Centralize log management instead of configuring multiple sources
- Adjust one system when you change vendors, not all your sources
- Normalize log data between systems
- Mask PII before storage
- Improve overall log ETL performance

## Code Log Pipelines in Python

```python
from vaero.stream import Vaero

result = Vaero().source("http_server", port = 8080, endpoint = "/log") \
        .rename("hostname", "host") \
        .add("newfield", "Hello, world!") \
        .sink("s3")

Vaero.start()
```

Vaero's Python syntax is modern and easy to use. Log pipelines are executed with high-performance Go code.

## [Documentation][docs.intro]

### Getting Started
- [**Installation**][docs.install]
- [**Quickstart**][docs.quickstart]

### Pipelines
- [**Creating Pipelines**][docs.pipelines]
- [**More Examples**][docs.examples]

### Deployment
- [**Deploying Vaero**][docs.deployment]

### Adminstration
- [**CLI**][docs.cli]

### Reference
- [**Sources**][docs.sources] - [HTTP endpoint][docs.sources.http_server], [Okta][docs.sources.okta], and more.
- [**Transforms**][docs.transforms] - [add][docs.transforms.add], [delete][docs.transforms.delete], [rename][docs.transforms.rename], and more.
- [**Routes**][docs.routes]
- [**Sinks**][docs.sinks] - [S3][docs.sinks.s3], [Stdout][docs.sinks.stdout], and more.
- [**Modifiers**][docs.modifiers] - [Options][docs.modifiers.option], [Option File][docs.modifiers.option_file], [Secrets Manager][docs.modifiers.secret], and more.
- [**Path Syntax**][docs.path_syntax]

### Configuration
- [**Global Settings**][docs.global_settings]

---
Built and supported by [Vaero][vaero.home]

[docs.cli]:https://docs.vaero.co/docs/cli
[docs.community]: https://discord.gg/GwxpVyx68X
[docs.deployment]: https://docs.vaero.co/docs/deploying-vaero
[docs.examples]: https://docs.vaero.co/docs/examples
[docs.global_settings]: https://docs.vaero.co/docs/global-settings
[docs.install]: https://docs.vaero.co/docs/installation
[docs.intro]: https://docs.vaero.co/docs/what-is-vaero
[docs.modifiers]: https://docs.vaero.co/docs/modifiers
[docs.modifiers.option]: https://docs.vaero.co/docs/option
[docs.modifiers.option_file]: https://docs.vaero.co/docs/option_file
[docs.modifiers.secret]: https://docs.vaero.co/docs/secret
[docs.path_syntax]: https://docs.vaero.co/docs/path-syntax
[docs.pipelines]: https://docs.vaero.co/docs/pipelines
[docs.quickstart]: https://docs.vaero.co/docs/quickstart
[docs.sources]: https://docs.vaero.co/docs/sources
[docs.sources.http_server]: https://docs.vaero.co/docs/http-server
[docs.sources.okta]: https://docs.vaero.co/docs/okta
[docs.transforms]: https://docs.vaero.co/docs/transforms
[docs.transforms.add]: https://docs.vaero.co/docs/add
[docs.transforms.delete]: https://docs.vaero.co/docs/delete
[docs.transforms.rename]: https://docs.vaero.co/docs/rename
[docs.routes]: https://docs.vaero.co/docs/routing
[docs.sinks]: https://docs.vaero.co/docs/sinks
[docs.sinks.s3]: https://docs.vaero.co/docs/s3
[docs.sinks.stdout]: https://docs.vaero.co/docs/stdout
[vaero.home]: https://www.vaero.co