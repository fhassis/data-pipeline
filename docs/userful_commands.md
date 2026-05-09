# Useful Commands

## uv setup and useful commands

Initialize the project:

```bash
uv init --lib packages/shared
uv init --app packages/workers

```

Add the following to the `pyproject.toml` to create a workspace:

```toml
[tool.uv.workspace]
members = ["packages/*"]
```

Create the sub-projects:

> **NOTE**: using `--lib` as it creates the project with a _src_ folder layout.

```bash
uv init --lib packages/shared
uv init --lib packages/workers
```

Add dependencies in the sub-projects:

```bash
cd packages/shared
uv add msgspec
```

Bind the projects (_shared_ into _workers_):

```bash
cd packages/workers
uv add --workspace shared
```

## Pretty printing logs in development

As we are using structured logging in json, to see them nicely formatted in the terminal during development we can use [kelora](https://www.kelora.dev/).

Install it in your machine:

```bash
curl -LO https://github.com/dloss/kelora/releases/latest/download/kelora-x86_64-unknown-linux-musl.tar.gz
tar xzf kelora-x86_64-unknown-linux-musl.tar.gz
sudo mv kelora /usr/local/bin/
```

Now you can do this anywhere:

```bash
uv run --package workers python -u -m workers.main | kelora
```
