# ScyllaDB - Model Context Protocol

Uses Python `uv`.

```shell
docker run -dit --name mcp -p 10000:10000 -p 9042:9042 -p 19042:19042 scylladb/scylla:2025.1 --api-address 0.0.0.0
uv sync
source .venv/bin/activate
```

Sample JSON to integrate with Claude Desktop:

```json
{
    "mcpServers": {
        "ScyllaDB": {
            "command": "uv",
            "args": [
                "--directory",
                "/your/path/to/scylla-mcp",
                "run",
                "scylladb.py",
                "--contact-points",
                "127.0.0.1"
            ]
        }
    }
}
```
