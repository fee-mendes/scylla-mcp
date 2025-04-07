from typing import Any
import httpx, argparse
import socket, random
import asyncio
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("scylladb")

initial_contact_point = None
session = None
port = 10000

def _connect(contact_points):
    cluster = Cluster([contact_points])
    return cluster.connect()

def get_contact_point(contact_points, port=10000, timeout=1):
    random.shuffle(contact_points)

    for host in contact_points:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return host
        except (socket.timeout, socket.error):
            pass

    raise Exception("No available contact points found.")

async def api_get_request(url: str) -> dict[str, Any] | None:
    headers = {
        "Accept": "application/json"
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=5.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Exception in GET: {e}")
            return None

@mcp.tool()
async def query_cql(query=None) -> dict[str, Any]:
    try:
        rows = session.execute(query)
        result = [dict(row.items()) for row in rows]
        return {"result": result}
    except Exception as e:
        return {"CQL error": e}

@mcp.tool()
async def describe_ring(node=None, token=None, count=True) -> dict[str, Any]:
    ring = f"http://{initial_contact_point}:{port}/storage_service/tokens_endpoint"
    tokens_ep = await api_get_request(ring)

    token_map = {int(entry["key"]): entry["value"] for entry in tokens_ep}

    node_tokens = {}
    for t, owner in token_map.items():
        node_tokens.setdefault(owner, []).append(t)


    if token is not None:
        for t, owner in token_map.items():
            if t >= int(token):
                return {"owner": owner}
        return {"owner": list(token_map.values())[0]} # Wrap-around.

    if node is not None:
        if count:
            return {node: len(node_tokens.get(node, []))}
        return {node: node_tokens.get(node, [])}

    if count:
        return {n: len(tokens) for n, tokens in node_tokens.items()}

    return token_map


def ring_ep(ring):
    """ Returns the unique endpoints in ring """
    return list(set(ring.values()))

@mcp.tool()
async def get_status() -> dict[str, Any]:
    ring = await describe_ring(count=False)
    ep = ring_ep(ring)
    url = f"http://{initial_contact_point}:{port}/gossiper/endpoint/live"
    live = await api_get_request(url)

    url = f"http://{initial_contact_point}:{port}/gossiper/endpoint/down"
    down = await api_get_request(url)

    status = { 'up': [host for host in ep if host in live],
               'down': [host for host in ep if host in down],
             }

    return status

def main():
    global initial_contact_point
    global session
    parser = argparse.ArgumentParser(description="ScyllaDB MCP")
    parser.add_argument(
       "--contact-points",
       type=lambda s: s.split(","),
       required=True,
       help="Comma-separated list of ScyllaDB contact points"
    )

    args = parser.parse_args()
    try:
        initial_contact_point = get_contact_point(args.contact_points)
    except Exception as e:
        print(f"Error: {e}")

    #print(asyncio.run(get_status()))
    #print(asyncio.run(describe_ring(count=True)))
    #print(asyncio.run(describe_ring(count=False)))
    #print(asyncio.run(describe_ring(node='172.17.0.2', count=False)))
    #print(asyncio.run(describe_ring(node='172.17.0.2', count=True)))
    #print(asyncio.run(describe_ring(token=1)))

    session = _connect(initial_contact_point)
    session.row_factory = dict_factory
    #print(asyncio.run(query_cql('select * from ks.donotexist ')))

    mcp.run(transport='stdio')

    
if __name__ == "__main__":
    main()

