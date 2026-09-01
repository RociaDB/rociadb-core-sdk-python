# RociaDB Python SDK

Typed, async Python client for the RociaDB document, graph, file, and tenant
gRPC services. It covers all 22 RPCs exposed by the packaged protobuf and
adds pagination helpers, JSON encoding/decoding, bounded batch operations,
OAuth2 client-credentials auth with automatic background token refresh, and
ergonomic file streaming, so callers work with plain Python values instead of
hand-building protobuf messages.

## Contents

- [What the SDK Manages](#what-the-sdk-manages)
- [Requirements and Installation](#requirements-and-installation)
- [Connecting](#connecting)
- [Authentication](#authentication)
- [Pagination](#pagination)
- [Documents](#documents)
- [Graph](#graph)
- [Files](#files)
- [Tenants](#tenants)
- [Idempotence and Retries](#idempotence-and-retries)
- [Error Handling](#error-handling)
- [API Coverage](#api-coverage)
- [Parity with the Other SDKs](#parity-with-the-other-sdks)
- [Development and Testing](#development-and-testing)

## What the SDK Manages

RociaDB organizes data around four identifiers:

- A `tenant_id` segments data by customer or workspace. Every RPC except
  `list_tenants` requires one.
- A document `collection` groups JSON documents addressed by `document_id`.
- A `graph` groups nodes and directed edges. Node IDs conventionally use
  `label:id`, for example `product:sku-123`.
- A file `bucket` groups binary objects addressed by `file_id`.

Document, node, and edge payloads are plain JSON-serializable Python values
(`dict`, `list`, `str`, `int`, `float`, `bool`, `None`). Read methods that
decode a payload accept an optional `decoder` keyword argument to turn the
raw JSON value into a caller-supplied type; the SDK itself performs no
runtime schema validation beyond "is this valid JSON".

**`tenant_id` is not a security boundary.** It is not derived from the
caller's identity, so any authenticated client can address any `tenant_id` —
it exists to segment data for application-level purposes, not to isolate
customers from each other at the protocol level. Per-user or per-customer
authorization is the calling application's responsibility; see
[Authentication](#authentication) for what the server does check.

## Requirements and Installation

- Python 3.10 through 3.14
- A reachable RociaDB gRPC endpoint
- OAuth2 client credentials, unless authentication is explicitly disabled

```bash
pip install rociadb-sdk
```

or, with [uv](https://docs.astral.sh/uv/):

```bash
uv add rociadb-sdk
```

The only runtime dependencies are `grpcio` and `protobuf` — the OAuth2 token
exchange is done with `urllib.request` from the standard library, not a
third-party HTTP client. The SDK is asynchronous, built on `grpc.aio`, and
ships full inline type annotations (a `py.typed` marker) that pass
`mypy --strict` in a consumer's own codebase.

## Connecting

Use the builder when you want fluent configuration:

```python
from rociadb_sdk import RociaDbBuilder

client = await (
    RociaDbBuilder()
    .host("https://db.example.com:443")
    .auth_client_credentials(
        "https://auth.example.com/oauth/token",
        "client-id",
        "client-secret",
    )
    .connect_timeout(15.0)
    .build()
)
```

`http://` creates an insecure gRPC channel and `https://` enables TLS.
RociaDB servers do not terminate TLS themselves — they listen in clear text —
so `https://` is only meaningful when it points at a reverse proxy sitting in
front of the server; that proxy is the normal production endpoint, typically
on port 443 (a bare `https://host` with no explicit port defaults to it, the
same way `http://host` defaults to port 80). Point `http://host:50051` (the
default gRPC port) directly at a RociaDB process for a local or unproxied
connection instead.

When `.auth_client_credentials(...)` is not called and auth is not disabled,
each of the token URL, client ID, and client secret falls back independently
to an environment variable:

```text
AUTH_TOKEN_URL
AUTH_CLIENT_ID
AUTH_CLIENT_SECRET
```

For a controlled local environment, authentication can be disabled
explicitly:

```python
client = await RociaDbBuilder().host("http://127.0.0.1:50051").disable_auth().build()
```

A direct, non-fluent constructor covers the same options in one call:

```python
from rociadb_sdk import RociaDbClient

client = await RociaDbClient.connect(
    "https://db.example.com:443",
    auth_token_url="https://auth.example.com/oauth/token",
    auth_client_id="client-id",
    auth_client_secret="client-secret",
    connect_timeout=15.0,
)
```

Omitting `host` defaults to `"http://127.0.0.1:50051"`; omitting any of the
three `auth_*` keyword arguments falls back to its environment variable
exactly as the builder does. `RociaDbClient` also supports the async
context-manager protocol, which calls `close()` automatically:

```python
async with await RociaDbClient.connect(disable_auth=True) as client:
    tenants = await client.list_tenants()
```

Create one client per upstream configuration and reuse it — a single
`RociaDbClient` instance is already safe to call concurrently from many
`asyncio` tasks, with no locking or cloning step needed. Call `close()`
during graceful shutdown; the client must not be reused afterward.

`.connect_timeout(seconds)` sets the deadline applied while every service
connects; call it on the builder before `.build()`, or pass
`connect_timeout` directly to `RociaDbClient.connect(...)`. When neither is
supplied, the client falls back to its own default of **10.0 seconds**
(`DEFAULT_CONNECT_TIMEOUT`), so a slow or unreachable DNS/TCP target fails
after a bounded wait instead of hanging forever. A non-positive value raises
`RociaDbValidationError` immediately when `.connect_timeout(...)` is called,
before any connection attempt is made.

`.host(...)` must resolve to a bare hostname and port — no path component
beyond an absent one or a lone `/`. A mistyped host with a leftover path
(`http://127.0.0.1:50051/v1`, pasted from somewhere else) is rejected with
`RociaDbConnectionError` before any connection attempt, rather than silently
dialing the host and dropping the path.

## Authentication

Every call carries a bearer token as gRPC metadata:

```text
authorization: Bearer <jwt>
```

The SDK obtains it from the configured token URL with a standard OAuth2
client-credentials request (`POST`, `application/x-www-form-urlencoded`,
`grant_type=client_credentials&client_id=...&client_secret=...`) and expects
a response shaped like:

```json
{ "access_token": "...", "token_type": "Bearer", "expires_in": 600 }
```

**Tokens issued by RociaDB's identity provider are valid for 600 seconds (10
minutes), fixed server-side.** The SDK does not hardcode that number — it
reads `expires_in` from each response — but in practice it is always 600.

A background task refreshes the cached token on its own schedule for as
long as the client is open, derived from the last reported token lifetime:
`max(expires_in * 2 // 3, 5.0)` seconds — about 400 seconds for the IdP's
fixed 600-second lifetime, leaving margin so the token never actually
expires between two refreshes. As long as the client stays open, you never
need to schedule or poll for a refresh yourself.

### `UNAUTHENTICATED` vs `PERMISSION_DENIED`

These two statuses look similar but call for opposite handling:

- **`UNAUTHENTICATED`** — the token is missing, expired, malformed, or signed
  by a different issuer. Treat this as a renewal signal: refresh the token
  and retry.
- **`PERMISSION_DENIED`** — the token is valid but its scope does not cover
  the operation. Retrying does not help, even after a refresh, because a
  fresh token carries the same scope. Two causes exist:
  - a **read-only** scoped client called one of the 7 write RPCs:
    `put_document`/`create_document`, `delete_document`,
    `put_node`/`put_nodes`, `add_edge`/`add_edges`, `delete_edge`,
    `upload_file`/`upload_file_chunked`/`upload_file_raw`, or
    `delete_file`. A read-only token is not otherwise crippled — all 15
    read RPCs remain available.
  - an **admin**-scoped token was presented — the credentials used to manage
    `rocia-idp` service accounts, not to read or write data. It is rejected
    on all 22 RPCs, reads included. If a read you expect to work returns
    `PERMISSION_DENIED`, check which `client_id` produced the token: the
    data-plane account and the administration account are different
    credentials.

```python
from rociadb_sdk import RociaDbStatusError

try:
    await client.get_document("tenant-1", "products", "sku-123")
except RociaDbStatusError as error:
    if error.is_unauthenticated():
        await client.refresh_auth_token()
        await client.get_document("tenant-1", "products", "sku-123")  # retry once
    else:
        raise  # includes PERMISSION_DENIED: retrying will not help
```

`is_unauthenticated()` and `is_permission_denied()` are defined on the base
`RociaDbError` too (always returning `False` there), so either predicate is
safe to call on any caught error without checking the exception type first.

### Two ways to recover from `UNAUTHENTICATED`

`client.invalidate_auth_token()` is the **lazy** option: synchronous,
returns immediately, and makes no network call itself — it only wakes the
background refresh task described above so it refreshes sooner. The cached
token keeps being used for any call made before that background refresh
actually completes, so calling `invalidate_auth_token()` immediately before
retrying can still send the same, still-invalid token. `refresh_auth_token()`
is the **eager** counterpart: it `await`s the round trip to the identity
provider itself and only returns once a fresh token is confirmed and cached,
or raises `RociaDbAuthError` on failure — the right choice immediately before
retrying the call that just failed, as shown above.

Reach for `invalidate_auth_token()` instead when you just want to mark the
token stale without blocking on a fresh one right now — a fire-and-forget
error handler that is not about to retry immediately, for example. Both are
no-ops when the client was built with `disable_auth()`. Neither ever
discards a still-valid cached token just because a refresh attempt failed: a
failed background refresh is logged and retried on the next tick rather than
clearing the cache.

### Auth helpers outside a client

`fetch_token` and `TokenManager` are importable from `rociadb_sdk.auth` for
callers who need OAuth2 token handling outside of a `RociaDbClient` — to
reuse the same access token against a different service, for example. They
are not re-exported from the `rociadb_sdk` package root, since they are
useful independently of any `RociaDbClient` instance:

```python
from rociadb_sdk.auth import TokenManager, fetch_token

# One-off token exchange, no caching or refresh:
token = await fetch_token("https://auth.example.com/oauth/token", "client-id", "client-secret")
print(token.access_token, token.token_type, token.expires_in)

# Cached, self-refreshing token manager - the same one RociaDbClient uses internally:
token_manager = TokenManager("https://auth.example.com/oauth/token", "client-id", "client-secret")
header = await token_manager.get_authorization_header()  # "Bearer <token>"
```

`fetch_token` performs one token exchange and returns; it does not cache
anything. `TokenManager(...)` fetches nothing until its first
`get_authorization_header()` call (or an explicit `await
token_manager.refresh_now()`); starting its own background refresh loop
requires `token_manager.spawn_refresh(token_manager.refresh_interval())`,
which returns a `TokenRefreshHandle` to `await ....aclose()` on shutdown.

## Pagination

Every listing method — documents, collections, graphs, nodes, neighbors,
buckets, files, and tenants — takes keyword-only `limit`/`cursor` arguments
and returns a `Page[T]` (`items`, `next_cursor`), or a `DocumentPage[T]`
which additionally carries `total_count`.

- **`limit` must be a positive integer.** The SDK rejects `0` and negative
  values client-side, before any RPC, with `RociaDbValidationError`. Omit it
  to use the SDK's own default of 20 (`DEFAULT_PAGE_SIZE`).
- **The server enforces its own ceiling**, `limits.max_page_size` (200 by
  default, operator-configurable per deployment). A `limit` above that
  ceiling is rejected server-side with `INVALID_ARGUMENT` — the SDK does
  **not** hardcode 200, or any other ceiling, client-side.
- **An absent `next_cursor` is the only end-of-list signal.** Loop while it
  is not `None`:

  ```python
  from typing import Optional

  cursor: Optional[str] = None
  while True:
      page = await client.list_documents("tenant-1", "products", limit=100, cursor=cursor)
      for item in page.items:
          print(item)
      if page.next_cursor is None:
          break
      cursor = page.next_cursor
  ```

- **Do not stop because a page is short, or even empty.**
  `find_documents_by_field`, `list_documents`, and `query_documents` in
  particular can return fewer items than `limit` — or none at all — in the
  middle of a paginated walk, when an index entry briefly survives the
  document it points to being deleted. A short or empty page with a
  non-`None` `next_cursor` is not the end; only the cursor tells you that.
- **An exact-multiple total produces one extra, empty page.** If a
  collection holds exactly `limit` items, the final full page still carries
  a cursor — the server has no way to know it just emitted everything — so
  the next call returns an empty page with `next_cursor=None`. This is
  expected, not a bug; the loop above handles it correctly by construction.
- Cursors are opaque: never construct, parse, or persist one across
  sessions. Their shape differs per RPC and may change.

`list_graphs`, `list_buckets`, and `list_nodes` paginate over graph/bucket
names, not the items each one contains — a graph or bucket name appears once
no matter how many nodes or files it holds.

## Documents

### Create, read, and delete

```python
product = {
    "sku": "sku-123",
    "label": "Widget",
    "active": True,
    "price": 19.9,
}

await client.put_document("tenant-1", "products", "sku-123", product)

fetched = await client.get_document("tenant-1", "products", "sku-123")

await client.delete_document("tenant-1", "products", "sku-123")
```

`get_document` accepts an optional `decoder` keyword to turn the raw JSON
value into any type you like:

```python
from dataclasses import dataclass


@dataclass
class Product:
    sku: str
    label: str
    active: bool
    price: float


typed = await client.get_document(
    "tenant-1", "products", "sku-123", decoder=lambda raw: Product(**raw)
)
```

`put_document` replaces the document completely — there is no partial merge
— and recomputes its indexes (exact-match and trigram) in the same
transaction. Its JSON payload is capped at the server's `limits.max_doc_bytes`
(2 MiB by default); a larger payload is rejected with `INVALID_ARGUMENT`
before anything is written. `delete_document` is idempotent: deleting a
document that does not exist is not an error (contrast this with
`delete_edge` in [Directed edges and neighbors](#directed-edges-and-neighbors),
which is not).

### Create a document and its graph reference

`create_document` first stores the document, then optionally creates a graph
node whose payload points back to `{"collection": ..., "id": ...}`:

```python
await client.create_document(
    "tenant-1",
    "products",
    "sku-123",
    product,
    node_label="product",
    node_graph="catalog",
)
# Creates document products/sku-123 and node product:sku-123 in catalog.
```

`node_label` and `node_graph` must be supplied together — passing only one
raises `RociaDbValidationError` before any network call. This is a
composite, non-atomic operation: if the node write fails, the document
remains stored and the caller should retry or compensate.

### Search, query, and pagination

Use `find_documents_by_field` for one exact field lookup, `list_documents`
for an unfiltered collection, and `query_documents` for multiple filters and
sorting:

```python
by_sku = await client.find_documents_by_field("tenant-1", "products", "sku", "sku-123")
```

`find_documents_by_field`'s `value` must encode to a JSON **scalar** — a
string, number, boolean, or `None`. An object or array raises
`INVALID_ARGUMENT` server-side; it uses the same exact-match index as
`query_documents`'s `EQ` operator.

```python
from typing import Optional

from rociadb_sdk import (
    DocumentQueryFilter,
    DocumentQueryOperator,
    DocumentQuerySort,
    DocumentSortDirection,
)

cursor: Optional[str] = None
while True:
    page = await client.query_documents(
        "tenant-1",
        "products",
        filters=[
            DocumentQueryFilter(field="active", operator=DocumentQueryOperator.EQ, values=[True]),
            DocumentQueryFilter(
                field="label", operator=DocumentQueryOperator.CONTAINS, values=["Widget"]
            ),
        ],
        sort=[DocumentQuerySort(field="price", direction=DocumentSortDirection.ASC)],
        limit=50,
        cursor=cursor,
    )
    for item in page.items:
        print(item["sku"])
    print("matching documents:", page.total_count)
    if page.next_cursor is None:
        break
    cursor = page.next_cursor
```

Supported query operators are `EQ`, `IN`, and `CONTAINS`. Filters are sent in
order and combined by the server with an implicit AND — there is no OR.
`CONTAINS` is a case-insensitive substring match backed by a trigram index,
with two restrictions: a `CONTAINS` term shorter than 3 characters is not
indexable, and a query where *no* filter is indexable is rejected with
`INVALID_ARGUMENT` rather than served by a full scan — pair a short
`CONTAINS` term with an `EQ` or `IN` filter on another field. Cursors are
opaque: pass `next_cursor` back unchanged.

`total_count` is not uniformly cheap. `list_documents`'s count is read from
a counter maintained on every write, so it costs nothing extra.
`find_documents_by_field`'s count is an index count over matching entries —
not free, but cheaper than a full evaluation. `query_documents`'s count is
computed by evaluating the full filtered result set, so it scales with the
number of matching documents — prefer `list_documents` (no filters) when you
just need a count, and avoid calling `query_documents` in a loop purely to
read `total_count`.

### Discovering collections

`list_collections` returns the collections that hold at least one document,
each with its document count:

```python
collections = await client.list_collections("tenant-1", limit=50)
for info in collections.items:
    print(info.collection, info.count)
```

## Graph

### Nodes and batches

```python
await client.put_node("tenant-1", "catalog", "product:sku-123", product)
node = await client.get_node("tenant-1", "catalog", "product:sku-123")
```

`put_node`'s payload must encode to a JSON **object** — not a scalar or an
array — and, like `put_document`, is capped at the server's
`limits.max_doc_bytes` (2 MiB by default).

```python
from rociadb_sdk import NodeInput

await client.put_nodes(
    "tenant-1",
    "catalog",
    [
        NodeInput(node_id="product:sku-124", value={**product, "sku": "sku-124"}),
        NodeInput(node_id="group:featured", value={"title": "Featured"}),
    ],
)
```

Batch helpers (`put_nodes`, `add_edges`, and the neighbor-node helpers below)
issue at most `CONCURRENT_REQUESTS` (10) requests concurrently. They are not
atomic: if one item fails, earlier items may already have been stored, and
every other already-dispatched call in the batch is cancelled rather than
left to run to completion. Retry a failed batch by reusing the same
`request_id` values on each item — see
[Idempotence and Retries](#idempotence-and-retries).

### Directed edges and neighbors

An edge goes from one node to another. For `product:sku-123 ->
group:featured`, the group is an outgoing neighbor of the product, while the
product is an incoming neighbor of the group.

```python
await client.add_edge(
    "tenant-1",
    "catalog",
    "membership-1",
    "product:sku-123",
    "group:featured",
    "belongs_to",
    {"weight": 1},
)

outgoing = await client.neighbors_out(
    "tenant-1", "catalog", "product:sku-123", "belongs_to", limit=25
)

products = await client.get_incoming_neighbor_nodes(
    "tenant-1", "catalog", "group:featured", "belongs_to"
)

await client.delete_edge("tenant-1", "catalog", "membership-1")
```

`add_edge` takes the endpoint node IDs as `from_id`/`to_id` — `from` is a
reserved word in Python and cannot be used as a parameter name. It fails
with `NOT_FOUND` if either endpoint does not already exist as a node in
`graph` — create both endpoint nodes before the edge — and, like
`put_document` and `put_node`, its JSON payload is capped at the server's
`limits.max_doc_bytes` (2 MiB by default). `delete_edge` also fails with
`NOT_FOUND` if the edge itself does not exist; unlike `delete_document`,
deleting an edge is not idempotent.

`neighbors_out` and `neighbors_in` return one raw `Page[Neighbor]`, each item
carrying `node_id` and `edge_id`. `get_outgoing_neighbor_nodes` and
`get_incoming_neighbor_nodes` follow every page internally and load each
neighbor's JSON node payload with the same bounded concurrency as `put_nodes`
(both also accept a `decoder` keyword, like `get_node`).

`add_edges(tenant_id, graph, edges)` batches edge creation from a list of
`EdgeInput` the same way `put_nodes` batches `NodeInput` — see
[Nodes and batches](#nodes-and-batches) for the concurrency and
non-atomicity rules shared by every batch helper.

### Discovering graphs and nodes

```python
graphs = await client.list_graphs("tenant-1")
nodes = await client.list_nodes("tenant-1", "catalog", limit=100)
```

Both return a `Page[str]` of names and node IDs. Use `get_node` (with an
optional `decoder`) to load a payload once the ID is known.

## Files

Three levels of upload help exist, from most to least hand-holding:
`upload_file` (buffers the whole file in memory, computes the checksum for
you), `upload_file_chunked` (streams arbitrarily-sized chunks without
buffering the whole file — you supply the checksum, but it still re-chunks
and validates everything else for you), and `upload_file_raw` (a raw
pass-through — you build every protobuf-backed message yourself, with zero
validation). The wire contract all three implement is worth understanding
even if you only ever call `upload_file`.

### The upload wire contract

`Upload` is a client-streaming RPC:

- **The first message carries the file's metadata** — `tenant_id`, `bucket`,
  `file_id`, `size_bytes` (the exact total byte count), `content_type`,
  `checksum`, and `request_id`. Every later message is only read for its
  `chunk` field; metadata fields on those later messages are ignored.
- **Chunk size is the client's choice, capped at 1 MiB — not a fixed
  requirement.** The server stores each chunk verbatim at its position in
  the stream and, on download, reads chunks back until it has collected
  `size_bytes` bytes in total — it does not assume any particular chunk size
  when replaying them. A single message's `chunk` larger than 1 MiB is
  rejected outright with `INVALID_ARGUMENT`; anything at or under that cap
  is fine. `upload_file` and `upload_file_chunked` both always emit
  exactly-1-MiB messages (the last one possibly shorter): 1 MiB is the
  largest message the server allows, so it is also the fewest possible
  messages for a given file, and it remains the only chunk size safe
  against a server older than `1.0.0-rc.16`, which reassembled a download
  from a guessed chunk count instead of the recorded `size_bytes`. This is
  also why neither method exposes a chunk-size option.
- **`checksum` must be exactly 32 raw bytes — a SHA-256 digest.** Any other
  length, including empty, is rejected with `INVALID_ARGUMENT` before a
  single chunk is read. The server does not verify that the checksum
  actually matches the uploaded bytes, only that its length is correct.
- **The sum of every `chunk`'s bytes across the stream must equal
  `size_bytes` exactly**, or the server rejects the upload with
  `INVALID_ARGUMENT` at the end of the stream — this is what makes
  `size_bytes` a value the SDK, and the server on download, can trust,
  rather than just a caller-supplied claim.
- **Re-uploading an existing `file_id` replaces it, with no error for the
  duplicate** — no separate delete-then-upload dance is required.
  `download_file`/`stat_file` afterward always serve the newest upload.
- **Files are capped at the server's `limits.max_file_bytes`** (5 GiB by
  default, `MAX_FILE_BYTES` in this SDK). `upload_file` and
  `upload_file_chunked` check this client-side, before any RPC, and raise
  `RociaDbValidationError` if it is exceeded.
- **An empty file is valid and common**: it needs exactly one message
  (metadata only, empty `chunk`) and no data messages. `upload_file` and
  `upload_file_chunked` both send it automatically.
- **The file only becomes visible** (in `list_files`, `stat_file`,
  `download_file`) once the whole stream has been received and validated.
  An interrupted stream leaves orphaned chunks that a background GC
  eventually reclaims; the partial file never appears anywhere, so retrying
  (with a fresh `request_id`) is always safe.
- **`delete_file` removes a whole file**, regardless of how many chunks it
  was stored in.

### Buffered files

Use the buffered helpers for reasonably sized objects. `upload_file`
computes a SHA-256 checksum of `data` automatically when `checksum` is
omitted:

```python
payload = b"hello RociaDB"

await client.upload_file(
    "tenant-1", "assets", "manual.txt", payload, content_type="text/plain; charset=utf-8"
)

metadata = await client.stat_file("tenant-1", "assets", "manual.txt")
print(metadata.size_bytes, metadata.content_type)
print(metadata.checksum.hex())  # sha256 hex

data = await client.download_file("tenant-1", "assets", "manual.txt")
await client.delete_file("tenant-1", "assets", "manual.txt")
```

`metadata.checksum` is the raw 32-byte digest; hex-encode it for display or
comparison, as shown above. If you already have a checksum computed
elsewhere, pass it as `checksum=...` instead — it must be exactly 32 bytes,
checked before any network call.

### Uploading from a stream without buffering the whole file

`upload_file_chunked` avoids holding the complete object in memory. The
caller must know the total upload size **and a precomputed SHA-256
checksum** before starting the RPC — unlike `upload_file`, it cannot hash
the data for you, because the checksum has to travel on the first message,
before the SDK has read anything from your source:

```python
import hashlib
from pathlib import Path

source = Path("large.bin")
size_bytes = source.stat().st_size

hasher = hashlib.sha256()
with source.open("rb") as f:
    for block in iter(lambda: f.read(65536), b""):
        hasher.update(block)
checksum = hasher.digest()


def read_chunks():
    with source.open("rb") as f:
        while True:
            block = f.read(65536)
            if not block:
                return
            yield block


await client.upload_file_chunked(
    "tenant-1", "assets", "large.bin", size_bytes, checksum, read_chunks()
)

with open("downloaded.bin", "wb") as out:
    async for chunk in client.download_file_stream("tenant-1", "assets", "large.bin"):
        out.write(chunk)
```

`chunks` may be a sync `Iterable[bytes]` or an `AsyncIterable[bytes]`, and
its pieces may be any size — `read_chunks()` above yields 64 KiB blocks —
because `upload_file_chunked` re-buffers internally and only ever writes 1
MiB per outgoing message, per
[The upload wire contract](#the-upload-wire-contract) above. `download_file_stream`
is an async generator: iterate it directly with `async for`, no `await` on
the call itself.

Porting upload code from the Rust or TypeScript SDK by method name alone is
unsafe here: neither `upload_file_chunked`'s nor `upload_file_raw`'s
counterpart in those SDKs is the method whose name looks closest — see
[Parity with the Other SDKs](#parity-with-the-other-sdks) for the naming
table before translating upload calls between SDKs.

### Raw streaming upload escape hatch

`upload_file_raw` passes every message straight through to the gRPC call
exactly as given — **no re-chunking, no checksum validation, and no
distinction between the first message and the rest.** You are fully
responsible for the wire contract described in
[The upload wire contract](#the-upload-wire-contract) above.

```python
import hashlib
from uuid import uuid4

from rociadb_sdk import RawUploadMessage


async def raw_upload():
    payload = b"hello RociaDB"
    yield RawUploadMessage(
        tenant_id="tenant-1",
        bucket="assets",
        file_id="manual.txt",
        size_bytes=len(payload),
        content_type="text/plain",
        checksum=hashlib.sha256(payload).digest(),
        chunk=payload,
        request_id=f"upload_file:{uuid4()}",
    )
    # A larger file would yield further RawUploadMessage instances here,
    # chunk <= 1 MiB each. Every field is required by the dataclass, but the
    # server only reads the non-chunk fields off the first message, so later
    # messages can repeat placeholder values for them.


await client.upload_file_raw(raw_upload())
```

Getting the chunk *size* wrong here fails fast with `INVALID_ARGUMENT`
rather than silently corrupting a later download — but a wrong `size_bytes`
total, or a `checksum` that does not actually match the bytes (the server
only checks its length, never its content), can still slip through as an
upload that looks successful while carrying bad data. Prefer `upload_file`
or `upload_file_chunked` above unless you specifically need to hand-build
the message stream.

### Discovering buckets and files

```python
buckets = await client.list_buckets("tenant-1")
files = await client.list_files("tenant-1", "assets", limit=100)
```

## Tenants

`list_tenants` is the only RPC that takes no `tenant_id`: it enumerates
every tenant known to the deployment (the registry is filled in implicitly —
a tenant appears the first time any RPC mentions it). It lives on its own
service so a dedicated authorization policy could be attached to it
independently of the data-plane services; today, any authenticated client
can call it, including a read-only one. The one credential that cannot is an
admin-scoped token — like every other RPC, it gets `PERMISSION_DENIED` (see
[Authentication](#authentication)).

```python
from typing import Optional

cursor: Optional[str] = None
while True:
    page = await client.list_tenants(limit=100, cursor=cursor)
    for tenant_id in page.items:
        print(tenant_id)
    if page.next_cursor is None:
        break
    cursor = page.next_cursor
```

## Idempotence and Retries

Every mutating call creates a unique request ID by default. When retrying an
operation after a timeout, pass the same `request_id` so the server can
recognize the attempt as the same logical mutation instead of applying it
twice:

```python
from uuid import uuid4

request_id = str(uuid4())
await client.put_document("tenant-1", "products", "sku-123", product, request_id=request_id)
```

A `request_id` is scoped to `(tenant_id, operation, request_id)` — replaying
the same value against a *different* operation is not treated as the same
mutation. Reusing a `put_document` call's `request_id` on a later
`delete_document` call, for instance, does not cancel or replace the earlier
write; it is simply a different idempotency key. Markers expire after the
server's `gc.request_ttl_secs` (24 hours by default); a retry older than
that window executes again rather than being deduplicated.

Every mutating method except the raw upload escape hatch takes `request_id`
as an optional keyword argument, defaulting to an auto-generated value if
omitted; `put_nodes` and `add_edges` read it off each `NodeInput`/`EdgeInput`
item instead, one per batch entry. `upload_file_raw` has no such default —
each `RawUploadMessage` carries its own `request_id` explicitly.

## Error Handling

All SDK failures raise a subclass of `RociaDbError`. Catching the base class
covers everything; catching a specific subclass narrows to one cause:

| Exception | Meaning |
|---|---|
| `RociaDbStatusError` | A gRPC call returned a non-OK status. Carries `operation`, `code` (a `grpc.StatusCode`), `reason` (the server's `reason` trailing metadata, finer-grained than `code` alone), and `grpc_error` (the original `grpc.aio.AioRpcError`). |
| `RociaDbConnectionError` | Failed to connect to, or configure, the endpoint — invalid host, TLS setup, connection refused, missing auth configuration, a non-positive connect timeout. |
| `RociaDbAuthError` | Failed to obtain or refresh the OAuth2 bearer token. |
| `RociaDbEncodeError` | Failed to JSON-encode a document, node, or edge payload before sending it. Carries `context`. |
| `RociaDbDecodeError` | Failed to JSON-decode a payload received from the server. Carries `context`. |
| `RociaDbValidationError` | A client-side rule was rejected before any network call — a non-positive page `limit`, a checksum of the wrong length, a file size out of bounds, a partial `node_label`/`node_graph` pair, and so on. |

Only `RociaDbStatusError` ever carries a gRPC `code`/`reason`; the other
subclasses are always raised client-side, before any RPC. Narrow on the
exception type first to handle a whole category (every validation error
alike, say), and on `code`/`reason` for gRPC-specific branching:

```python
import grpc

from rociadb_sdk import RociaDbStatusError

try:
    await client.get_document("tenant-1", "products", "missing")
except RociaDbStatusError as error:
    if error.code == grpc.StatusCode.NOT_FOUND:
        print("document does not exist")
    else:
        raise
```

Catch the `RociaDbError` base instead of a specific subclass for a
catch-all across every possible SDK failure.

| gRPC `code` | `reason` | When |
|---|---|---|
| `INVALID_ARGUMENT` | `invalid_argument` | Missing/malformed field, `limit` out of bounds, unreadable cursor, invalid JSON |
| `NOT_FOUND` | `not_found` | Document, node, edge, or file does not exist |
| `ALREADY_EXISTS` | `already_exists` | A uniqueness conflict |
| `PERMISSION_DENIED` | `permission_denied` | Insufficient scope — see [Authentication](#authentication) |
| `UNAUTHENTICATED` | `unauthenticated` | Token missing, expired, malformed, or from another issuer — see [Authentication](#authentication) |
| `INTERNAL` | `internal` | Storage-layer failure |

`error.is_unauthenticated()` and `error.is_permission_denied()` are
shorthands for checking `code` against those two values; they are defined
on the `RociaDbError` base (returning `False`) so either is safe to call on
any caught error, as shown in [Authentication](#authentication).

## API Coverage

| Service | RPC | SDK method |
|---|---|---|
| Document | `PutDoc` | `put_document`, `create_document` |
| Document | `GetDoc` | `get_document` |
| Document | `DeleteDoc` | `delete_document` |
| Document | `FindByField` | `find_documents_by_field` |
| Document | `ListDoc` | `list_documents` |
| Document | `QueryDoc` | `query_documents` |
| Document | `ListCollections` | `list_collections` |
| Graph | `PutNode` | `put_node`, `put_nodes` |
| Graph | `GetNode` | `get_node` |
| Graph | `AddEdge` | `add_edge`, `add_edges` |
| Graph | `DeleteEdge` | `delete_edge` |
| Graph | `NeighborsOut` | `neighbors_out`, `get_outgoing_neighbor_nodes` |
| Graph | `NeighborsIn` | `neighbors_in`, `get_incoming_neighbor_nodes` |
| Graph | `ListGraphs` | `list_graphs` |
| Graph | `ListNodes` | `list_nodes` |
| File | `Upload` | `upload_file`, `upload_file_chunked`, `upload_file_raw` |
| File | `Download` | `download_file`, `download_file_stream` |
| File | `Stat` | `stat_file` |
| File | `Delete` | `delete_file` |
| File | `ListBuckets` | `list_buckets` |
| File | `ListFiles` | `list_files` |
| Tenant | `ListTenants` | `list_tenants` |

## Parity with the Other SDKs

This SDK, the Rust SDK
([`rociadb-core-sdk-rust`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-rust)),
and the TypeScript SDK
([`rociadb-core-sdk-ts`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-ts))
cover the same 22 RPCs against the same server and are maintained to the
same standard: **every capability available in one is available in all
three.** None imitates another's syntax — this package stays
snake_case/exception-idiomatic Python, the Rust crate stays
snake_case/`Result`-idiomatic Rust, the TypeScript package stays
camelCase/`Promise`-idiomatic TypeScript — but a piece of client code should
always have a mechanical translation from any one SDK to either of the
others. Parity is about what you can *do*, not about identical method
names. Because this SDK and the Rust SDK both use snake_case, most method
names already match character for character between them (`put_nodes`,
`get_outgoing_neighbor_nodes`, `list_documents`, and so on); translating to
or from the TypeScript SDK is usually just the camelCase transform
(`putNodes`, `getOutgoingNeighborNodes`, `listDocuments`). The handful of
places where a name does **not** translate mechanically are the naming
table below.

| Capability | Python (this SDK) | Rust | TypeScript | Note |
|---|---|---|---|---|
| Assisted streaming upload — re-chunks to the 1 MiB wire contract, validates the total, caller supplies the checksum | `upload_file_chunked` | `upload_file_chunked` | `uploadFileStream` | Python reuses Rust's unambiguous name. **Names do not correspond to TypeScript's `uploadFileStream`** — see the naming trap below. |
| Raw streaming upload — zero validation, caller builds every message | `upload_file_raw` | `upload_file_stream` | `uploadFileRaw` | Python reuses TypeScript's unambiguous name. **This is Rust's `upload_file_stream`, the mirror image of the row above.** |
| Idempotency key scoped to a `create_document`/`createDocument` call's document write only (the graph node binding keeps its own auto-generated key) | `create_document(..., request_id=...)` — a keyword argument | `create_document_with_request_id` — a sibling method | `createDocument(..., { requestId })` — an options-object field | Same capability, three idiomatic shapes for an optional named argument in each language. |
| Releasing the connection and any cached auth state | `close()` / `async with` (owns its channel outright; must not be reused after) | Drop the last live `RociaDbClient` clone (`Clone` + one shared channel; no method) | `close()` (owns its channel outright, same model as Python) | Rust's ownership model shares one channel across every clone, so there is no explicit release method to call — dropping the last clone is the release point. |
| Marking the cached token stale without blocking on a refresh | `invalidate_auth_token()` — wakes a background refresh task; the cached token is still used until that refresh completes | `invalidate_auth_token()` / `TokenManager::request_refresh` — same background-task design | `invalidateToken()` — synchronously drops the cached token; the next call fetches inline | Python mirrors Rust's background-refresh design (see [Authentication](#authentication)) rather than TypeScript's on-demand-near-expiry design, so the *method name* matches Rust while the *exact timing* differs from TypeScript's synchronous drop. |
| Discriminating why an error happened | `RociaDbError` subclass hierarchy — `isinstance`/`except` on `RociaDbStatusError`, `RociaDbConnectionError`, etc. | `RociaDbError` — a `match`-able enum: `Status { .. }` / `Connection { .. }` / ... | `RociaDbError` — one class with a `kind: "status" \| "connection" \| ...` field | Three idiomatic shapes for the same closed set of six causes: a subclass hierarchy, a sum type, and a discriminated union. |
| Escape hatch to the raw generated protobuf/gRPC types, to build a custom client against the same `.proto` | *(none — `rociadb_sdk._pb` is private; every public method returns this package's own dataclasses)* | the `pb` module (`#[doc(hidden)] pub mod pb`; a few generated types are re-exported individually at the crate root) | the `rocia-db-sdk/proto` subpath export | Python deliberately keeps its generated stubs fully private — there is no equivalent escape hatch in this SDK by design. |

**The upload naming trap, spelled out:** `upload_file_chunked` (this SDK,
same name as Rust) and `uploadFileStream` (TypeScript) are the *same*
capability — the middle tier that re-chunks and validates for you (see
[Uploading from a stream without buffering the whole file](#uploading-from-a-stream-without-buffering-the-whole-file)).
`upload_file_raw` (this SDK, same name as TypeScript) and `upload_file_stream`
(Rust) are also the *same* capability — the raw, zero-validation escape
hatch (see [Raw streaming upload escape hatch](#raw-streaming-upload-escape-hatch)).
`upload_file_chunked` and `upload_file_stream` are **not** each other's
counterpart despite the near-identical name: reading either name in
isolation and guessing at the other SDK's equivalent by ear silently swaps
which tier you land on. Always cross-check against the table above.

Two capabilities are intentionally kept on one or two sides without a
mirror everywhere: `ApiKeyInterceptor` (Rust only — it validates an
*incoming* API key, so it serves building a server or a test double, not
talking to RociaDB, which puts it out of scope for any client SDK), and
having both a fluent `RociaDbBuilder` and a direct one-call constructor
(`RociaDbClient.connect`) — Python and TypeScript
both offer this pair; Rust offers only the builder, since a second entry
point there would add an API to maintain for no new capability.

## Development and Testing

The project is managed with [uv](https://docs.astral.sh/uv/). From a clone
of this repository:

```bash
uv sync
uv run --python 3.10 pytest -q
uv run --python 3.14 pytest -q      # repeat for 3.11, 3.12, 3.13 as needed
uv run --python 3.10 ruff check .
uv run --python 3.10 ruff format --check .
uv run --python 3.10 mypy
```

The protobuf/gRPC stubs under `src/rociadb_sdk/_pb/` are generated, vendored
code — never edit them by hand. After changing `proto/upstream/v1/upstream.proto`
(itself a byte-identical copy of the server's `.proto` file, never edited
independently), regenerate them with:

```bash
uv run --python 3.10 python scripts/generate_proto.py
```

Licensed under Apache-2.0.
