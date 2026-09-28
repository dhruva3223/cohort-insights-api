# Cohort Insights API

Users submit documents. Each document goes through two mock stages: first a summary, then tags made from that summary. Only the user who submitted a document can see it. A partner system can also find a document by its own id, `client_doc_ref`.

Python 3.11, FastAPI, MongoDB (PyMongo async client), Redis, Docker Compose.

## Setup

```bash
docker compose up --build
```

The API runs on `http://localhost:8000` and the Swagger docs are at `/docs`. Settings are in `docker-compose.yml`, and `.env.example` explains what each one does.

To run the API without Docker (MongoDB and Redis still need to be running):

```bash
cp .env.example .env
uvicorn app.main:create_app --factory --port 8000
```

## Tests

The tests need MongoDB and Redis running locally:

```bash
docker compose up -d mongo redis
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest
```

They use their own database (`cohort_insights_test`) and Redis DB 15, so they don't touch app data.

## Try it

You need `curl` and `jq`. Run the commands in order in one terminal.

```bash
H='X-User-ID: alice'
J='Content-Type: application/json'

# Submit. Returns 201 and status "queued"
DOC=$(curl -s -X POST localhost:8000/documents -H "$H" -H "$J" \
  -d '{"title":"Notes","content":"first draft","client_doc_ref":"cms-42"}' | jq -r .document_id)

# Poll. Goes queued -> processing -> enriching -> completed, about 15-35 seconds
curl -s localhost:8000/documents/$DOC -H "$H" | jq '{status, version, is_stale, failed_stage, summary, tags}'

# Change the content. version goes to 2, summary and tags are null until the new run finishes
curl -s -X PATCH localhost:8000/documents/$DOC -H "$H" -H "$J" -d '{"content":"second draft"}' | jq

# Find it by the partner's ref
curl -s localhost:8000/documents/by-ref/cms-42 -H "$H" | jq '{document_id, status, version}'

# List alice's documents, newest first (add &status=completed to filter)
curl -s "localhost:8000/users/alice/documents?page=1&page_size=10" -H "$H" | jq '{total, items: [.items[] | {document_id, status}]}'
```

The edge cases:

```bash
# Same ref and same content the document has now: 200 with the same document_id
curl -s -w ' %{http_code}\n' -X POST localhost:8000/documents -H "$H" -H "$J" \
  -d '{"title":"Notes","content":"second draft","client_doc_ref":"cms-42"}'

# Same ref, different content: 409
curl -s -w ' %{http_code}\n' -X POST localhost:8000/documents -H "$H" -H "$J" \
  -d '{"title":"Notes","content":"something else","client_doc_ref":"cms-42"}'

# Another user asks for alice's document: 404, same as a document that doesn't exist
curl -s -w ' %{http_code}\n' localhost:8000/documents/$DOC -H 'X-User-ID: bob'

# Four new documents at once: 201 201 201 429
for i in 1 2 3 4; do
  curl -s -o /dev/null -w '%{http_code} ' -X POST localhost:8000/documents -H 'X-User-ID: carol' -H "$J" \
    -d "{\"title\":\"t\",\"content\":\"load $i $RANDOM\"}"
done; echo
```

Submitting content that was already processed comes back `completed` right away because of the cache. That's why the last example adds `$RANDOM` to the content. To make the pipeline faster, lower `PROCESSING_*` and `ENRICHING_*` in `docker-compose.yml`.

## Endpoints

The caller is identified by the `X-User-ID` header. `POST` also accepts `user_id` in the body.

| Endpoint | Purpose | Status codes |
|---|---|---|
| `POST /documents` | Submit `title`, `content`, optional `client_doc_ref` | 201, 200 (repeat of same ref), 409, 429, 400, 422 |
| `PATCH /documents/{id}` | Replace `content`, optional `expected_version` | 200, 404, 409, 429 |
| `GET /documents/{id}` | Status, both stages, and results | 200, 404 |
| `GET /documents/by-ref/{ref}` | Same, found by partner ref | 200, 404 |
| `GET /users/{user_id}/documents` | `page`, `page_size`, optional `status` | 200, 404 |
| `GET /health` | Checks MongoDB and Redis | 200, 503 |

## Design

### Code layout

`app/routers` has the HTTP endpoints, `app/services` has the logic (submit, patch, pipeline, rate limit, cache), and `app/repositories/documents.py` has all the MongoDB queries. Settings, the database, Redis and the current user come in through FastAPI dependencies.

### Pipeline and status

`status` is one of `queued`, `processing`, `enriching`, `completed`, `failed`. Each document also has a `stages` object with an entry for `processing` and one for `enriching`. Each entry has its own `state` (`pending`, `running`, `succeeded`, `failed`) and `error`, and the response has `failed_stage`, so you can see which stage failed. The stages run as `asyncio` tasks in the API process. Enriching reads the summary that processing saved.

To retry a failed document, PATCH it with the same content:

- If enriching failed, the summary is kept, so only enriching runs again. The version stays the same.
- If processing failed, the whole run starts again with a new version.

Until the retry finishes, the API shows `summary` and `tags` as null, because it only shows them together.

### Rate limit

Each user has a counter in Redis, `active_jobs:{user_id}`. A document takes a slot when it's accepted and gives it back when it completes or fails. The check and increment happen in one Lua script, so two requests at the same time can't both get the last slot. If Redis is down, it counts the user's active documents in MongoDB instead. The counters are rebuilt from MongoDB on startup.

### Cache

Results are cached by content hash (`cache:content:{sha256}`), not by document id, so after a PATCH the old content's result can't be used for the new content. A cache hit returns `completed` immediately, skips the pipeline and doesn't use a slot. Entries expire after 24 hours. If Redis is down, it's treated as a cache miss.

### Ownership

Only the owner can read, list or patch a document. Everyone else gets the same 404 as for a document that doesn't exist. I picked 404 over 403 so a non-owner can't tell whether the document exists.

### Race conditions

- Two runs claiming the same document: starting a stage is a conditional update (`queued` to `processing`, or enriching from pending to running after a retry or restart), so only one run gets it.
- Two PATCHes on the same document: an update only applies if the version is still the one that was read. The loser re-reads and tries again, up to 3 times, then gets 409. A caller can send `expected_version` to get 409 instead if the document changed.
- A PATCH during a run: see "Schema & Staleness Design".

### Startup and shutdown

On startup the app creates indexes, restarts documents that were in the pipeline when it stopped (from enriching if their summary is still current), and rebuilds the rate-limit counters. On shutdown it cancels running pipeline tasks. Logs are JSON.

## Crosswalk (`client_doc_ref`)

`client_doc_ref` is optional. When it's given, it must be unique across all users. If the same ref is submitted again:

| Repeat submission | Result |
|---|---|
| Same user, same content as the document has now | 200 with the existing `document_id` and status. No new job |
| Different content, or a different user | 409. The existing document isn't returned |

The first submission wins. Later ones are compared with the document's current content, so it doesn't matter if they arrive out of order. To change the content, use PATCH. If two submissions with a new ref arrive at the same time, the unique index lets only one insert through, and the other gets 200 or 409 using the rules above.

Index used by `GET /documents/by-ref/{ref}`:

```text
{ client_doc_ref: 1 }  unique, partialFilterExpression: { client_doc_ref: { $exists: true } }
```

Documents without a ref don't have the field at all, so they don't clash on the unique index. The query filters on `client_doc_ref` only, and ownership is checked in code afterwards. `tests/test_reads.py::test_by_ref_query_uses_client_doc_ref_index` runs `explain()` and checks it's an index scan, not a collection scan.

Other indexes: `document_id` (unique), `content_hash`, and two for the list: `{user_id, status, created_at, _id}` and `{user_id, created_at, _id}`. Both end with the list's sort order, so MongoDB reads pages straight from the index without sorting in memory. `tests/test_reads.py::test_list_query_reads_index_in_order` checks this with `explain()`. MongoDB's `_id` is never returned. The public id is `document_id`, a UUID.

## Assumptions

- `POST` takes the user from the header or the body. If both are sent, they must match.
- By-ref lookup also needs the owner's `X-User-ID`.
- A 409 on a repeated ref tells the caller the ref exists. That's fine because refs are partner ids, not secrets, and nothing from the document is returned.
- A repeat with the same ref and content but a different title returns 200 and keeps the original title.
- A repeat for a document that `failed` returns 200 with `failed` and doesn't retry it. Retrying is done with PATCH.
- A PATCH with the content the document already has does nothing, unless the document failed.
- A PATCH on a finished document needs a free slot because it starts a new run. A PATCH on a document still in the pipeline keeps its current slot.
- The cache is shared across users. The result only depends on the content.
- It runs as a single API instance.

## Known limitations

- The MongoDB fallback for the rate limit isn't atomic, so a burst of requests while Redis is down can go slightly over 3.
- If giving a slot back to Redis fails, the error is logged and the counter stays high until its 1 hour TTL runs out or the app restarts.
- Pipeline tasks run inside the API process, so running several API instances isn't safe.

## What I'd do with more time

- Move the pipeline to a job queue with separate workers, so jobs survive crashes and the API can scale out.
- Retry a failed stage automatically with backoff, instead of waiting for the caller to PATCH.
- Metrics for queue depth, stage time, cache hit rate and 429s.
- The pagination change from "At 100x".

## Schema & Staleness Design

Each document has an integer `version`. It goes up by 1 on every PATCH that starts a new run from processing: new content, or a retry after processing failed. Retrying only enriching keeps the version, because the stored summary was already made for it. `summary` and `tags` each store `source_version`, the version they were made from, and `source_hash`, the hash of that content.

On the write side, every pipeline write filters on `document_id`, the version the run started with, and the status it expects, for example `{document_id, version: 2, status: "enriching"}`. If a PATCH moves the document to version 3 during a version 2 run, the rest of that run's writes match nothing and it stops. An old run can't write results onto new content.

On the read side, every endpoint builds its response with one function, `document_to_response`. It returns `summary` and `tags` only if both have `source_version` equal to the document's current `version`. Otherwise both are null and `is_stale` is true. `is_stale` isn't stored. It's worked out on every read from that comparison.

With both of these in place, a reader never sees a mismatch. Old results can't be written to a newer version, and even if the database held a v3 summary with v2 tags, the read would return neither.

## At 100x

### What happens to the user_id indexes if one user has 500K documents?

Filtering by `user_id` was fine. The slow part was the sort. The list is sorted by `created_at` and then `_id`, newest first, and my first indexes (`{user_id, status}` and `{user_id, created_at}`) didn't match that order. When I ran `explain()` on the list query, MongoDB used the index to find the documents and then sorted them in memory. With 500K documents that means loading all of them to return 20, and it can hit the in-memory sort limit.

So I changed the indexes to end with the sort: `{user_id: 1, status: 1, created_at: -1, _id: -1}` for filtered lists and `{user_id: 1, created_at: -1, _id: -1}` for the rest. MongoDB now reads a page in order and stops after 20. What's left is the `total` count, which still goes through all of the user's entries on every page, so at that size I'd cache it or stop returning it.

### What would the shard key be?

I'd shard on `{user_id: 1, document_id: 1}`.

I didn't go with just `user_id` because all of one user's documents would sit in a single chunk that MongoDB can't split, and one shard would take all of that user's traffic. With `document_id` added, a big user's documents can be spread over several chunks, and listing a user's documents still only goes to the shards that hold that user. GET by id already filters on both fields.

I'd also have to change two things. The pipeline updates don't filter on `user_id` right now, so they'd go to every shard, and I'd add it. And MongoDB can't keep `client_doc_ref` unique on a collection sharded by a different key. I'd move the refs to a small separate collection (`client_doc_ref -> user_id, document_id`) sharded by hashed ref, and the by-ref lookup would check that first.

### Does the Redis rate-limit counter still work at 100x submit QPS?

The speed would be fine. Each acquire and release is one Lua script on one key per user, so it stays fast and atomic, and different users end up on different Redis nodes.

The problem is that the count can drift. It's only correct if every job releases exactly once. If a process crashes or a release call fails, the user stays blocked until the one-hour TTL expires, and the counters are only rebuilt on startup.

I'd use a sorted set per user instead of a counter, with one entry per job (`document_id:version`) and the expiry time as the score. Acquire removes expired entries and then checks the count. Release removes only that job's entry, so running it twice doesn't break anything. I'd also remove the MongoDB fallback, since at that traffic it would just push all the load onto the database.

### Does skip/limit pagination still work?

It still returns the right results, but pages get slower the deeper you go, because `skip(n)` still reads the `n` skipped entries. Page 10,000 with 20 per page reads 200K entries only to throw them away. New documents also shift the pages, so you can see the same one twice.

I'd switch to cursor pagination on `(created_at, _id)`. Each response includes the last item's pair, and the next request asks for everything older than it. With the new index, page 10,000 costs the same as page 1.
