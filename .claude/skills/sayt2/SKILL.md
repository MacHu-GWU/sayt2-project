---
name: sayt2
description: Teaches how to use the sayt2 Python library to build search-as-you-type applications. Use when the user asks how to use sayt2, wants to build a search index, or needs help with sayt2 DataSet, fields, queries, sorting, or caching.
---

# sayt2 — Search-As-You-Type Library for Python

## Overview

**sayt2** is a Python library that lets you build a full-text search index from a list of dictionaries and query it with substring matching (ngram), BM25 full-text search, fuzzy search, range queries, sorting, and more — all through a single `DataSet` object.

Under the hood it uses [tantivy](https://github.com/quickwit-oss/tantivy-py) (Rust-based search engine), [pydantic](https://docs.pydantic.dev/) for validation, and [diskcache](https://grantjenks.com/docs/diskcache/) for a two-layer disk cache.

## Installation

```bash
pip install sayt2
```

## Import Convention

```python
# Option A: import specific names
from sayt2.api import (
    DataSet,
    NgramField,
    TextField,
    KeywordField,
    NumericField,
    DatetimeField,
    BooleanField,
    StoredField,
    SortKey,
    Hit,
    SearchResult,
)

# Option B: import the module
import sayt2.api as sayt2
# then use sayt2.DataSet, sayt2.NgramField, etc.
```

## Core Concepts

### Field Types

Fields tell sayt2 how to index each column in your data.

| Field | Purpose | Searchable | Example Use |
|-------|---------|-----------|-------------|
| `KeywordField(name="id")` | Exact match | Yes (exact) | IDs, tags, status |
| `NgramField(name="name", min_gram=2, max_gram=6)` | Substring matching (search-as-you-type) | Yes (partial) | Names, titles |
| `TextField(name="bio")` | Full-text BM25 word-level search | Yes (words) | Descriptions, content |
| `NumericField(name="year", kind="i64")` | Numeric values, range queries, sorting | Yes (range) | Year, price, rating |
| `DatetimeField(name="created_at")` | Datetime values | Yes (range) | Timestamps |
| `BooleanField(name="active")` | True/false values | Yes | Flags, toggles |
| `StoredField(name="metadata")` | Stored but not searchable | No | Extra data to return |

**Field options:**
- `boost=3.0` — increase ranking weight for this field
- `indexed=True` — enable range queries (NumericField)
- `fast=True` — enable sorting (NumericField)
- `kind="i64"` or `kind="f64"` — integer or float (NumericField)

### DataSet

`DataSet` is the main entry point. It manages indexing, searching, and caching.

**Constructor parameters:**
- `dir_root` — path to store the index and cache files
- `name` — unique name for this dataset
- `fields` — list of field definitions
- `downloader` — callable that returns `list[dict]` (your data source)
- `cache_expire` — seconds before cache expires (None = never)
- `sort` — optional list of `SortKey` for default ordering

### SearchResult and Hit

`ds.search(query)` returns a `SearchResult` with:
- `result.hits` — list of `Hit` objects
- `result.size` — number of results
- `result.took_ms` — search time in milliseconds
- `result.cache` — True if served from cache
- `result.fresh` — True if index was just rebuilt

Each `Hit` has:
- `hit.source` — the original dict
- `hit.score` — relevance score

## Example 1 — People Directory (Ngram + Full-Text Search)

### Step 1: Prepare the data

```python
records = [
    {
        "id": "1",
        "name": "Alice Johnson",
        "title": "Senior Data Scientist",
        "bio": "Alice specializes in machine learning and natural language processing.",
    },
    {
        "id": "2",
        "name": "Bob Martinez",
        "title": "Backend Engineer",
        "bio": "Bob builds scalable microservices with Python and Go.",
    },
    {
        "id": "3",
        "name": "Charlie Wang",
        "title": "Frontend Developer",
        "bio": "Charlie creates beautiful user interfaces with React and TypeScript.",
    },
    {
        "id": "4",
        "name": "Diana Patel",
        "title": "DevOps Engineer",
        "bio": "Diana manages cloud infrastructure on AWS and Kubernetes.",
    },
    {
        "id": "5",
        "name": "Edward Kim",
        "title": "Machine Learning Engineer",
        "bio": "Edward trains and deploys deep learning models for computer vision.",
    },
]
```

### Step 2: Define the schema

```python
from sayt2.api import DataSet, NgramField, TextField, KeywordField

fields = [
    KeywordField(name="id"),
    NgramField(name="name", min_gram=2, max_gram=6, boost=3.0),
    TextField(name="title", boost=2.0),
    TextField(name="bio"),
]
```

### Step 3: Create the DataSet and search

```python
from pathlib import Path
import shutil

dir_index = Path("./my_search_index")
if dir_index.exists():
    shutil.rmtree(dir_index)

def downloader() -> list[dict]:
    """Return the raw records. In real use this could hit a DB or API."""
    return records
```

**Option A — context manager (recommended):**

`DataSet` supports `with` statement, which automatically closes the index and cache when the block exits.

```python
with DataSet(
    dir_root=dir_index,
    name="people",
    fields=fields,
    downloader=downloader,
    cache_expire=None,
) as ds:
    # Ngram search — substring matching (search-as-you-type)
    result = ds.search("ali")
    # Returns Alice Johnson
    for hit in result.hits:
        print(f"{hit.source['name']} (score: {hit.score:.2f})")

    # Full-text search — BM25 word-level
    result = ds.search("machine learning")
    # Returns Edward Kim, Alice Johnson

    # Check cache behavior
    r1 = ds.search("kubernetes")     # cache miss
    r2 = ds.search("kubernetes")     # cache hit (r2.cache == True)

    # Force rebuild the index
    r = ds.search("kubernetes", refresh=True)  # r.fresh == True
# ds is automatically closed here
```

**Option B — manual close:**

If you cannot use a `with` block, call `ds.close()` explicitly when done.

```python
ds = DataSet(
    dir_root=dir_index,
    name="people",
    fields=fields,
    downloader=downloader,
    cache_expire=None,
)

result = ds.search("ali")
for hit in result.hits:
    print(f"{hit.source['name']} (score: {hit.score:.2f})")

# Always close when done
ds.close()
```

## Example 2 — Book Catalog (Sort + Range Queries)

### Step 1: Define schema with NumericFields

```python
from sayt2.api import DataSet, NgramField, TextField, KeywordField, NumericField, SortKey

book_fields = [
    KeywordField(name="id"),
    NgramField(name="title", min_gram=2, max_gram=6, boost=3.0),
    TextField(name="author", boost=2.0),
    TextField(name="description"),
    NumericField(name="year", kind="i64", indexed=True, fast=True),
    NumericField(name="price", kind="f64", indexed=True, fast=True),
    NumericField(name="rating", kind="f64", indexed=True, fast=True),
]
```

### Step 2: Create DataSet with sorting

```python
books = [
    {"id": "1", "title": "Fluent Python", "author": "Luciano Ramalho",
     "description": "A guide to writing effective Python code.",
     "year": 2022, "price": 49.99, "rating": 4.7},
    {"id": "2", "title": "Python Crash Course", "author": "Eric Matthes",
     "description": "A fast-paced introduction to programming with Python.",
     "year": 2023, "price": 35.99, "rating": 4.6},
    {"id": "3", "title": "Programming Rust", "author": "Jim Blandy",
     "description": "Fast, safe systems development with Rust.",
     "year": 2021, "price": 45.99, "rating": 4.5},
]

with DataSet(
    dir_root=Path("./book_index"),
    name="books",
    fields=book_fields,
    downloader=lambda: books,
    sort=[SortKey(name="rating", descending=True)],
) as ds:
    # Sort by rating (highest first)
    result = ds.search("python")

    # Range queries (Lucene syntax)
    result = ds.search("year:[2020 TO 2023]")
    result = ds.search("price:>40")

    # Field-specific search
    result = ds.search("author:blandy")

    # Boolean operators
    result = ds.search("python AND year:[2022 TO 2025]")
```

## Quick Reference

| Feature | How to use |
|---------|-----------|
| Substring search (search-as-you-type) | `NgramField` + `ds.search("ali")` |
| Full-text search | `TextField` + `ds.search("machine learning")` |
| Exact match | `KeywordField` |
| Sort results | `SortKey(name="rating", descending=True)` |
| Range query | `ds.search("year:[2020 TO 2025]")` or `ds.search("price:>40")` |
| Field-specific search | `ds.search("author:blandy")` |
| Boolean operators | `ds.search("python AND year:[2020 TO 2025]")` |
| Force refresh | `ds.search("query", refresh=True)` |
| Automatic caching | Built-in, no config needed |

## Common Patterns

### Data from a database or API

```python
import json
import requests

def downloader() -> list[dict]:
    resp = requests.get("https://api.example.com/items")
    return resp.json()

# Or from a local JSON file
def downloader() -> list[dict]:
    with open("data.json") as f:
        return json.load(f)
```

### Refreshing the index when data changes

```python
# Normal search — uses cached index
result = ds.search("query")

# Force rebuild — re-downloads data and rebuilds index
result = ds.search("query", refresh=True)
```

### Iterating over results

```python
result = ds.search("python")
print(f"Found {result.size} results in {result.took_ms:.1f}ms")
for hit in result.hits:
    print(f"  [{hit.score:.2f}] {hit.source['title']}")
```

For full documentation, see https://sayt2.readthedocs.io/
