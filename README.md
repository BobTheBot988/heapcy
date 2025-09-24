# heapcy — Cython-accelerated heap utilities

<!--toc:start-->

- [heapcy — Cython-accelerated heap utilities](#heapcy-cython-accelerated-heap-utilities)
  - [Features](#features)
  - [Build](#build)
  - [Install](#install)
  - [Quick start](#quick-start)
  - [API reference](#api-reference)
    - [Class: `Heap(capacity: int)`](#class-heapcapacity-int)
    - [File string helpers](#file-string-helpers)
  - [Typical workflow: top-K strings by probability](#typical-workflow-top-k-strings-by-probability)
  - [Performance notes](#performance-notes)
  - [Troubleshooting](#troubleshooting)
  - [License](#license)
  <!--toc:end-->

A tiny, fast **min-heap** for `(value, offset)` pairs,
plus helpers to recover strings from a file by **byte offset**.
Implemented in Cython for speed; exposes a Pythonic API similar to `heapq`.

---

## Features

- **Min-heap** of `Entry{ double value; uint64_t offset; }`.
- Fast core operations implemented in C (`nogil` internally where safe).
- **Pythonic API**: `heappush`, `heappop`, `heappushpop`, `nlargest`, `heappeek`.
- Iterate a `Heap` (heap-array order, not sorted).
- **String helpers** to fetch tokens from a file at known byte offsets:
  - `string_generator(path, offsets, encoding="ascii")`
  - `string_getter(path, offset, encoding="ascii")`
- Handy **introspection**:
  - `len(heap)` → count of items
  - `heap.__sizeof__()` → header + buffer **bytes**
  - `heap.nbytes_used()` / `heap.nbytes_capacity()` → used vs reserved bytes
- Cython directives tuned for performance:
  `boundscheck=False`, `wraparound=False`,
  `cdivision=True`, `infer_types=True`, `embedsignature=True`.

---

## Build

```bash
cibuildwheel .
```

## Install

```bash
pip install heapcy
```

> You need a working C/C++ toolchain and Python headers for your interpreter.

---

## Quick start

```python
import heapcy

# Make a heap with capacity for 100k entries
h = heapcy.Heap(100_000)

# Push (probability, byte_offset) pairs
heapcy.heappush(h, 0.15, 123456)
heapcy.heappush(h, 0.03, 987)
heapcy.heappush(h, 0.42, 5555)

print(len(h))                # item count
print(h.nbytes_used())       # bytes occupied by current entries
print(h.nbytes_capacity())   # bytes reserved for the buffer
print(h.__sizeof__())        # header + capacity bytes reported to sys.getsizeof

# Pop the minimum (by value)
v, off = heapcy.heappop(h)   # -> (0.03, 987)

# Peek without removing (k=0 is the root/min)
v0, off0 = heapcy.heappeek(h, 0)

# Top-K largest (generator of (value, offset))
top = list(heapcy.nlargest(h, 2))   # e.g. [(0.42, 5555), (0.15, 123456)]

# Stream tokens from a file at those offsets (file must be uncompressed)
path = "/path/to/uncompressed.txt"
for token in heapcy.string_generator(path, [off for _, off in top]):
    print(token)
```

---

## API reference

### Class: `Heap(capacity: int)`

Creates an empty min-heap with fixed `capacity`. Raises `ValueError` if `capacity <= 0`.

**Methods & dunder:**

- `def __len__(self) -> int`  
  Number of items currently stored.
- `def __sizeof__(self) -> int`  
  **Bytes** reported to `sys.getsizeof(self)`: CPython header + malloc’ed entry buffer (capacity × `sizeof(Entry)`).
- `def nbytes_used(self) -> int`  
  Bytes used by current items (`occupied × sizeof(Entry)`).
- `def nbytes_capacity(self) -> int`  
  Bytes reserved for the backing array (`capacity × sizeof(Entry)`).
- `def __iter__(self)`  
  Iterate items in heap-array order (not sorted).

**Functions (module-level):**

- `heappush(heap: Heap, value: float, offset: int) -> None`  
  Insert `(value, offset)`. `value` must be in `[0.0, 1.0]`. Raises `MemoryError` if full.
- `heappop(heap: Heap) -> tuple[float, int]`  
  Remove and return the smallest `(value, offset)`. Raises `IndexError` if empty.
- `heappushpop(heap: Heap, value: float, offset: int) -> tuple[float, int]`  
  **Pop then push** (matches the current implementation’s order). Returns the popped `(value, offset)`.
- `nlargest(heap: Heap, k: int)`  
  **Generator** yielding the `k` largest `(value, offset)` in descending order. Internally builds a max-heap in place and (by default) restores the min-heap invariant afterward.
- `heappeek(heap: Heap, k: int = 0) -> tuple[float, int]`  
  Return the item at heap-array index `k` (0 is the root). Raises `IndexError` if out of range.

**Convenience:**

- `build_heap(self, array: Iterable[tuple[float, int] | tuple[int, float]] ) -> Heap`  
  Build and return a new heap from a list of pairs (accepts either `(float, int)` or `(int, float)` and normalizes them).

### File string helpers

- `string_generator(file_name: str, offsets: Iterable[int], encoding: str = "ascii") -> Iterator[str]`  
  Open the file **once**, then for each byte offset:
  - seek to `offset`
  - read a line (`b"...\n"`)
  - yield the **first space-delimited token** decoded to `str`  
    If an offset is past EOF, yields `""`.

- `string_getter(name: str, offset: int, encoding: str = "ascii") -> str`  
  Read and return the first token at a single offset.

> Use binary mode (`"rb"`) for stable byte offsets. If your file is compressed, decompress first (e.g., with `gzip` to a temp file) before random access.

---

## Typical workflow: top-K strings by probability

```python
import gzip, shutil, tempfile, os, heapcy

# Inflate a .gz so we can seek by byte offsets
with tempfile.NamedTemporaryFile(delete=False) as tmp:
    with gzip.open("example.gz", "rb") as gz:
        shutil.copyfileobj(gz, tmp, length=1024*1024)
    path = tmp.name

h = heapcy.Heap(100_000)
with open(path, "rb") as f:
    while True:
        off = f.tell()
        line = f.readline()
        if not line:
            break
        parts = line.split(b" ", 1)
        if len(parts) == 2:
            prob = float(parts[1].decode("ascii"))
            heapcy.heappush(h, prob, off)

offsets = [off for _, off in heapcy.nlargest(h, 1000)]
for token in heapcy.string_generator(path, offsets):
    print(token)

os.remove(path)
```

---

## Performance notes

- The heap array is a single `malloc`’ed block of `Entry` structs; push/pop/heapify are tight C loops.
- `__sizeof__` includes the reserved buffer so memory profilers reflect true footprint.  
  Use `nbytes_used()` vs `nbytes_capacity()` to track utilization.
- `nlargest` mutates the internal order during iteration (like a partial heapsort), but not the multiset of items.
- Random file seeks dominate cost if offsets are scattered; batch/sort offsets to improve locality.

---

## Troubleshooting

- **`ValueError: The size must be positive`** → pass a capacity > 0 to `Heap`.
- **`MemoryError: The heap is full`** → capacity is fixed; pop or allocate a larger heap.
- **`IndexError: The heap is empty`** → push before popping/peeking.
- **File helpers return empty strings** → the offset was at/after EOF, or the line was empty.

---

## License

LGPL-3.0-or-later. See `LICENSE`.
