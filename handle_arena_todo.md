# Handle/Offset Arena – To-Do Checklist

<!--toc:start-->

- [Handle/Offset Arena – To-Do Checklist](#handleoffset-arena-to-do-checklist)
  - [Data layout](#data-layout)
  - [Arena API (conceptual)](#arena-api-conceptual)
  - [Handle safety](#handle-safety)
  - [Allocation strategy](#allocation-strategy)
  - [Freeing & garbage tracking](#freeing-garbage-tracking)
  - [Compaction policy](#compaction-policy)
  - [Heap integration](#heap-integration)
  - [Python boundary](#python-boundary)
  - [Concurrency & GIL](#concurrency-gil)
  - [Error handling & invariants](#error-handling-invariants)
  - [Diagnostics & metrics](#diagnostics-metrics)
  - [Testing](#testing)
  - [Performance tuning (optional)](#performance-tuning-optional)
  <!--toc:end-->

## Data layout

- [ ] Replace `char*` in items with a **handle (int index)**.
- [ ] Add an arena **slot table** with fields: `offset`, `size`, `in_use` (and optional `generation`).
- [ ] Keep a single byte buffer in the arena: `base`, `capacity`, `used`.

## Arena API (conceptual)

- [ ] Implement `alloc(src, size) → handle`.
- [ ] Implement `free(handle)` (mark slot dead; no immediate byte moves).
- [ ] Implement `ptr(handle) → char*` (compute `base + offset`; transient use only).
- [ ] Implement `size(handle) → Py_ssize_t`.
- [ ] Implement `compact()` to pack live slots left and update slot `offset`s.
- [ ] (Optional) Implement `shrink_to_fit()` after compaction.
- [ ] Implement `reset()/clear()`.

## Handle safety

- [ ] Make handles **stable** (slot index doesn’t change).
- [ ] Decide on **generation/version** to detect stale handles, or ensure handles never escape untrusted code.
- [ ] Document: callers must not cache raw pointers across `compact()`.

## Allocation strategy

- [ ] Append-only writes into buffer (`used += size`), grow when needed (doubling/geometric).
- [ ] Maintain a **free list** of slot indices for reuse.
- [ ] Grow the slot array when out of free slots (geometric growth).

## Freeing & garbage tracking

- [ ] `free(handle)` flips `in_use = false`.
- [ ] Track `bytes_garbage += size` and `bytes_live`.

## Compaction policy

- [ ] Track **garbage ratio** = `bytes_garbage / used`.
- [ ] Choose a trigger threshold (e.g., 30–50%) and/or explicit call site.
- [ ] Compaction steps:
  - [ ] Set write cursor `w = 0`.
  - [ ] For each slot in fixed order:
    - [ ] If `in_use` and `offset != w`, move block and set `offset = w`.
    - [ ] Advance `w += size`.
  - [ ] Set `used = w`, reset `bytes_garbage = 0`.
- [ ] Optionally downsize capacity if `capacity >> used`.

## Heap integration

- [ ] Change `RawItem` to store `{value, handle, size}` (or query size from arena).
- [ ] On `push`: allocate in arena, store handle (and size).
- [ ] On item removal: `arena.free(item.handle)`.
- [ ] Leave heap algorithms (`swap/heapify`) unchanged.

## Python boundary

- [ ] Convert to Python `str` via `PyUnicode_FromStringAndSize(ptr(handle), size)` under the GIL.
- [ ] When accepting `str/bytes`, obtain pointer & length briefly, then copy via `alloc`.

## Concurrency & GIL

- [ ] Ensure `PyMem_*` and Python conversions run **with GIL**.
- [ ] Keep `alloc/free/compact` otherwise GIL-light; guard `realloc` with `with gil`.
- [ ] Prohibit holding `char*` across `compact()` or concurrent compaction.

## Error handling & invariants

- [ ] On allocation failure, leave state unchanged and raise under GIL.
- [ ] Maintain invariants:
  - [ ] For live slots: `0 ≤ offset < used` and `offset + size ≤ used`.
  - [ ] Slot is either `in_use` or on free list, not both.
  - [ ] Always `used ≤ capacity`.

## Diagnostics & metrics

- [ ] Track `alloc_count`, `free_count`, `bytes_live`, `bytes_garbage`, `max_capacity`.
- [ ] Add a debug validator to scan slots and verify non-overlap & invariants.

## Testing

- [ ] Unit tests for alloc/free/compact sequences and growth paths.
- [ ] Randomized tests interleaving heap `push/pop` with arena `compact()`.
- [ ] Realloc movement tests (contents preserved).
- [ ] Handle reuse tests (with/without generations).
- [ ] Stress tests with many tiny & large strings; measure fragmentation & compaction time.

## Performance tuning (optional)

- [ ] Consider size-classed arenas for small vs large strings.
- [ ] Consider small-string inline storage in item (flagged).
- [ ] Consider dedup/interning if duplicates are common.
- [ ] Define snapshot/rollback if temporary allocations are frequent.
