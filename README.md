# Top‑K Selection with 4 GiB Segments and a 16 GB Min‑Heap

<!--toc:start-->

- [Top‑K Selection with 4 GiB Segments and a 16 GB Min‑Heap](#topk-selection-with-4-gib-segments-and-a-16-gb-minheap)
  - [Disclaimer](#disclaimer)
  - [Overview](#overview)
  - [Why 4 GiB segments?](#why-4-gib-segments)
  - [Record Layout](#record-layout)
  - [Handle Format](#handle-format)
  - [Heap Layout (≈16 GB for K = 10^9)](#heap-layout-16-gb-for-k-109)
  - [Splitting & Writing (Single Pass)](#splitting-writing-single-pass)
  - [Reading by Handle (O(1) Seek)](#reading-by-handle-o1-seek)
  - [Optional: Seekable Compression Later](#optional-seekable-compression-later)
  - [TODO Checklist](#todo-checklist)
    - [Splitting Strategy](#splitting-strategy)
    - [Segment Format](#segment-format)
    - [Handle Packing](#handle-packing)
    - [Heap (16 GB)](#heap-16-gb)
    - [Readback (no caching)](#readback-no-caching)
    - [Tests & Guards](#tests-guards)
  - [Notes](#notes)
  - [License](#license)
  <!--toc:end-->

This document describes a straightforward pipeline to pick the **top‑K** strings (by score) from very large streams while keeping RAM bounded. It prioritizes simplicity and true **O(1)** random access via byte offsets.

---

## Disclaimer

THIS README WAS MADE WITH CHATGPT I WILL FIX ANY MISTAKES LATER

## Overview

- **Storage:** split data into **uncompressed, seekable binary segments** `data.0000`, `data.0001`, … with size ≤ **4 GiB** each.
- **Record format:** per record → `[length:uint8][payload:length bytes]`. Length is bytes (e.g., UTF‑8), max 255.
- **Heap:** a fixed‑size **min‑heap** that stores only `(score: float64, handle: uint64)`; no strings in RAM.
- **Handle packing:** `handle = (seg_id << 32) | offset32` (which file + byte offset inside that file).
- **Reading:** open segment in binary, `seek(offset32)`, read 1 byte length, then the payload; decode to text if needed.
- **Compression:** **not** used during selection (to preserve O(1) seek). Optional seekable compression (e.g., BGZF or indexed gzip) can be adopted later for archival.

---

## Why 4 GiB segments?

- Keeps byte offsets inside each segment within **32 bits** (`offset32`), making the in‑RAM handle compact (64‑bit total).
- Enables direct `seek()` to `offset32` without scanning or indexing lines.
- Plays nicely with OS page cache and `mmap` if you choose to use it later.

---

## Record Layout

```
[length : uint8] [payload : length bytes]
```

- No newline, no NUL terminator.
- Length measured **after encoding** (UTF‑8). Enforce `length ≤ 255` (truncate/skip by policy).

---

## Handle Format

```
handle = (seg_id << 32) | offset32
seg_id  = handle >> 32
offset  = handle & 0xFFFFFFFF
```

- `seg_id` → which `data.%04d` file.
- `offset32` → start byte of the record (the `uint8` length) inside that file.

---

## Heap Layout (≈16 GB for K = 10^9)

Array of entries (AoS), each **16 bytes**:

```
struct Entry {
  double  score;   // 8 B
  uint64  handle;  // 8 B
}
```

- Compare **only `score`** in heap operations.
- Pre‑allocate once; avoid `realloc`.
- Provide thin Python wrappers; keep core methods `nogil` in Cython.

> If you later switch to SoA + `float32` scores, heap footprint can drop to ≈12 GB (4 GB scores + 8 GB handles).

---

## Splitting & Writing (Single Pass)

1. Choose `MAX_SEG = 4 * 1024**3` (optionally minus a safety margin).
2. Start `seg_id = 0`, `offset = 0`, open `data.0000` in binary write mode.
3. For each string:
   - Encode (`utf‑8`) → `b`; `n = len(b)`.
   - Enforce `n ≤ 255` (truncate/skip/error per policy).
   - If `offset + 1 + n > MAX_SEG`: close current file, `seg_id += 1`, open `data.%04d`, reset `offset = 0`.
   - Record **pre‑write** `offset32 = offset`.
   - Write `n` (1 byte), then `b`.
   - Update `offset += 1 + n`.
   - Compute `handle = (seg_id << 32) | offset32` and **push `(score, handle)` into the heap**.

> Alternative: if starting from a `.gz`, stream‑decompress once (binary mode) and re‑emit records into these uncompressed segments.

---

## Reading by Handle (O(1) Seek)

Given `(score, handle)`:

1. `seg_id = handle >> 32`
2. `offset = handle & 0xFFFFFFFF`
3. Open `data.%04d` in **binary** (`'rb'`), `seek(offset)`.
4. Read 1 byte → `n`; read `n` bytes → payload.
5. Decode payload to text if needed.

No caching is required; open‑read‑close is acceptable for simplicity.

---

## Optional: Seekable Compression Later

If you need compressed storage **with** random access afterwards, consider:

- **BGZF (block‑gzip)** via libraries that expose indexed random access.
- **Indexed gzip** readers that maintain a sidecar index.
- **Zstandard (seekable)** with an index.

> Keep offsets relative to the uncompressed stream positions exposed by the chosen library’s API.

---

## TODO Checklist

### Splitting Strategy

- [ ] Decide when to split:
  - [ ] Convert existing `.gz` → stream‑decompress and write uncompressed segments.
  - [ ] Or split **during generation** as soon as the next record would exceed 4 GiB.
- [ ] Enforce **max string length ≤ 255 bytes** post‑encoding (UTF‑8).
- [ ] File naming: `data.0000`, `data.0001`, …

### Segment Format

- [ ] Implement writer for `[uint8 length][payload bytes]` records.
- [ ] Maintain running `offset`; roll segment when `offset + 1 + len > 4 GiB`.
- [ ] Store `offset32` (pre‑write) in the handle.

### Handle Packing

- [ ] Pack `handle = (seg_id << 32) | offset32`.
- [ ] Unpack with shifts/masks when reading.
- [ ] Assert segment size ≤ 4 GiB.

### Heap (16 GB)

- [ ] Fixed entry `{double score, uint64 handle}`; 16 bytes.
- [ ] `push/pop/peek` with `nogil` core; Python wrappers at edges.
- [ ] Compare only `score`.

### Readback (no caching)

- [ ] For each top‑K, open the segment in `'rb'`, `seek(offset)`, read `n`, then `n` bytes.
- [ ] Decode to text; build final result list / `(score, string)` pairs.

### Tests & Guards

- [ ] Round‑trip write/read correctness (random data).
- [ ] Handle pack/unpack (bit‑math) tests.
- [ ] Segment rollover boundary tests.
- [ ] Heap correctness & fuzz tests.
- [ ] Assert `sizeof(Entry) == 16` at init.

---

## Notes

- Use **binary** I/O for both writing and reading—avoid text mode to preserve byte offsets.
- During selection, keep segments **uncompressed** to maintain true O(1) seeking.
- You can compress final outputs later, or switch to a seekable format if needed.
- If opening files repeatedly becomes a bottleneck, you can add a simple FD cache later—**not required** for correctness.

---

## License

LGPL
