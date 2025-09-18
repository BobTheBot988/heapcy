# heapc.pyx
# -----------------------------------------------------------------------------
# A compact, arena-backed min-heap for (float, string) items with a heapq-style
# API. Strings are copied once into a grow-only arena; heap items store
# (offset, length, value). Public interface accepts/returns (value, string).
#
# Usage (Python):
#   import heapc
#   h = heapc.heapify([(0.9, "foo"), (0.5, "bar")], consume=True)
#   heapc.heappush(h, (0.8, "baz"))
#   v, s = heapc.heappop(h)                 # (0.5, "bar")
#   top = list(h.iter_nlargest(2))          # [(0.9, "foo"), (0.8, "baz")]
#   names = h.nlargest_strings(2)           # ["foo", "baz"]
#
# Notes:
# - Internal structure is a MIN-HEAP by value (v).
# - All string decoding on output uses ASCII (UTF-8 ASCII subset is fine).
# - Non-destructive top-k provided, plus destructive pop_nsmallest/pop_nlargest.
# -----------------------------------------------------------------------------

# distutils: language = c
# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, infer_types=True

from libc.stdlib cimport malloc, realloc, free
from libc.string cimport memcpy
from cpython.unicode cimport PyUnicode_AsUTF8AndSize, PyUnicode_DecodeASCII
from cpython.bytes cimport PyBytes_AsStringAndSize
cimport cython

# ------------------------------- Data layout ---------------------------------
# Raw heap node:
#   off : Py_ssize_t   - byte offset into arena base
#   n   : Py_ssize_t   - string length in bytes (no NUL required)
#   v   : double       - probability in [0,1] (no enforcement here)

cdef struct RawHeapItem:
    Py_ssize_t off   # byte offset into arena base
    Py_ssize_t n     # string length in bytes
    double v         # probability

cdef inline int cmp_items(const RawHeapItem* a, const RawHeapItem* b) nogil:
    # Return -1 if a < b, 1 if a > b, 0 if equal.
    if a[0].v < b[0].v: return -1
    if a[0].v > b[0].v: return 1
    return 0

cdef inline void swap(RawHeapItem* arr, Py_ssize_t i, Py_ssize_t j) nogil:
    # Swap two items in-place (tiny hot helper).
    cdef RawHeapItem t = arr[i]
    arr[i] = arr[j]
    arr[j] = t

# --------------------------------- Arena -------------------------------------

cdef class _Arena:
    # Grow-only byte arena for storing strings contiguously.
    # Offsets remain valid across resizes; all memory is released when the
    # arena (or owning Heap) is destroyed or when .clear() is called.
    char* base
    cdef Py_ssize_t cap, used

    def __cinit__(self, Py_ssize_t initial=0):
        self.base = <char*>NULL
        self.cap = self.used = 0
        if initial > 0:
            self._reserve(initial)

    cdef void _reserve(self, Py_ssize_t need) except *:
        # Ensure capacity >= need bytes. Geometric growth; first chunk is 64 KiB.
        if need <= self.cap:
            return

        cdef Py_ssize_t newcap = self.cap * 2 if self.cap else 65536
        if newcap < need:
            newcap = need

        void* p
        if self.base == NULL:
            p = malloc(newcap)
        else:
            p = realloc(self.base, newcap)

        if p == NULL:
            raise MemoryError()

        self.base = <char*>p
        self.cap = newcap

    cdef Py_ssize_t add(self, const char* src, Py_ssize_t n) except *:
        # Append n bytes from src to arena, return starting offset.
        cdef Py_ssize_t start = self.used
        self._reserve(self.used + n)
        if n:
            memcpy(self.base + start, src, n)
        self.used += n
        return start

    cdef void clear(self):
        # Logically clear contents (keeps allocation; next add() starts at 0).
        self.used = 0

    def __dealloc__(self):
        if self.base != NULL:
            free(self.base)

# --------------------------------- Heap --------------------------------------

cdef class Heap:
    # Min-heap of (value: float, string), backed by a contiguous RawHeapItem array
    # and a grow-only arena for string bytes.
    #
    # Public methods/iterators accept/return (value, string). The internal
    # structure stores (off, n, v) for compactness.
    #
    # Important:
    # - Iteration (for x in heap) yields in *heap-array order* (NOT sorted).
    # - Non-destructive top-k: iter_nsmallest / iter_nlargest (+ *_strings).
    # - Destructive top-k: pop_nsmallest/pop_nlargest (+ *_strings).
    cdef RawHeapItem* _data
    cdef Py_ssize_t _size, _cap
    cdef _Arena _arena
    cdef unsigned long _modcount

    def __cinit__(self, Py_ssize_t initial_capacity=0, Py_ssize_t arena_bytes=0):
        # Create an empty heap.
        # initial_capacity : anticipated number of items (for node array)
        # arena_bytes      : anticipated total bytes for strings
        self._data = <RawHeapItem*>NULL
        self._size = 0
        self._cap = 0
        self._arena = _Arena(arena_bytes if arena_bytes > 0 else 0)
        self._modcount = 0
        if initial_capacity > 0:
            self._reserve_items(initial_capacity)

    def __dealloc__(self):
        if self._data != NULL:
            free(self._data)

    # ------------------------- internal helpers -------------------------

    cdef void _reserve_items(self, Py_ssize_t newcap) except *:
        # Ensure capacity for at least newcap heap nodes (geometric growth).
        if newcap <= self._cap:
            return
        void* p
        if self._data == NULL:
            p = malloc(newcap * cython.sizeof(RawHeapItem))
        else:
            p = realloc(self._data, newcap * cython.sizeof(RawHeapItem))
        if p == NULL:
            raise MemoryError()
        self._data = <RawHeapItem*>p
        self._cap = newcap

    cdef void _sift_up(self, Py_ssize_t i) nogil:
        # Sift a node up toward the root to restore the heap invariant.
        cdef Py_ssize_t parent
        while i > 0:
            parent = (i - 1) >> 1
            if cmp_items(&self._data[i], &self._data[parent]) < 0:
                swap(self._data, i, parent)
                i = parent
            else:
                break

    cdef void _sift_down(self, Py_ssize_t i) nogil:
        # Sift a node down the tree (classic binary-heap sift-down).
        cdef Py_ssize_t size = self._size
        cdef Py_ssize_t child
        while True:
            child = (i << 1) + 1
            if child >= size:
                break
            if child + 1 < size and cmp_items(&self._data[child+1], &self._data[child]) < 0:
                child += 1
            if cmp_items(&self._data[child], &self._data[i]) < 0:
                swap(self._data, i, child)
                i = child
            else:
                break

    cdef object _py_item(self, Py_ssize_t idx):
        # Build a Python (value, string) from internal slot idx.
        cdef RawHeapItem* it = &self._data[idx]
        cdef object s = PyUnicode_DecodeASCII(self._arena.base + it.off, it.n, "strict")
        return (it.v, s)

    # ---------------------------- API methods ---------------------------

    def __len__(self):
        # len(heap) -> number of items.
        return self._size

    def clear(self):
        # Remove all items and reset the arena (O(1)).
        # Note: arena capacity is kept; memory is reclaimed on object deletion.
        self._size = 0
        self._arena.clear()
        self._modcount += 1

    def push(self, double v, s)->void:
        # Push a single (value, string) item.
        # `s` may be `str` (ASCII/UTF-8) or `bytes` (ASCII).
        if self._size == self._cap:
            self._reserve_items(self._cap * 2 + 8)

        cdef Py_ssize_t n
        if isinstance(s, bytes):
            char* p
            PyBytes_AsStringAndSize(s, &p, &n)
            self._data[self._size].off = self._arena.add(<const char*>p, n)
        else:
            char* p = PyUnicode_AsUTF8AndSize(s, &n)
            if p == NULL:
                raise ValueError("invalid ASCII/UTF-8 string")
            self._data[self._size].off = self._arena.add(p, n)

        self._data[self._size].n = n
        self._data[self._size].v = v

        cdef Py_ssize_t i = self._size
        self._size += 1
        with nogil:
            self._sift_up(i)
        self._modcount += 1

    def pop(self):
        # Pop and return the smallest (value, string) item.
        # Raises IndexError if the heap is empty.
        if self._size == 0:
            raise IndexError("pop from empty heap")

        cdef RawHeapItem root = self._data[0]
        self._size -= 1
        if self._size > 0:
            self._data[0] = self._data[self._size]
            with nogil:
                self._sift_down(0)
        self._modcount += 1
        cdef object s = PyUnicode_DecodeASCII(self._arena.base + root.off, root.n, "strict")
        return (root.v, s)

    def replace(self, double v, s):
        # Pop and return the smallest (value, string), then push (v, s).
        # Faster than pop() followed by push().
        if self._size == 0:
            raise IndexError("replace on empty heap")

        cdef RawHeapItem old = self._data[0]

        cdef Py_ssize_t n
        if isinstance(s, bytes):
            char* p
            PyBytes_AsStringAndSize(s, &p, &n)
            self._data[0].off = self._arena.add(<const char*>p, n)
        else:
            char* p = PyUnicode_AsUTF8AndSize(s, &n)
            if p == NULL:
                raise ValueError("invalid ASCII/UTF-8 string")
            self._data[0].off = self._arena.add(p, n)

        self._data[0].n = n
        self._data[0].v = v
        with nogil:
            self._sift_down(0)
        self._modcount += 1

        cdef object s_old = PyUnicode_DecodeASCII(self._arena.base + old.off, old.n, "strict")
        return (old.v, s_old)

    def pushpop(self, double v, s):
        # Push (v, s) and then pop and return the smallest (value, string).
        # More efficient than push()+pop().
        if self._size and v > self._data[0].v:
            # Fast path: pop current min, then insert (v, s)
            res = self.pop()
            self.push(v, s)
            return res
        # Else, (v, s) would pop immediately; avoid mutation:
        return (v, s)

    def peek_value(self):
        # Return the smallest value without popping (IndexError if empty).
        if self._size == 0:
            raise IndexError("peek on empty heap")
        return self._data[0].v

    def peek(self):
        # Return the smallest (value, string) without popping.
        if self._size == 0:
            raise IndexError("peek on empty heap")
        return self._py_item(0)

    @classmethod

    def from_pairs(cls, pairs, bint consume=False):
        # Build a Heap from an iterable of items.
        # Accepts:
        #  - (value: float, string)
        #  - (string, value)
        #  - objects with attributes .prob and .string_string / .password_string
        #
        # If consume=True and `pairs` supports .clear(), it will be emptied.
        cdef Py_ssize_t n = -1
        try:
            n = len(pairs)
        except Exception:
            pass

        cdef Heap h = cls(initial_capacity=(n if n > 0 else 0),
                          arena_bytes=(n * 8 if n > 0 else 0))

        # helper to normalize one item to (v, s)
        def _split(item):
            cdef Py_ssize_t start = (n >> 1) - 1
            cdef Py_ssize_t i,L
            try:
                a, b = item
                if isinstance(a, (int, float)) and isinstance(b, (bytes, str)):
                    return float(a), b
                if isinstance(b, (int, float)) and isinstance(a, (bytes, str)):
                    return float(b), a
            except Exception:
                pass

            v = getattr(item, "prob")
            s = getattr(item, "string_string", getattr(item, "password_string"))
            return float(v), s

        if n > 0 and hasattr(pairs, "__getitem__"):
            # Fast path with O(n) heapify
            h._reserve_items(n)
            for i in range(n):
                v, s = _split(pairs[i])
                if isinstance(s, bytes):
                    char* p
                    PyBytes_AsStringAndSize(s, &p, &L)
                    h._data[i].off = h._arena.add(<const char*>p, L)
                else:
                    char* p = PyUnicode_AsUTF8AndSize(s, &L)
                    if p == NULL:
                        raise ValueError("invalid ASCII/UTF-8 string")

                    h._data[i].off = h._arena.add(p, L)
                h._data[i].n = L
                h._data[i].v = <double>v
            h._size = n
            # Bottom-up heapify
            with nogil:
                while start >= 0:
                    h._sift_down(start)
                    start -= 1
        else:
            # Generic slow path
            for item in pairs:
                v, s = _split(item)
                h.push(v, s)

        h._modcount += 1

        if consume and hasattr(pairs, "clear"):
            try:
                pairs.clear()
            except Exception:
                pass

        return h

    # ----------------------------- Iteration ----------------------------

    def __iter__(self):
        # Iterate items in internal heap-array order (NOT sorted).
        # Mutating the heap during iteration raises RuntimeError.
        return HeapIter(self)

    def iter_nsmallest(self, Py_ssize_t k):
        # Yield up to k items (value, string) with the *smallest* values,
        # without modifying the heap.
        #
        # Algorithm:
        #   - Maintain a frontier over indices using a Python min-heap (value, idx),
        #     starting from the root (0).
        #   - Each time we pop, we push that node's children.
        # Complexity: O(k log k) time, O(k) extra memory.
        import heapq as _hq
        cdef Py_ssize_t left = (i << 1) + 1
        cdef Py_ssize_t right = left + 1

        cdef Py_ssize_t size = self._size
        if k <= 0 or size == 0:
            return

        cdef RawHeapItem* data = self._data
        char* base = self._arena.base

        cands = [(data[0].v, 0)]
        while k and cands:
            vv, i = _hq.heappop(cands)
            s = PyUnicode_DecodeASCII(base + data[i].off, data[i].n, "strict")
            yield (vv, s)
            k -= 1
            if left < size:
                _hq.heappush(cands, (data[left].v, left))
            if right < size:
                _hq.heappush(cands, (data[right].v, right))

    def nsmallest_strings(self, Py_ssize_t k):
        # Return a list[str] of the k smallest strings (non-destructive).
        return [s for (v, s) in self.iter_nsmallest(k)]

    def iter_nlargest(self, Py_ssize_t k):
        # Yield up to k items (value, string) with the *largest* values,
        # without modifying the heap.
        #
        # Because the structure is a min-heap, we do a single pass over the
        # array and keep a Python min-heap of size <= k with (value, idx).
        # Complexity: O(N log k) time, O(k) extra memory.
        import heapq as _hq

        cdef Py_ssize_t size = self._size
        if k <= 0 or size == 0:
            return

        cdef RawHeapItem* data = self._data
        char* base = self._arena.base

        res = []
        push = _hq.heappush
        replace = _hq.heapreplace

        cdef Py_ssize_t i
        cdef double vv
        for i in range(size):
            vv = data[i].v
            if len(res) < k:
                push(res, (vv, i))
            else:
                if vv > res[0][0]:
                    replace(res, (vv, i))

        for vv, i in sorted(res, reverse=True):
            s = PyUnicode_DecodeASCII(base + data[i].off, data[i].n, "strict")
            yield (vv, s)

    def nlargest_strings(self, Py_ssize_t k):
        # Return a list[str] of the k largest strings (non-destructive).
        return [s for (v, s) in self.iter_nlargest(k)]

    # ------------------------ Destructive top-k -------------------------

    def pop_nsmallest(self, Py_ssize_t k):
        # Pop and return up to k *smallest* items as (value, string) tuples.
        # Complexity: O(k log N).
        cdef Py_ssize_t m = k if k < self._size else self._size
        out = []
        cdef Py_ssize_t j
        for j in range(m):
            out.append(self.pop())
        return out

    def pop_nsmallest_strings(self, Py_ssize_t k):
        # Pop and return up to k *smallest* strings only.
        cdef Py_ssize_t m = k if k < self._size else self._size
        out = []
        cdef Py_ssize_t j
        cdef object s
        cdef double v
        for j in range(m):
            v, s = self.pop()
            out.append(s)
        return out

    def pop_nlargest(self, Py_ssize_t k):
        # Pop and return up to k *largest* items as (value, string) tuples.
        # Destructive: removed items are deleted from this heap.
        #
        # Algorithm:
        #   1) Single pass to collect top-k indices in a Python min-heap.
        #   2) Sort descending to form results.
        #   3) Build a removal mask and compact survivors in-place.
        #   4) Bottom-up heapify survivors (O(n)).
        # Complexity: O(N log k) + O(N) time, O(k) extra memory.
        cdef Py_ssize_t size = self._size
        out = []
        if k <= 0 or size == 0:
            return out

        import heapq as _hq
        cdef RawHeapItem* data = self._data
        char* base = self._arena.base

        res = []
        push = _hq.heappush
        replace = _hq.heapreplace

        cdef Py_ssize_t i
        cdef double vv
        for i in range(size):
            vv = data[i].v
            if len(res) < k:
                push(res, (vv, i))
            else:
                if vv > res[0][0]:
                    replace(res, (vv, i))

        res.sort(reverse=True)
        out = [(vv, PyUnicode_DecodeASCII(base + data[i].off, data[i].n, "strict"))
               for vv, i in res]

        # removal mask
        cdef bytearray mask_py = bytearray(size)
        for _, i in res:
            mask_py[i] = 1
        cdef unsigned char[:] mask = mask_py

        # compact survivors
        cdef Py_ssize_t w = 0
        for i in range(size):
            if not mask[i]:
                self._data[w] = self._data[i]
                w += 1
        self._size = w

        # restore heap property
        if w <1:
            raise ValueError("w must be > 1")
        cdef Py_ssize_t start = (w >> 1) - 1
        if w > 1:
            with nogil:
                while start >= 0:
                    self._sift_down(start)
                    start -= 1

        self._modcount += 1
        return out

    def pop_nlargest_strings(self, Py_ssize_t k):
        # Pop and return up to k *largest* strings only (destructive).
        cdef Py_ssize_t size = self._size
        out = []
        if k <= 0 or size == 0:
            return out

        import heapq as _hq
        cdef RawHeapItem* data = self._data
        char* base = self._arena.base

        res = []
        push = _hq.heappush
        replace = _hq.heapreplace

        cdef Py_ssize_t i
        cdef double vv
        for i in range(size):
            vv = data[i].v
            if len(res) < k:
                push(res, (vv, i))
            else:
                if vv > res[0][0]:
                    replace(res, (vv, i))

        res.sort(reverse=True)
        out = [PyUnicode_DecodeASCII(base + data[i].off, data[i].n, "strict")
               for vv, i in res]

        # removal mask
        cdef bytearray mask_py = bytearray(size)
        for _, i in res:
            mask_py[i] = 1
        cdef unsigned char[:] mask = mask_py

        # compact survivors
        cdef Py_ssize_t w = 0
        for i in range(size):
            if not mask[i]:
                self._data[w] = self._data[i]
                w += 1
        self._size = w
        if w<1:
            raise ValueError("w > 1")
        cdef Py_ssize_t start = (w >> 1) - 1
        if w > 1:
            with nogil:
                while start >= 0:
                    self._sift_down(start)
                    start -= 1

        self._modcount += 1
        return out

# --------------------------- Iteration helper -------------------------------

cdef class HeapIter:
    # Iterator over a Heap in heap-array order (not sorted).
    # Detects concurrent modification and raises RuntimeError if mutated.
    cdef Heap _h
    cdef Py_ssize_t _i
    cdef unsigned long _expect_mod

    def __cinit__(self, Heap h):
        self._h = h
        self._i = 0
        self._expect_mod = h._modcount

    def __iter__(self):
        return self

    def __next__(self):
        if self._expect_mod != self._h._modcount:
            raise RuntimeError("Heap mutated during iteration")
        if self._i >= self._h._size:
            raise StopIteration()
        cdef Py_ssize_t idx = self._i
        self._i += 1
        return self._h._py_item(idx)

# ------------------------- heapq-style wrappers ------------------------------

def heapify(seq, consume=False):
    # heapc.heapify(seq, consume=False) -> Heap
    # Build a new Heap from an iterable of (value, string) (also accepts (string, value)).
    # If consume=True and seq supports .clear(), it will be emptied.
    return Heap.from_pairs(seq, consume=consume)

@cython.cfunc
cdef tuple _parse_item(object item):
    # Normalize an item to (value: float, string: object).
    # Accepts (v, s) or (s, v); raises TypeError for other shapes.
    cdef object a
    cdef object b
    try:
        a, b = item
    except Exception:
        raise TypeError("item must be a 2-tuple (float, str) or (str, float)")
    if isinstance(a, (int, float)) and isinstance(b, (bytes, str)):
        return (float(a), b)
    if isinstance(b, (int, float)) and isinstance(a, (bytes, str)):
        return (float(b), a)
    raise TypeError("expected (float, str) or (str, float)")

def heappush(h, item):
    # heappush(heap, (value, string)) -> None
    # Also accepts (string, value).
    if not isinstance(h, Heap):
        raise TypeError("heappush requires a Heap")
    v, s = _parse_item(item)
    h.push(<double>v, s)

def heappop(h):
    # heappop(heap) -> (value, string)
    if not isinstance(h, Heap):
        raise TypeError("heappop requires a Heap")
    return h.pop()

def heapreplace(h, item):
    # heapreplace(heap, (value, string)) -> (value, string)
    # Pop and return the smallest item, then push the new item.
    if not isinstance(h, Heap):
        raise TypeError("heapreplace requires a Heap")
    v, s = _parse_item(item)
    return h.replace(<double>v, s)

def heappushpop(h, item):
    # heappushpop(heap, (value, string)) -> (value, string)
    # Push then pop the smallest item (more efficient than push+pop).
    if not isinstance(h, Heap):
        raise TypeError("heappushpop requires a Heap")
    v, s = _parse_item(item)
    return h.pushpop(<double>v, s)

def nsmallest(n, iterable, key=None):
    # nsmallest(n, iterable[, key]) -> list
    # If iterable is a Heap and key is None, uses Heap.iter_nsmallest for speed.
    # Otherwise, falls back to Python's heapq.nsmallest.
    import heapq as _hq
    if key is None and isinstance(iterable, Heap):
        return list(iterable.iter_nsmallest(n))
    return _hq.nsmallest(n, iterable, key=key)

def nlargest(n, iterable, key=None):
    # nlargest(n, iterable[, key]) -> list
    # If iterable is a Heap and key is None, uses Heap.iter_nlargest for speed.
    # Otherwise, falls back to Python's heapq.nlargest.
    import heapq as _hq
    if key is None and isinstance(iterable, Heap):
        return list(iterable.iter_nlargest(n))
    return _hq.nlargest(n, iterable, key=key)
