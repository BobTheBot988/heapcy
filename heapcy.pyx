# distutils: language = c
# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, infer_types=True, c_string_encoding=ascii, embedsignature=True

from cpython.mem cimport PyMem_Malloc,PyMem_Free
from libc.stdint cimport uint64_t

cdef struct Entry:
    double value
    uint64_t offset

cdef class Heap:
    cdef Entry* lis
    cdef Py_ssize_t _size_of_heap, _occupied,_modcount

    def __cinit__(self,Py_ssize_t size_of_heap) :
        if size_of_heap <= 0:
            raise ValueError("The size must be positive")

        self.lis = < Entry* > PyMem_Malloc(size_of_heap*sizeof(Entry))
        if self.lis == NULL:
            raise MemoryError("Could not allocate memory to Heap")

        self._size_of_heap = size_of_heap
        self._occupied = 0
        self._modcount = 0

    cdef void heapify(self,Py_ssize_t i) nogil:
        cdef  Py_ssize_t smallest,left,right
        while True:
            left = (i<<1)+1
            right = left+1
            smallest = i

            if left < self._occupied and self.lis[left].value < self.lis[smallest].value:
                smallest = left

            if  right < self._occupied and self.lis[right].value < self.lis[smallest].value:
                smallest = right

            if smallest == i:
                break

            self.swap(i, smallest)
            i = smallest
            
    cdef inline void swap(self,Py_ssize_t a, Py_ssize_t b)nogil:
        cdef Entry temp = self.lis[a]
        self.lis[a] = self.lis[b] 
        self.lis[b] = temp

    cdef void push(self,double new_val, uint64_t offset)nogil:
        cdef Entry item,parent
        cdef Py_ssize_t item_position, parent_position

        with gil:
            if(self._occupied >= self._size_of_heap):
                raise MemoryError("The heap is full")
            if new_val > 1 or new_val < 0:
                raise ValueError("The value must be between 0 <= value <= 1")



        item.value = new_val
        item.offset = offset 
        
        item_position = self._occupied 
        self.lis[item_position] = item
        self._occupied+=1
        if item_position == 0 :
            return

        parent_position = (item_position-1)>>1
        parent = self.lis[parent_position]

        while item_position > 0:
            parent_position = (item_position - 1) >> 1
            parent = self.lis[parent_position]
            if item.value >= parent.value:
                break
            self.swap(item_position, parent_position)
            item_position = parent_position
        
        self._modcount+=1

    cdef Entry pop(self) nogil:
        cdef Entry item
        cdef Py_ssize_t last

        if self._occupied == 0:
            with gil: raise IndexError("The heap is empty")
    
        item = self.lis[0]
        last = self._occupied - 1
        self._occupied -= 1

        if self._occupied > 0:
            self.lis[0] = self.lis[last]
            self.heapify(0)

        self._modcount += 1

        return item
    
    cpdef Heap build_heap(self,array:list):
        """
        Parameters: listof float,int or int,float
        Description: builds  a min-heap from a list of int,float float,int
        return Heap 
        """
        cdef double val1
        cdef uint64_t val2 
        cdef Heap heap = Heap(len(array))

        for item in array:
            val1 = item[0]
            val2 = item[1]
            if isinstance(item[0],int) and isinstance(item[1],float):
                val1 = item[1]
                val2 = item[0]

            heap.push(val1,val2)

        return heap
    cdef Entry _peek_element(self,Py_ssize_t k)nogil:
        return self.lis[k]

    cdef inline void _heapify_max_self(self, Py_ssize_t n, Py_ssize_t i) nogil:
        cdef Py_ssize_t largest, l, r
        cdef Entry tmp
        while True:
            l = (i << 1) + 1
            r = l + 1
            largest = i
            if l < n and self.lis[l].value > self.lis[largest].value:
                largest = l
            if r < n and self.lis[r].value > self.lis[largest].value:
                largest = r
            if largest == i:
                break
            tmp = self.lis[i]; self.lis[i] = self.lis[largest]; self.lis[largest] = tmp
            i = largest

    cdef inline void _heapify_min_self(self, Py_ssize_t n, Py_ssize_t i) nogil:
        # same as your current heapify but with explicit n (not self._occupied),
        # so we can rebuild from arbitrary array states.
        cdef Py_ssize_t smallest, l, r
        cdef Entry tmp
        while True:
            l = (i << 1) + 1
            r = l + 1
            smallest = i
            if l < n and self.lis[l].value < self.lis[smallest].value:
                smallest = l
            if r < n and self.lis[r].value < self.lis[smallest].value:
                smallest = r
            if smallest == i:
                break
            tmp = self.lis[i]; self.lis[i] = self.lis[smallest]; self.lis[smallest] = tmp
            i = smallest

    def get_n_largest(self, Py_ssize_t k, bint restore=True):
      #  In-place, O(1) extra memory.
      #  Temporarily builds a max-heap in self.lis[0:n], yields k largest (value, str),
      #  then (optionally) restores the min-heap property.

      #  Note: This is NON-destructive to the multiset of items, but it mutates
      #  the array order during iteration.
        cdef Py_ssize_t i
        cdef Py_ssize_t n = self._occupied

        if k < 0:
            raise ValueError("k must be non-negative")
        if n == 0:
            return
        if k > n:
            k = n

        # Build max-heap in-place over current items
        with nogil:
            i = (n >> 1) - 1
            while i >= 0:
                self._heapify_max_self( n, i)
                i -= 1

        # Pop max k times (classic heapsort step): swap root with end, shrink heap
        cdef Py_ssize_t m = n
        cdef Entry it
        cdef uint64_t py_s

        for _ in range(k):
            it = self.lis[0]
            m -= 1
            if m >= 0:
                self.lis[0] = self.lis[m]
                with nogil:
                    self._heapify_max_self( m, 0)

            # convert to Python tuple (value, offset)
            yield (it.value, it.offset)

        # Optionally restore min-heap invariant over all n items
        if restore:
            with nogil:
                i = (n >> 1) - 1
                while i >= 0:
                    self._heapify_min_self( n, i)
                    i -= 1

    cdef tuple _py_item(self,Py_ssize_t idx):
        if idx < 0 or idx >= self._occupied:
            raise IndexError

        cdef Entry e = self.lis[idx]
        return (e.value, e.offset)

    def __iter__(self):
        return HeapIter(self)

    def __dealloc__(self):
        if self.lis == NULL:
            return
        PyMem_Free(self.lis)
        self.lis = NULL

    def __repr__(self)->str:
        built_string = str([(self.lis[i])for i in range(self._occupied)])
        return built_string

    def __sizeof__(self)->int:
        return self._occupied

    def __len__(self)->int:
        return self.__sizeof__()
    



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
        cdef Py_ssize_t idx = self._i
        if self._expect_mod != self._h._modcount:
            raise RuntimeError("Heap mutated during iteration")
        if self._i >= self._h._occupied:
            raise StopIteration()
        self._i += 1
        return self._h._py_item(idx)

cpdef heappush(Heap heap,double value,uint64_t offset) :
    """
    Parameters: 
    -Heap an Heap object
    -double value 
    -uint64_t a positive offset this represent the position of a string in a file
    Description: 
    -Pushes a couple of values to a the Heap, this emulates heapq.heappush
    return None
    """
    with nogil:
       heap.push(value,offset)

cpdef tuple heappop(Heap heap):
    """
    Parameters: Heap an Heap object
    Description: Pops the first element of the Heap
    return a tuple(float,int)
    """
    cdef Entry e     
    with nogil:
        e = heap.pop()
    return (e.value,e.offset) 

cpdef tuple heappushpop(Heap heap,double value ,uint64_t offset):
    """
    Parameters: Heap an Heap object, double value,uint64_t a positive offset this represent the position of a string in a file
    Description: Pops the first element of the Heap and then pushes a new element
    return a tuple(float,int)
    """
    cdef Entry e 
    with nogil:
        e = heap.pop()
        heap.push(value,offset)
    return (e.value,e.offset)

cpdef object nlargest(Heap heap,Py_ssize_t k):
    """
    Parameters: Heap an Heap object,Py_ssize_t k the number of elements to return
    Description: Pops the first element of the Heap and then pushes a new element
    return a tuple(float,int)
    """
    return heap.get_n_largest(k)

cpdef tuple heappeek(Heap heap,Py_ssize_t k=0):
    """Peek at element at index k (0 = root/min)."""
    if k < 0 or k >= heap._occupied:
        raise IndexError("index out of range")

    cdef Entry e
    with nogil:
        e = heap._peek_element(k)
    return (e.value, e.offset)

def string_generator(file_name: str, offsets: list[int], encoding: str = "ascii"):
    """Yield the first space-delimited token on the line starting at each byte offset."""
    with open(file_name, "rb") as f:
        for off in offsets:
            f.seek(off)                       # go to byte offset
            line = f.readline()               # bytes to next b"\n" (or EOF)
            if not line:
                yield ""                      # offset past EOF
                continue
            # take bytes up to first space (or whole line), strip newline
            i = line.find(b" ")
            token = line[:i] if i != -1 else line.rstrip(b"\r\n")
            yield token.decode(encoding, errors="replace")

def string_getter(name: str, offset: int, encoding: str = "ascii") -> str:
    """Return the first space-delimited token on the line starting at byte offset."""
    with open(name, "rb") as f:
        f.seek(offset)
        line = f.readline()
        if not line:
            return ""
        i = line.find(b" ")
        token = line[:i] if i != -1 else line.rstrip(b"\r\n")
        return token.decode(encoding, errors="replace")
