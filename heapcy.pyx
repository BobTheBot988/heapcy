# distutils: language = c
# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, infer_types=True, c_string_encoding=ascii
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.string cimport strlen

cdef struct RawItem:
    double value
    char * start_of_string
    Py_ssize_t size_of_string 

cdef class _Arena:
    void * _pointer
    cdef Py_ssize_t _capacity,_used

    def __cinit__(self,Py_ssize_t size):
        self._pointer = PyMem_Malloc(size)
        if(self._pointer == NULL):
            raise MemoryError("Could not allocate memoery for _pointer")

        self._capacity = size
        
    cdef void* add_element(self,void* element,Py_ssize_t size) nogil:
        pass

    cdef void clear(self):
        if self._pointer == NULL:
            return
        PyMem_Free(self._pointer)

    def __dealloc__(self):
        if self._pointer == NULL:
            return 
        PyMem_Free(self._pointer)
        

cdef class Heap:
    RawItem* lis
    Arena _arena 
    Py_ssize_t _size_of_heap, _occupied

    def __cinit__(self,Py_ssize_t size_of_heap):
        self.lis = (RawItem *)PyMem_Malloc(size_of_heap*sizeof(RawItem))

    cdef void heapify(self,Py_ssize_t i) nogil:
        cdef  Py_ssize_t smallest,Py_ssize_t left,Py_ssize_t right
        while True:
            left = (i<<1)+1
            right = left+1
            smallest = i

            if left < self._occupied and self.lis[left].value < self.lis[smallest].value:
                smallest = left

            if right < self._occupied and self.lis[right].value < self.lis[smallest].value:
                smallest = right

            if smallest == i:
                break

            self.swap(i, smallest)
            i = smallest
            
    cdef void inline swap(self,Py_ssize_t a, Py_ssize_t b)nogil:
        cdef Py_ssize_t temp = self.lis[a]
        self.lis[a] = self.lis[b] 
        self.lis[b] = temp

    cdef void push(self,double new_val, char* new_string)nogil:
        cdef RawItem item,parent
        cdef Py_ssize_t item_position, parent_position
        with gil:
            if(self._occupied+1 >= self._size_of_heap):
                raise MemoryError("The heap is full")
            if new_val > 1 or new_val < 0:
                raise ValueError("The value must be between 0< value <1")

        self._occupied+=1


        item.value = new_val
        item.size_of_string = strlen(new_string)
        item.start_of_string = <char*>self._arena.add_element(<void*>new_string,item.size_of_string)
        
        if item.start_of_string == NULL:
            with gil:
                raise MemoryError("Problem in creating assigning new string to Arena")

        item_position = self._occupied 
        self.lis[item_position] = item
        
        parent_position = (item_position-1)>>1
        parent = self.lis[parent_position]

        while item.value < parent.value and item_position > 0:
            self.swap(item_position,parent_position)
            item_position = parent_position
            parent_position = (item_position-1)>>1
        
            

    cdef RawItem pop(self) nogil:
        RawItem item = self.lis[0]
        self.lis[0] = self.lis[self._occupied]
        self.heapify(0)
        return item

    cpdef Heap build_heap(self,array:list[(float,str)|(str,float)]):
        Heap heap = Heap(len(array))
        double val1
        char * val2 

        for item in array:
            val1 = item[0]
            val2 = item[1]
            if isinstanceof(item[0],str) and isinstanceof(item[1],float):
                val1 = item[1]
                val2 = item[0]

            heap.push(val1,val2)

        heap.heapify(0)
        return heap

    cdef RawItem* get_n_largest(self,Py_ssize_t k) nogil:
        RawItem*  my_array 
        if k >= self._size_of_heap:
            with gil:
                raise ValueError("k must be smaller than the length of the heap")

        with gil:
            my_array = PyMem_Malloc(k*sizeof(RawItem))
        

    def __dealloc__(self):
        if self._lis == NULL:
            return
        PyMem_Free(self.lis)
    



cdef class HeapIter:
    # Iterator over a Heap in heap-array order (not sorted).
    # Detects concurrent modification and raises RuntimeError if mutated.
    cdef Heap* _h
    cdef Py_ssize_t _i
    cdef unsigned long _expect_mod

    def __cinit__(self, Heap *h):
        self._h = h
        self._i = 0
        self._expect_mod = h._modcount

    def __iter__(self):
        return self

    def __next__(self):
        if self._expect_mod != self._h._modcount:
            raise RuntimeError("Heap mutated during iteration")
        if self._i >= self._h._size_of_heap:
            raise StopIteration()
        cdef Py_ssize_t idx = self._i
        self._i += 1
        return self._h._py_item(idx)

cpdef void heappush(Heap* heap,item:(float,str)):
    heap.push(item)

cpdef RawItem heappop(Heap* heap):
    return heap.pop()

cpdef RawItem heappushpop(Heap* heap):
    pass

cpdef RawItem* nlargest(Heap* heap,Py_ssize_t n):
   

