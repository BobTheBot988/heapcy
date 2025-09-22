import tempfile
import os
import shutil
import gzip
import heapcy


def function():
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        with gzip.open("total_guesses.gz") as fopen:
            shutil.copyfileobj(fopen, tmpfile)
            temp_file_name = tmpfile.name

    with open(temp_file_name, "rb") as f_open:
        my_heap = heapcy.Heap(100_000)

        while True:
            offset = f_open.tell()
            line = f_open.readline()

            if not line:
                break

            parts = line.rstrip(b"\b\n").split(b" ", 1)
            if len(parts) != 2:
                continue
            prob = float(parts[1].decode(encoding="ascii"))
            heapcy.heappush(my_heap, prob, offset)

    lis: list[int] = list()
    for tup in heapcy.nlargest(my_heap, 1000):
        lis.append(tup[1])
    a = list(heapcy.string_generator(temp_file_name, lis))
    __import__("pprint").pprint(a)
    os.remove(temp_file_name)


if __name__ == "__main__":
    function()
