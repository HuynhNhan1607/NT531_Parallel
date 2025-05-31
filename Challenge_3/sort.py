##############  Challenge 3 – Parallel Sort  ##############

import math
import concurrent.futures              # đã được phép dùng
import os                               # chỉ để lấy số CPU (được phép)

import math
import concurrent.futures
import os

def merge_sort(arr):
    """Thuật toán Merge Sort thuần túy (đệ quy)."""
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left  = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge_two(left, right)

def merge_two(left, right):
    """Trộn hai mảng đã sắp xếp."""
    merged = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            merged.append(left[i]); i += 1
        else:
            merged.append(right[j]); j += 1
    merged.extend(left[i:])
    merged.extend(right[j:])
    return merged

def _parallel_sort(chunk):
    """Hàm chạy trong tiến trình con: sắp xếp bằng merge sort."""
    return merge_sort(chunk)

def MAIN(arrayA):
    n = len(arrayA)
    if n <= 16384:
        return merge_sort(arrayA)

    cpu_cnt = os.cpu_count() or 4
    chunk_size = math.ceil(n / cpu_cnt)
    chunks = [arrayA[i:i+chunk_size] for i in range(0, n, chunk_size)]

    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_cnt) as pool:
        sorted_chunks = list(pool.map(_parallel_sort, chunks))

    while len(sorted_chunks) > 1:
        next_round = []
        for i in range(0, len(sorted_chunks), 2):
            if i + 1 < len(sorted_chunks):
                merged = merge_two(sorted_chunks[i], sorted_chunks[i + 1])
                next_round.append(merged)
            else:
                next_round.append(sorted_chunks[i])
        sorted_chunks = next_round

    return sorted_chunks[0]


#####################  Unit test nhanh  ####################
if __name__ == "__main__":
    import random, time
    arr = [random.randint(-10**9, 10**9) for _ in range(1_000_000)]
    t0 = time.perf_counter()
    out = MAIN(arr)
    t1 = time.perf_counter()
    assert out == sorted(arr), "Kết quả sai!"
    print(f"✔ Passed – thời gian {t1 - t0:.2f}s với {len(arr):,} phần tử.")
