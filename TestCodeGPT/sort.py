# --------- Challenge 3 – Parallel Merge-Sort toàn phần ------------
import math, os
import concurrent.futures 

def merge_sort(arr):
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left  = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge_two(left, right)

def merge_two(left, right):
    merged, i, j = [], 0, 0
    nL, nR = len(left), len(right)
    while i < nL and j < nR:
        if left[i] <= right[j]:
            merged.append(left[i]); i += 1
        else:
            merged.append(right[j]); j += 1
    merged.extend(left[i:]); merged.extend(right[j:])
    return merged

# Sắp xếp từng block bằng merge_sort thủ công
def _sort_block(block):
    return merge_sort(block)

# Merge một cặp đã sắp xếp
def _merge_pair(pair):
    return merge_two(*pair)

# ----- MAIN với merge song song -----
def MAIN(arrayA):
    n = len(arrayA)
    if n <= 16384:
        return merge_sort(arrayA)

    cpu = os.cpu_count() or 4
    chunk = math.ceil(n / cpu)
    blocks = [arrayA[i:i + chunk] for i in range(0, n, chunk)]

    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu) as pool:
        # Giai đoạn 1: sort từng block
        sorted_blocks = list(pool.map(_sort_block, blocks))

        # Giai đoạn 2: merge song song từng tầng
        while len(sorted_blocks) > 1:
            # Gom cặp để merge
            pairs = []
            it = iter(sorted_blocks)
            for left in it:
                right = next(it, [])   # Khối lẻ: merge với []
                pairs.append((left, right))

            # Song song merge các cặp
            sorted_blocks = list(pool.map(_merge_pair, pairs))

    return sorted_blocks[0]



# ------------------ Unit-test/Benchmark nhanh -------------------
if __name__ == "__main__":
    import random, time
    SIZE = 1_000_000
    data = [random.randint(-10**9, 10**9) for _ in range(SIZE)]

    t0 = time.perf_counter()
    result = MAIN(data)
    t1 = time.perf_counter()

    assert result == sorted(data)
    print(f"✓ Sorted {SIZE:,} items in {t1 - t0:.2f} s "
          f"using {os.cpu_count() or 4} CPU cores")
