import concurrent.futures
import os

def _find_in_chunk(args):
    array, start, end, key = args
    for i in range(start, end):
        if array[i] == key:
            return i
    return -1

def MAIN(input_file="data.txt", key=20):
    # Đọc toàn bộ số nguyên từ file
    with open(input_file, 'r') as f:
        array = list(map(int, f.read().split()))

    n = len(array)
    if n == 0:
        return -1

    cpu_count = os.cpu_count()
    chunk_size = (n + cpu_count - 1) // cpu_count

    args_list = [
        (array, i, min(i + chunk_size, n), key)
        for i in range(0, n, chunk_size)
    ]

    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count) as executor:
        futures = [executor.submit(_find_in_chunk, args) for args in args_list]

        # Dừng sớm khi có kết quả đầu tiên
        done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
        for future in done:
            result = future.result()
            if result != -1:
                # Hủy các tiến trình còn lại
                for f in not_done:
                    f.cancel()
                return result

    return -1

# ----------------- Test nhanh -----------------
if __name__ == "__main__":
    import random, time
    N = 2_000_000
    KEY = 999
    # sinh file mẫu
    filename = "demo_data.txt"
    with open(filename, "w") as f:
        nums = [random.randint(0, 10**6) for _ in range(N)]
        idx_place = random.randint(0, N - 1)
        nums[idx_place] = KEY
        f.write(" ".join(map(str, nums)))

    t0 = time.perf_counter()
    idx = MAIN(filename, KEY)
    t1 = time.perf_counter()
    print(f"✓ Found at index {idx}  •  time: {t1 - t0:.2f}s")
