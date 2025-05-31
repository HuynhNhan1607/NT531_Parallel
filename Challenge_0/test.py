import multiprocessing
import math

# ----------------------- Đệ quy reduce → scan -----------------------
def _recursive_scan(arr):
    """
    Trả về (prefix_list, total_sum) của arr.
    Thực hiện hoàn toàn trong 1 process bằng đệ quy nhị phân.
    """
    n = len(arr)
    if n == 1:
        return [arr[0]], arr[0]

    mid = n // 2
    left_prefix,  left_sum  = _recursive_scan(arr[:mid])
    right_prefix, right_sum = _recursive_scan(arr[mid:])

    # cộng offset cho nửa phải
    right_prefix = [x + left_sum for x in right_prefix]

    return left_prefix + right_prefix, left_sum + right_sum


def _worker(chunk):
    """Hàm chạy trong process con: tính prefix-sum đệ quy của 1 chunk."""
    return _recursive_scan(chunk)        # tuple (prefix_list, total_sum)


# ----------------------- Hàm giao diện -----------------------
def MAIN(arrayA):
    """
    Prefix-sum (inclusive) song song:
    * Tạo 1 Pool với k = số CPU.
    * Chia mảng thành k phần gần bằng nhau, mỗi process tính prefix-sum đệ quy.
    * Ghép kết quả và cộng dồn offset giữa các chunk.
    """
    n = len(arrayA)
    if n == 0:
        return []

    # Thiết lập phương thức khởi tạo process an toàn cho mọi OS
    try:
        multiprocessing.set_start_method('spawn')
    except RuntimeError:
        # Trên Windows hoặc nếu đã thiết lập trước đó
        pass

    cpu_cnt = multiprocessing.cpu_count()
    cpu_cnt = min(cpu_cnt, n)            # không tạo nhiều process hơn phần tử

    # Chia mảng thành các chunk gần bằng nhau
    chunk_size = (n + cpu_cnt - 1) // cpu_cnt
    chunks = [arrayA[i:i + chunk_size] for i in range(0, n, chunk_size)]

    # Song song xử lý từng chunk
    with multiprocessing.Pool(cpu_cnt) as pool:
        results = pool.map(_worker, chunks)   # [(prefix_chunk, sum_chunk), ...]

    # Ghép kết quả, cộng offset
    output = [0] * n
    offset = 0
    idx = 0
    for prefix_chunk, sum_chunk in results:
        m = len(prefix_chunk)
        output[idx:idx + m] = [val + offset for val in prefix_chunk]
        idx += m
        offset += sum_chunk

    return output

import time
if __name__ == '__main__':
    start_time = time.time()
    result = MAIN(list(range(1, 1000001)))
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.4f} seconds")
    print(result[-5])
