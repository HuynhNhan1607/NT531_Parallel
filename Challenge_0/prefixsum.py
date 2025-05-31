import multiprocessing
import concurrent.futures
import math

# ---------- tuần tự: reduce-then-scan đệ quy ----------
def _serial_scan(arr):
    """Trả về (prefix_list, tổng) – chạy hoàn toàn trong 1 process."""
    n = len(arr)
    if n == 1:
        return [arr[0]], arr[0]

    mid = n // 2
    left_scan, left_sum = _serial_scan(arr[:mid])
    right_scan, right_sum = _serial_scan(arr[mid:])

    # cộng offset cho nửa phải
    right_scan = [x + left_sum for x in right_scan]
    return left_scan + right_scan, left_sum + right_sum


# ---------- hàm đệ quy có song song ở tầng trên ----------
def _parallel_scan(arr, depth, pool):
    """Song song hóa đến 'depth' lớp, sau đó tuần tự."""
    n = len(arr)
    if n == 1:
        return [arr[0]], arr[0]

    mid = n // 2
    if depth > 0:
        # gửi hai nửa tới pool; mỗi nửa tự xử lý tuần tự
        f_left  = pool.submit(_serial_scan, arr[:mid])
        f_right = pool.submit(_serial_scan, arr[mid:])
        left_scan, left_sum   = f_left.result()
        right_scan, right_sum = f_right.result()
    else:
        left_scan,  left_sum  = _serial_scan(arr[:mid])
        right_scan, right_sum = _serial_scan(arr[mid:])

    right_scan = [x + left_sum for x in right_scan]
    return left_scan + right_scan, left_sum + right_sum


# ---------- giao diện chấm điểm ----------
def MAIN(arrayA):
    """
    Trả về mảng prefix-sum (inclusive).  
    Giữ đúng lý thuyết đệ quy 2-nhánh, nhưng chỉ tạo pool một lần
    để tránh trễ thời gian khởi tạo process.
    """
    n = len(arrayA)
    if n == 0:
        return []

    # quyết định song song bao nhiêu tầng (thường 1 hoặc 2 là đủ)
    cpu_cnt = multiprocessing.cpu_count()
    # depth = floor(log2(cpu_cnt)) nhưng không cần quá 2 tầng
    depth = min(2, max(0, int(math.log2(cpu_cnt))))

    # mảng nhỏ chạy tuần tự sẽ nhanh hơn
    if n < 50_000 or depth == 0:
        return _serial_scan(arrayA)[0]

    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_cnt) as pool:
        prefix, _ = _parallel_scan(arrayA, depth - 1, pool)
        return prefix

if __name__ == '__main__':
    result = MAIN(list(range(1, 1000001)))
    print(result)

