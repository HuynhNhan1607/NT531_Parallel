import multiprocessing as mp
import math
import time
import ctypes

# ----------------------- Đệ quy reduce → scan -----------------------
def _recursive_scan(arr, start, end):
    """
    Trả về (prefix_list, total_sum) của arr[start:end].
    Thực hiện hoàn toàn trong 1 process bằng đệ quy nhị phân.
    """
    n = end - start
    if n == 1:
        return [arr[start]], arr[start]

    mid = start + n // 2
    left_prefix, left_sum = _recursive_scan(arr, start, mid)
    right_prefix, right_sum = _recursive_scan(arr, mid, end)

    # cộng offset cho nửa phải
    right_prefix = [x + left_sum for x in right_prefix]

    return left_prefix + right_prefix, left_sum + right_sum


def _worker(shared_input, shared_output, start_idx, end_idx, result_queue):
    """Hàm chạy trong process con: tính prefix-sum đệ quy của 1 chunk."""
    # Tính prefix sum của chunk
    prefix_list, total_sum = _recursive_scan(shared_input, start_idx, end_idx)
    
    # Ghi kết quả vào vùng nhớ dùng chung
    for i in range(len(prefix_list)):
        shared_output[start_idx + i] = prefix_list[i]
    
    # Truyền tổng của chunk qua queue
    result_queue.put((start_idx, total_sum))


# ----------------------- Hàm giao diện -----------------------
def MAIN_SHARED(arrayA):
    """
    Prefix-sum (inclusive) song song sử dụng shared memory:
    * Tạo shared memory array cho input và output.
    * Chia mảng thành k phần gần bằng nhau, mỗi process tính prefix-sum đệ quy.
    * Ghép kết quả và cộng dồn offset giữa các chunk.
    """
    n = len(arrayA)
    if n == 0:
        return []

    # Thiết lập phương thức khởi tạo process an toàn cho mọi OS
    try:
        mp.set_start_method('spawn')
    except RuntimeError:
        # Trên Windows hoặc nếu đã thiết lập trước đó
        pass

    # Tạo shared memory cho input và output
    shared_input = mp.RawArray(ctypes.c_double, arrayA)  # c_double cho kiểu float
    shared_output = mp.RawArray(ctypes.c_double, n)      # mảng output dùng chung
    
    # Queue để nhận kết quả tổng của mỗi chunk
    result_queue = mp.Queue()

    # Thiết lập tham số
    cpu_cnt = mp.cpu_count()
    cpu_cnt = min(cpu_cnt, n)            # không tạo nhiều process hơn phần tử
    chunk_size = (n + cpu_cnt - 1) // cpu_cnt

    # Tạo và khởi động các process
    processes = []
    for i in range(cpu_cnt):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, n)
        
        p = mp.Process(
            target=_worker,
            args=(shared_input, shared_output, start_idx, end_idx, result_queue)
        )
        processes.append(p)
        p.start()

    # Đợi tất cả process hoàn thành
    for p in processes:
        p.join()
    
    # Thu thập kết quả và áp dụng offset
    chunk_results = []
    while not result_queue.empty():
        chunk_results.append(result_queue.get())
    
    # Sắp xếp theo chỉ số bắt đầu để đảm bảo đúng thứ tự
    chunk_results.sort()
    
    # Áp dụng offset giữa các chunk
    offset = 0
    for start_idx, sum_chunk in chunk_results:
        if start_idx > 0:  # Bỏ qua chunk đầu tiên
            end_idx = min(start_idx + chunk_size, n)
            for i in range(start_idx, end_idx):
                shared_output[i] += offset
        offset += sum_chunk

    # Chuyển kết quả từ shared memory sang list thông thường
    output = list(shared_output)
    return output


if __name__ == '__main__':
    start_time = time.time()
    result = MAIN_SHARED(list(range(1, 1000001)))
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.4f} seconds")
    print(result[-5])