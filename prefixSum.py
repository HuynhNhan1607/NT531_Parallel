import multiprocessing as mp
import numpy as np
import math

def parallel_prefix_sum(lengths, result_array, start, end, chunk_totals, process_id, P):
    """Tính prefix sum song song cho một phần của mảng lengths"""
    result_np = np.frombuffer(result_array.get_obj(), dtype=np.int32)
    chunk_size = end - start
    # Tính prefix sum cục bộ
    local_sum = 0
    for i in range(start, end):
        result_np[i] = local_sum
        local_sum += lengths[i]
    chunk_totals[process_id] = local_sum

def combine_prefix_sums(result_array, chunk_totals, P):
    """Kết hợp prefix sum từ các chunk"""
    result_np = np.frombuffer(result_array.get_obj(), dtype=np.int32)
    # Tính prefix sum của các tổng chunk
    total = 0
    for i in range(P):
        offset = total
        total += chunk_totals[i]
        # Cộng offset vào prefix sum của chunk tiếp theo
        start = i * (len(result_np) // P)
        end = min((i + 1) * (len(result_np) // P), len(result_np))
        if i > 0:
            result_np[start:end] += offset

def worker(args):
    """Xử lý chép dữ liệu từ mảng con vào mảng kết quả"""
    indices, data, result, offsets = args
    result_np = np.frombuffer(result.get_obj(), dtype=np.int32)
    offsets_np = np.frombuffer(offsets.get_obj(), dtype=np.int32)
    for i in indices:
        offset = offsets_np[i]
        result_np[offset:offset + 1000] = data[i]

def parallel_flatten(data):
    """Làm phẳng mảng 2 chiều song song"""
    # Số tiến trình tối đa bằng số CPU vật lý
    P = mp.cpu_count()
    num_subarrays = len(data)

    # Bước 1: Tính prefix sum song song
    lengths = [1000] * num_subarrays  # Độ dài mỗi mảng con là 1000
    offsets = mp.Array('i', num_subarrays)  # Mảng chứa offset
    chunk_totals = mp.Array('i', P)  # Tổng của mỗi chunk

    # Chia công việc tính prefix sum
    chunk_size = math.ceil(num_subarrays / P)
    processes = []
    for i in range(P):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, num_subarrays)
        p = mp.Process(target=parallel_prefix_sum, args=(lengths, offsets, start, end, chunk_totals, i, P))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()

    # Kết hợp kết quả prefix sum
    combine_prefix_sums(offsets, chunk_totals, P)

    # Bước 2: Chép dữ liệu song song
    result = mp.Array('i', 1000 * 1000)  # Mảng kết quả
    list_of_indices = [list(range(i * chunk_size, min((i + 1) * chunk_size, num_subarrays))) for i in range(P)]
    
    with mp.Pool(P) as pool:
        pool.map(worker, [(indices, data, result, offsets) for indices in list_of_indices])

    # Lấy kết quả cuối cùng
    result_np = np.frombuffer(result.get_obj(), dtype=np.int32)
    return [result_np[0], result_np[-1], len(result_np)]

# Ví dụ sử dụng
if __name__ == "__main__":
    # Tạo dữ liệu giả định
    data = [[i * 1000 + j for j in range(1000)] for i in range(1000)]
    result = parallel_flatten(data)
    print(f"Kết quả: {result}")  # Dự kiến: [0, 999999, 1000000]