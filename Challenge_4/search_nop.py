import concurrent.futures
import os

def _find_in_chunk(args):
    chunk, base_index, key = args
    for i, val in enumerate(chunk):
        if val == key:
            return base_index + i
    return -1

def MAIN(input_file="data.txt", key=20):
    # Đọc toàn bộ số nguyên từ file
    with open(input_file, 'r') as f:
        raw = f.read()
    array = list(map(int, raw.split()))

    n = len(array)
    if n == 0:
        return -1

    # Tận dụng tất cả CPU cores có sẵn
    max_workers = os.cpu_count()
    
    # Chia mảng thành các khối để xử lý song song
    chunk_size = (n + max_workers - 1) // max_workers
    args_list = [
        (array[i:i+chunk_size], i, key)
        for i in range(0, n, chunk_size)
    ]

    # Sử dụng as_completed để dừng tìm kiếm ngay khi tìm thấy kết quả
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit tất cả các công việc
        futures = [executor.submit(_find_in_chunk, args) for args in args_list]
        
        # Xử lý kết quả ngay khi có bất kỳ tiến trình nào hoàn thành
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result != -1:
                # Hủy các công việc khác nếu đã tìm thấy kết quả
                for f in futures:
                    if not f.done():
                        f.cancel()
                return result
    
    return -1
import time
if __name__ == "__main__":
    start = time.time()
    result = MAIN("./demo_data.txt", 999)
    end = time.time()
    print(f"Kết quả: {result}, thời gian: {end - start:.4f}s")