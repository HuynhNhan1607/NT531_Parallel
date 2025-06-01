import concurrent.futures
import os
import time
import random

def _jump_search_in_chunk(chunk, base_index, key, jump=10):
    """Tìm kiếm kiểu nhảy trước, sau đó tìm chi tiết trong khoảng nhỏ"""
    n = len(chunk)
    
    # Kiểm tra nhanh phần tử đầu và cuối
    if n > 0 and chunk[0] == key:
        return base_index
    if n > 0 and chunk[-1] == key:
        return base_index + n - 1
    
    # Tìm kiểm bằng nhảy - kiểm tra mỗi jump phần tử
    positions = list(range(0, n, jump))
    for i in positions:
        if chunk[i] == key:
            return base_index + i
    
    # Nếu không tìm thấy bằng nhảy, tìm chi tiết hai đầu giữa các điểm nhảy
    for j in range(len(positions) - 1):
        start = positions[j] + 1
        end = positions[j+1] - 1
        
        left, right = start, end
        while left <= right:
            if chunk[left] == key:
                return base_index + left
            if chunk[right] == key:
                return base_index + right
            left += 1
            right -= 1
    
    return -1

def _find_in_chunk(args):
    chunk, base_index, key = args
    n = len(chunk)
    
    # Với chunk nhỏ, tìm kiếm hai đầu vào giữa
    if n < 1000:
        left, right = 0, n - 1
        while left <= right:
            if chunk[left] == key:
                return base_index + left
            if chunk[right] == key:
                return base_index + right
            left += 1
            right -= 1
        return -1
    
    # Với chunk lớn, sử dụng jump search + tìm kiếm hai đầu
    else:
        jump_size = max(10, n // 500)  # Tự động điều chỉnh kích thước nhảy
        return _jump_search_in_chunk(chunk, base_index, key, jump_size)

def MAIN(input_file="data.txt", key=20):
    start_time = time.time()
    
    # Đọc toàn bộ số nguyên từ file
    with open(input_file, 'r') as f:
        raw = f.read()
    array = list(map(int, raw.split()))

    n = len(array)
    if n == 0:
        return -1
    
    # Tìm kiếm nhanh một mẫu ngẫu nhiên
    sample_size = min(1000, n//10)
    if sample_size > 0:
        sampled_indices = random.sample(range(n), sample_size)
        for i in sampled_indices:
            if array[i] == key:
                return i
    
    # Kiểm tra một phần đầu và cuối (trường hợp phổ biến)
    head_size = min(500, n)
    for i in range(head_size):
        if array[i] == key:
            return i
    
    tail_size = min(500, n)
    for i in range(n-tail_size, n):
        if array[i] == key:
            return i
    
    # Chia mảng thành các khối để xử lý song song
    cpu_cnt = os.cpu_count() or 4
    
    # Tạo nhiều chunks hơn số CPU để cân bằng tải
    chunk_count = cpu_cnt * 4
    chunk_size = max(1000, (n + chunk_count - 1) // chunk_count)
    
    # Chuẩn bị chunks với kích thước thay đổi
    args_list = []
    for i in range(0, n, chunk_size):
        end = min(i + chunk_size, n)
        args_list.append((array[i:end], i, key))
    
    # Thực hiện tìm kiếm song song với kết thúc sớm
    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu_cnt) as executor:
        # Submit tất cả công việc
        futures = [executor.submit(_find_in_chunk, args) for args in args_list]
        
        # Xử lý kết quả ngay khi có
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result != -1:
                # Hủy công việc còn lại
                for f in futures:
                    if not f.done():
                        f.cancel()
                return result
    
    return -1

if __name__ == "__main__":
    start = time.time()
    result = MAIN("data.txt", 999)
    end = time.time()
    print(f"Kết quả: {result}, thời gian: {end - start:.4f}s")