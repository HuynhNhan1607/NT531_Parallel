import time
# --------------------- FAST-DOUBLING FIBONACCI ----------------------
def _fib_pair(n):                       # trả về (F(n), F(n+1)) mod MOD
    if n == 0:
        return (0, 1)
    a, b = _fib_pair(n // 2)
    c = (a * ((b << 1) - a)) % MOD      # F(2k)
    d = (a * a + b * b) % MOD           # F(2k+1)
    return (c, d) if n % 2 == 0 else (d, (c + d) % MOD)

def _fib_n(n):                          # F(n) mod MOD, n >= 0
    return _fib_pair(n)[0]

# --------------------- WORKER (mỗi process) -------------------------
def _worker(idx):
    # idx là chỉ số A_i đọc từ input
    return _fib_n(idx + 1)              # dịch +1 vì đề xác định F(0)=1

# --------------------- MAIN (theo format yêu cầu) -------------------
def MAIN(input_filename = "input.txt"):
    # ---- 1. Đọc dữ liệu ----
    start = time.time()
    with open(input_filename, 'r', encoding='utf-8') as f:
        first = f.readline().strip().split()
        N, Q_val = int(first[0]), int(first[1])
        A = [int(f.readline()) for _ in range(N)]

    # ---- 2. Thiết lập hằng MOD cho mọi process ----
    import multiprocessing                # không alias
    global MOD
    MOD = Q_val                           # gán để _worker & _fib_pair dùng

    # ---- 3. Quyết định chiến lược: tuần tự hay song song ----
    cpu_cnt = multiprocessing.cpu_count()
    SEQ_THRESHOLD = 50_000                # thực nghiệm: N nhỏ chạy tuần tự
    if N < SEQ_THRESHOLD or cpu_cnt == 1:
        res = [_fib_n(a + 1) for a in A]
    else:
        with multiprocessing.Pool(cpu_cnt, initializer=_init_mod, initargs=(Q_val,)) as pool:
            res = pool.map(_worker, A, chunksize=len(A)//(cpu_cnt*4)+1)

    # ---- 4. Trả về kết quả dưới dạng chuỗi ----
    end = time.time()
    print(end - start)
    return '\n'.join(str(x) for x in res)

# ------- Hàm init để truyền MOD cho mỗi process (tránh pickle lớn) --
def _init_mod(q):
    global MOD
    MOD = q

if __name__ == '__main__':
    MAIN("input.txt")          
