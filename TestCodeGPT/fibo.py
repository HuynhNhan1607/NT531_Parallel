# ---------- Challenge 1 – Fast-doubling Fibonacci (parallel, dynamic schedule) ----------
import concurrent.futures, math, os

# Fast-doubling: trả (F(k), F(k+1)) (mod m)
def _fib_pair(k: int, m: int):
    if k == 0:
        return 0, 1
    a, b = _fib_pair(k >> 1, m)
    c = (a * ((b << 1) - a) % m) % m
    d = (a * a + b * b) % m
    return (d, (c + d) % m) if k & 1 else (c, d)

# -------- Worker: xử lý 1 lô truy vấn ---------
def _fib_batch(args):
    start_idx, sub_queries, mod = args
    out = [0] * len(sub_queries)
    for i, k in enumerate(sub_queries):
        out[i] = _fib_pair(k + 1, mod)[0]   # F(0)=F(1)=1  → F(n)=F_std(n+1)
    return start_idx, out

# -------- API chính ---------
def MAIN(input_file="input.txt", batch_size: int = 2048):
    # Đọc file
    with open(input_file) as f:
        N, Q = map(int, f.readline().split())
        queries = [int(f.readline()) for _ in range(N)]

    cpu = os.cpu_count() or 4
    results = [0] * N                      # mảng kết quả cuối cùng

    # Tạo danh sách batch (start, sublist, mod)
    batches = []
    for i in range(0, N, batch_size):
        batches.append((i, queries[i:i + batch_size], Q))

    # Lịch trình động – submit từng batch, thu kết quả as_completed
    with concurrent.futures.ProcessPoolExecutor(max_workers=cpu) as pool:
        future_map = {pool.submit(_fib_batch, b): b[0] for b in batches}
        for fut in concurrent.futures.as_completed(future_map):
            start_idx, vals = fut.result()
            results[start_idx : start_idx + len(vals)] = vals

    return results


# ---------------- Demo nhanh -----------------
if __name__ == "__main__":
    # Tạo file demo nhỏ
    demo = "demo.txt"
    with open(demo, "w") as f:
        f.write("8 1000000000\n")          # N=8, Q=1e9
        f.write("\n".join(map(str, [3, 4, 5, 30, 31, 32, 1_000_000, 12345678])))

    out = MAIN(demo)
    print(out)
