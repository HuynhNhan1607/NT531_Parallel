import concurrent.futures

def _fib_pair(k, mod):                 # trả về (F(k), F(k+1))  theo chuẩn
    if k == 0:
        return 0, 1
    a, b = _fib_pair(k >> 1, mod)      # chia đôi chỉ số
    c = (a * ((b << 1) - a) % mod) % mod
    d = (a * a + b * b) % mod
    return (d, (c + d) % mod) if k & 1 else (c, d)


def _fib_mod(task):                    # worker cho ProcessPoolExecutor
    n, mod = task                      # n là Aᵢ, mod = Q
    return _fib_pair(n + 1, mod)[0]    # vì đề cho F(0)=F(1)=1  ⇒ F(n)=F_std(n+1)


def MAIN(input_filename="input.txt"):
    with open(input_filename) as f:
        N, Q = map(int, f.readline().split())
        jobs = [(int(f.readline()), Q) for _ in range(N)]

    # concurrent.futures đã được hệ thống import sẵn
    with concurrent.futures.ProcessPoolExecutor() as pool:
        results = list(pool.map(_fib_mod, jobs, chunksize=1_024))

    return results