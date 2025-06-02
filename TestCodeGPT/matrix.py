import multiprocessing

def _dot_product(row, col):
    return sum(a * b for a, b in zip(row, col))


def _get_column(matrix, j):
    return [row[j] for row in matrix]


def _matmul_block(A, B, bi, bj, n, block_size):
    """Nhân từng khối A[bi,:] x B[:,bj]"""
    C_block = [[0 for _ in range(block_size)] for _ in range(block_size)]
    for i in range(block_size):
        for j in range(block_size):
            row_idx = bi + i
            col_idx = bj + j
            row = A[row_idx]
            col = _get_column(B, col_idx)
            C_block[i][j] = _dot_product(row, col)
    return (bi, bj, C_block)


def MAIN(A, B):
    """Nhân 2 ma trận vuông không dùng numpy, chia 2x2 khối, chạy trên 4 core"""
    n = len(A)
    assert all(len(row) == n for row in A)
    assert all(len(row) == n for row in B)
    assert n % 2 == 0
    block_size = n // 2

    args = [(A, B, bi, bj, n, block_size)
            for bi in range(0, n, block_size)
            for bj in range(0, n, block_size)]

    with multiprocessing.Pool(processes=4) as pool:
        results = pool.starmap(_matmul_block, args)

    # Ghép lại ma trận kết quả
    C = [[0 for _ in range(n)] for _ in range(n)]
    for bi, bj, block in results:
        for i in range(block_size):
            for j in range(block_size):
                C[bi + i][bj + j] = block[i][j]

    return C



# -------------------- ĐỌAN TEST NHANH --------------------
if __name__ == "__main__":
    import random, time
    N = 1000
    A = [[random.randint(0, 9) for _ in range(N)] for _ in range(N)]
    B = [[random.randint(0, 9) for _ in range(N)] for _ in range(N)]

    t0 = time.perf_counter()
    C = MAIN(A, B)
    t1 = time.perf_counter()
    print(f"✓ Done {N}×{N} in {t1 - t0:.2f}s")
