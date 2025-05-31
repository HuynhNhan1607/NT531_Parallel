import multiprocessing

def _worker(args):
    matrix_a, matrix_b, row_start, row_end = args
    n = len(matrix_b)
    result_rows = []
    for i in range(row_start, row_end):
        row_result = []
        row_a = matrix_a[i]
        for j in range(n):
            s = 0
            for k in range(n):
                s += row_a[k] * matrix_b[k][j]
            row_result.append(s)
        result_rows.append(row_result)
    return (row_start, result_rows)

def MAIN(matrix_a, matrix_b):
    n = len(matrix_a)
    cpu_count = min(4, multiprocessing.cpu_count())  # max 4 core

    # Chia đều số dòng cho số process
    chunk_size = (n + cpu_count - 1) // cpu_count
    args_list = []
    for i in range(0, n, chunk_size):
        args_list.append((matrix_a, matrix_b, i, min(i + chunk_size, n)))

    with multiprocessing.Pool(processes=cpu_count) as pool:
        results = pool.map(_worker, args_list)

    # Ghép kết quả theo đúng thứ tự
    matrix_c = [None] * n
    for row_start, rows in results:
        for idx, row in enumerate(rows):
            matrix_c[row_start + idx] = row

    return matrix_c
