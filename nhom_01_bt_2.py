import multiprocessing
import random
import time
import numpy as np
from math import log2, floor
from concurrent.futures import ProcessPoolExecutor

"""

1. Xác định độ dài của mỗi mảng con trong mảng 2 chiều.

2. Tính prefix sum song song sử dụng thuật toán chia để trị đệ quy.
    - Nhóm em giữ lại cách tính toán Prefix sum ở bài tập 1.
    - Số tiến trình thực hiện cho việc đệ quy rồi mới tạo tiến trình 
    - Độ sâu của đệ quy song song phụ thuộc log(max_cpu_count). Sau đó sẽ là đệ quy nhưng không tạo thêm tiến trình nữa.

3. Sao chép dữ liệu từ mảng 2 chiều vào mảng kết quả dựa trên vị trí được tính từ prefix sum.

"""


def dictMerge(dictA, dictB):
    return {**dictA, **dictB}

def reduce_tree(A, L, R, depth):
    if L == R:
        return A[L], {(L, R): A[L]}
    mid = (L + R) // 2
    if depth > 0:
        with ProcessPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(reduce_tree, A, L, mid, depth - 1)
            f2 = executor.submit(reduce_tree, A, mid + 1, R, depth - 1)
            leftSum, leftDict = f1.result()
            rightSum, rightDict = f2.result()
    else:
        leftSum, leftDict = reduce_tree(A, L, mid, depth - 1)
        rightSum, rightDict = reduce_tree(A, mid + 1, R, depth - 1)
    total = leftSum + rightSum
    merged = dictMerge(leftDict, rightDict)
    merged[L, R] = total
    return total, merged

def scan_r(A, B, L, R, offset, preSumDict, depth):
    if L == R:
        B[L] = A[L] + offset
        return
    mid = (L + R) // 2
    leftSum = preSumDict[(L, mid)]
    if depth > 0:
        with ProcessPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(scan_r, A, B, L, mid, offset, preSumDict, depth - 1)
            f2 = executor.submit(scan_r, A, B, mid + 1, R, offset + leftSum, preSumDict, depth - 1)
            f1.result()
            f2.result()
    else:
        scan_r(A, B, L, mid, offset, preSumDict, depth - 1)
        scan_r(A, B, mid + 1, R, offset + leftSum, preSumDict, depth - 1)

def scan_exclusive_parallel(arr):
    max_depth = floor(log2(multiprocessing.cpu_count()))
    _, preSumDict = reduce_tree(arr, 0, len(arr) - 1, max_depth)
    with multiprocessing.Manager() as manager:
        result = manager.list([0] * len(arr))
        scan_r(arr, result, 0, len(arr) - 1, 0, preSumDict, max_depth)
        return [0] + list(result[:-1])  

def copy_chunk(args):
    matrix, offset, S, i = args
    off = offset[i]
    result = []
    for j in range(S[i]):
        result.append((off + j, matrix[i][j]))
    return result

def flatten(input_matrix):
    n = len(input_matrix)

    # Step 1: Lấy kích thước của từng mảng con
    S = [len(row) for row in input_matrix]  

    # Step 2: Tính song song prefix sum
    offset = scan_exclusive_parallel(S)

    # Step 3: Sao chép dữ liệu vào B dựa trên offset lấy từ Prefix sum
    total_size = sum(S)
    B = [0] * total_size

    args = [(input_matrix, offset, S, i) for i in range(n)]
    with multiprocessing.Pool() as pool:
        results = pool.map(copy_chunk, args)

    for sublist in results:
        for idx, val in sublist:
            B[idx] = val

    return B

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)
    random.seed(42)
    matrix = np.random.randint(1, 1000, size=(1000, 1000)).tolist()
    start_total = time.time()
    B = flatten(matrix)

    X = B[0]
    Y = B[-1]
    Z = len(B)

    print([X, Y, Z])
 