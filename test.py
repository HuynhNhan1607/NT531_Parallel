import numpy as np
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import time

# Constants
ROWS = 1000
COLS = 1000

def generate_random_data():
    """Generate a 2D array with 1000 sub-arrays, each containing 1000 random integers"""
    return np.random.randint(1, 1000, size=(ROWS, COLS)).tolist()

# Move nested function to module level
def calculate_prefix_segment(args):
    """Calculate a segment of prefix sum"""
    start, end, lengths = args
    local_sum = 0
    results = [0] * (end - start)
    for i in range(start, end):
        local_sum += lengths[i]
        results[i - start] = local_sum
    return local_sum, start, results

def parallel_prefix_sum(lengths, num_processes):
    """Calculate prefix sum in parallel using divide-and-conquer approach"""
    n = len(lengths)
    prefix_sums = [0] * n
    
    # Divide the work according to available CPU cores
    cpu_count = min(num_processes, mp.cpu_count())
    chunk_size = (n + cpu_count - 1) // cpu_count
    
    # Stage 1: Calculate local sums in parallel
    segments = []
    for i in range(0, n, chunk_size):
        segments.append((i, min(i + chunk_size, n), lengths))
    
    local_results = []
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        local_results = list(executor.map(calculate_prefix_segment, segments))
    
    # Stage 2: Adjust prefix sums to get the global result
    offset = 0
    for local_sum, start, results in local_results:
        for i, val in enumerate(results):
            prefix_sums[start + i] = offset + val
        offset += local_sum
    
    # Shift values to get correct starting positions
    result = [0]
    result.extend(prefix_sums[:-1])
    return result

# Replace the copy_segment function and parallel_flatten function with these versions

def copy_segment(args):
    start_idx, end_idx, data, start_positions = args
    result_segment = []
    segment_positions = []
    
    # Collect data and positions
    for i in range(start_idx, end_idx):
        row = data[i]
        pos = start_positions[i]
        for j in range(len(row)):
            result_segment.append(row[j])
            segment_positions.append(pos + j)
    
    return result_segment, segment_positions

def parallel_flatten(data, num_processes):
    """Flatten the 2D array in parallel"""
    # Get lengths of each sub-array
    lengths = [len(row) for row in data]
    
    # Calculate prefix sums to determine offsets
    start_positions = parallel_prefix_sum(lengths, num_processes)
    
    # Create the result array
    total_length = sum(lengths)
    flattened = [0] * total_length
    
    # Parallelize the copy operation
    cpu_count = min(num_processes, mp.cpu_count())
    chunk_size = (len(data) + cpu_count - 1) // cpu_count
    
    segments = []
    for i in range(0, len(data), chunk_size):
        end = min(i + chunk_size, len(data))
        segments.append((i, end, data, start_positions))
    
    # Process data in parallel and collect results
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        results = list(executor.map(copy_segment, segments))
    
    # Combine results from all processes
    for segment_data, segment_positions in results:
        for idx, pos in enumerate(segment_positions):
            flattened[pos] = segment_data[idx]
    
    return flattened

def main():
    # Set the maximum number of processes based on CPU cores
    num_processes = mp.cpu_count()
    print(f"Running with maximum {num_processes} processes")
    
    # Generate random 2D array
    print("Generating random 2D array...")
    data = generate_random_data()
    
    # Flatten the array in parallel
    print("Flattening array in parallel...")
    start_time = time.time()
    flattened = parallel_flatten(data, num_processes)
    end_time = time.time()
    
    # Verify results
    print(f"Flattening completed in {end_time - start_time:.4f} seconds")
    print(f"Output: [{flattened[0]}, {flattened[-1]}, {len(flattened)}]")
    
    # Verify the length
    expected_length = ROWS * COLS
    assert len(flattened) == expected_length, f"Expected length {expected_length}, got {len(flattened)}"
    print("Length verification successful!")
    
    # Verify correctness by comparing with sequential flatten
    sequential_flatten = []
    for row in data:
        sequential_flatten.extend(row)
    
    is_correct = (len(sequential_flatten) == len(flattened)) and all(a == b for a, b in zip(sequential_flatten, flattened))
    print(f"Correctness check: {'Passed' if is_correct else 'Failed'}")
    if not is_correct:
        # Print some diagnostics if the check fails
        mismatches = [(i, sequential_flatten[i], flattened[i]) 
                     for i in range(min(10, len(sequential_flatten))) 
                     if i < len(flattened) and sequential_flatten[i] != flattened[i]]
        print(f"First few mismatches (index, expected, actual): {mismatches}")

if __name__ == "__main__":
    main()