if __name__ == "__main__":
    import random, time
    SIZE = 1_000_000
    data = [random.randint(-10**9, 10**9) for _ in range(SIZE)]

    t0 = time.perf_counter()
    result = MAIN(data)
    t1 = time.perf_counter()

    assert result == sorted(data)
    print(f"✓ Sorted {SIZE:,} items in {t1 - t0:.2f} s "
          f"using {os.cpu_count() or 4} CPU cores")