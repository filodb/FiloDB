#!/usr/bin/env python3
"""
Fire N GET requests at a URL with a given parallelism and report latency stats.

Usage:
    python3 scripts/http_load_test.py <url> [--requests 1000] [--parallelism 10] [--timeout 30]

Only uses the standard library (urllib + concurrent.futures) so it runs with
no extra pip installs.
"""

import argparse
import concurrent.futures
import sys
import time
import urllib.error
import urllib.request


def do_request(url, timeout):
    """Issue a single GET request and return (latency_seconds, ok, error)."""
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            resp.read()
            ok = 200 <= resp.status < 400
    except Exception as exc:  # noqa: BLE001 - want to record any failure, not crash the run
        ok = False
        elapsed = time.perf_counter() - start
        return elapsed, ok, str(exc)
    elapsed = time.perf_counter() - start
    return elapsed, ok, None


def percentile(sorted_values, pct):
    """Nearest-rank percentile over an already-sorted list."""
    if not sorted_values:
        return float("nan")
    k = max(0, min(len(sorted_values) - 1, int(round(pct / 100.0 * len(sorted_values))) - 1))
    return sorted_values[k]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="URL to GET")
    parser.add_argument("--requests", "-n", type=int, default=1000, help="Total number of requests (default: 1000)")
    parser.add_argument("--parallelism", "-p", type=int, default=10, help="Number of concurrent requests (default: 10)")
    parser.add_argument("--timeout", type=float, default=30.0, help="Per-request timeout in seconds (default: 30)")
    args = parser.parse_args()

    latencies_ms = []
    errors = 0

    print(f"GET {args.url} x{args.requests} (parallelism={args.parallelism})")
    overall_start = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallelism) as pool:
        futures = [pool.submit(do_request, args.url, args.timeout) for _ in range(args.requests)]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), start=1):
            elapsed, ok, err = fut.result()
            latencies_ms.append(elapsed * 1000.0)
            if not ok:
                errors += 1
                if errors <= 10:
                    print(f"  request {i} failed: {err}", file=sys.stderr)

    overall_elapsed = time.perf_counter() - overall_start

    if not latencies_ms:
        print("No requests completed.", file=sys.stderr)
        sys.exit(1)

    latencies_ms.sort()
    n = len(latencies_ms)
    mean = sum(latencies_ms) / n

    print()
    print(f"Total requests : {n} ({errors} failed)")
    print(f"Total wall time: {overall_elapsed:.3f}s ({n / overall_elapsed:.1f} req/s)")
    print()
    print("Latency (ms):")
    print(f"  mean : {mean:.2f}")
    print(f"  min  : {latencies_ms[0]:.2f}")
    print(f"  p50  : {percentile(latencies_ms, 50):.2f}")
    print(f"  p95  : {percentile(latencies_ms, 95):.2f}")
    print(f"  p99  : {percentile(latencies_ms, 99):.2f}")
    print(f"  max  : {latencies_ms[-1]:.2f}")

    if errors:
        sys.exit(2)


if __name__ == "__main__":
    main()
