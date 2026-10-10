#!/usr/bin/env python3
"""Concurrent redirect latency sampler; benchmark cache modes separately."""

import argparse
import concurrent.futures
import statistics
import time
import urllib.error
import urllib.request


def request(url):
    started = time.perf_counter()
    try:
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, new_url):
                return None

        opener = urllib.request.build_opener(NoRedirect, urllib.request.HTTPHandler())
        req = urllib.request.Request(url, method="GET")
        try:
            opener.open(req, timeout=10).close()
        except urllib.error.HTTPError as response:
            if not 300 <= response.code < 400:
                return time.perf_counter() - started, f"unexpected HTTP status {response.code}"
    except Exception as error:  # noqa: BLE001
        return time.perf_counter() - started, str(error)
    return time.perf_counter() - started, None


def percentile(samples, fraction):
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="full redirect URL, e.g. http://localhost:8080/abc123")
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=50)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1:
        parser.error("--requests and --concurrency must be positive")
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        results = list(executor.map(request, [args.url] * args.requests))
    elapsed = time.perf_counter() - started
    samples = [duration * 1000 for duration, error in results if error is None]
    errors = sum(error is not None for _, error in results)
    if not samples:
        raise SystemExit("All requests failed")
    print(f"requests={args.requests} concurrency={args.concurrency} errors={errors} throughput={len(samples) / elapsed:.1f} req/s")
    print(f"p50={percentile(samples, .50):.2f}ms p95={percentile(samples, .95):.2f}ms p99={percentile(samples, .99):.2f}ms mean={statistics.mean(samples):.2f}ms")


if __name__ == "__main__":
    main()
