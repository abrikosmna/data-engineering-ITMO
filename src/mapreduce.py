import argparse
import json
import math
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from itertools import combinations

from common import rows_s3, save_json, selected, skills


def mapper(rows, vocab, optimized, fail=False):
    if fail:
        raise RuntimeError("Injected mapper failure before returning result")

    counts = Counter()
    emitted = []
    vacancies = 0
    events = Counter()

    for row in rows:
        if not selected(row):
            continue

        vacancies += 1
        stack = [skill for skill in skills(row["key_skills"]) if vocab is None or skill in vocab]
        for size in (1, 2, 3):
            if optimized:
                events[size] += math.comb(len(stack), size) if len(stack) >= size else 0
                counts.update(combinations(stack, size))
            else:
                events[size] += len(list(combinations(stack, size)))
                emitted.extend(combinations(stack, size))

    if not optimized:
        counts.update(emitted)
    return counts, vacancies, events


def collect_vocabulary(size, optimized):
    frequencies = Counter()
    buffered_rows = []
    for row in rows_s3():
        if selected(row):
            frequencies.update(skills(row["key_skills"]))
            if optimized:
                buffered_rows.append(row)

    ranked = sorted(frequencies, key=lambda skill: (-frequencies[skill], skill))
    vocabulary = set(ranked[:size]) if size else None
    return ranked, vocabulary, buffered_rows


def compute(size=20, optimized=True, fault=False, chunk_size=1000, workers=2):
    ranked, vocabulary, buffered_rows = collect_vocabulary(size, optimized)
    total = Counter()
    events = Counter()
    vacancies = 0
    failures = 0
    chunks = 0
    fault_sent = False

    with ProcessPoolExecutor(max_workers=workers) as pool:
        pending = {}

        def submit_chunk(batch):
            nonlocal fault_sent, chunks
            inject_failure = fault and not fault_sent
            fault_sent = fault_sent or inject_failure
            chunks += 1
            future = pool.submit(mapper, batch, vocabulary, optimized, inject_failure)
            pending[future] = (batch, 0)

        def collect_finished():
            nonlocal vacancies, failures
            finished, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in finished:
                batch, retries = pending.pop(future)
                try:
                    batch_counts, batch_vacancies, batch_events = future.result()
                except RuntimeError:
                    failures += 1
                    if retries >= 1:
                        raise
                    retry = pool.submit(mapper, batch, vocabulary, optimized, False)
                    pending[retry] = (batch, retries + 1)
                    continue

                total.update(batch_counts)
                vacancies += batch_vacancies
                events.update(batch_events)

        source = iter(buffered_rows) if optimized else rows_s3()
        chunk = []
        for row in source:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                submit_chunk(chunk)
                chunk = []
                if len(pending) >= 2 * workers:
                    collect_finished()

        if chunk:
            submit_chunk(chunk)
        while pending:
            collect_finished()

    counts = [
        {"skills": list(combination), "k": len(combination), "count": count}
        for combination, count in sorted(total.items(), key=lambda item: (len(item[0]), item[0]))
    ]
    return {
        "method": "mapreduce_optimized" if optimized else "mapreduce_baseline",
        "stack_size": size or "all",
        "vacancies": vacancies,
        "vocabulary": ranked[:size] if size else ranked,
        "chunks": chunks,
        "chunk_rows": chunk_size,
        "workers": workers,
        "mapper_failures": failures,
        "s3_passes": 1 if optimized else 2,
        "buffered_variant_rows": len(buffered_rows),
        "events": dict(events),
        "unique": dict(Counter(len(combination) for combination in total)),
        "counts": counts,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=20)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--fault", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.size < 0:
        parser.error("--size must be nonnegative; use 0 for the full vocabulary")

    started = time.perf_counter()
    result = compute(args.size, not args.baseline, args.fault)
    result["compute_seconds"] = time.perf_counter() - started
    save_json(args.output, result)
    summary = {key: value for key, value in result.items() if key not in ("counts", "vocabulary")}
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
