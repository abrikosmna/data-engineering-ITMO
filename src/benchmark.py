import json
import platform
import statistics
import subprocess
import sys
import time

import psutil

from common import BUCKET, ROOT, client, ensure_directories, save_json, write_csv

STACK_SIZES = (5, 10, 20, 0)
ATTEMPTS = (1, 2, 3)
METHODS = (
    ("mr_baseline", "mapreduce.py", ["--baseline"]),
    ("mr_optimized", "mapreduce.py", []),
    ("duckdb", "duckdb_counts.py", []),
)


def measured(args, label, size, attempt):
    output = ROOT / f"results/{label}_{size}_{attempt}.json"
    command = [sys.executable, *args, "--size", str(size), "--output", str(output)]
    started = time.perf_counter()
    peak_rss = 0
    samples = 0

    with (ROOT / f"evidence/{label}_{size}_{attempt}.log").open("w") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        coordinator = psutil.Process(process.pid)
        while process.poll() is None:
            try:
                process_tree = [coordinator, *coordinator.children(recursive=True)]
                current_rss = sum(
                    child.memory_info().rss for child in process_tree if child.is_running()
                )
                peak_rss = max(peak_rss, current_rss)
                samples += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            time.sleep(0.01)
        if process.returncode:
            raise RuntimeError(f"Failed {command}: see {log.name}")

    if not samples or not peak_rss:
        raise RuntimeError("Could not measure process memory")
    measurement = {
        "method": label,
        "stack_size": size or "all",
        "attempt": attempt,
        "wall_seconds": time.perf_counter() - started,
        "peak_tree_rss_mib": peak_rss / 1024**2,
        "samples": samples,
    }
    print(json.dumps(measurement), flush=True)
    return measurement, output


def save_environment():
    environment = {
        "platform": platform.platform(),
        "python": sys.version,
        "logical_cpus": psutil.cpu_count(),
        "total_ram_gib": psutil.virtual_memory().total / 1024**3,
        "sampling_seconds": 0.01,
        "memory_metric": "sum RSS of coordinator and mapper processes; shared pages may be counted repeatedly",
        "repeats": len(ATTEMPTS),
        "cache": "warm local S3/filesystem caches, fresh Python process per run; no cache flushing",
        "duckdb_threads": 2,
        "mapreduce_workers": 2,
    }
    save_json(ROOT / "evidence/environment.json", environment)


def summarize_measurements(measurements):
    summary = []
    groups = sorted({(row["method"], str(row["stack_size"])) for row in measurements})
    for method, size in groups:
        attempts = [
            row for row in measurements if (row["method"], str(row["stack_size"])) == (method, size)
        ]
        times = [row["wall_seconds"] for row in attempts]
        memory_peaks = [row["peak_tree_rss_mib"] for row in attempts]
        summary.append(
            {
                "method": method,
                "stack_size": size,
                "runs": len(attempts),
                "median_seconds": statistics.median(times),
                "min_seconds": min(times),
                "max_seconds": max(times),
                "median_peak_mib": statistics.median(memory_peaks),
                "max_peak_mib": max(memory_peaks),
            }
        )
    return summary


def verify_mapper_retry(normal_result):
    measurement, output = measured([str(ROOT / "src/mapreduce.py"), "--fault"], "mr_fault", 20, 1)
    failed_run = json.loads(output.read_text())
    counts_equal = failed_run["counts"] == normal_result["counts"]
    vacancies_equal = failed_run["vacancies"] == normal_result["vacancies"]
    if not counts_equal or not vacancies_equal or failed_run["mapper_failures"] != 1:
        raise AssertionError("Mapper retry changed the result")
    save_json(
        ROOT / "results/fault_verification.json",
        {
            "injected_mapper_errors": 1,
            "observed_mapper_errors": failed_run["mapper_failures"],
            "retry_limit": 1,
            "counts_equal": counts_equal,
            "vacancies_equal": vacancies_equal,
            "profile": measurement,
            "failure_type": "task exception before result commit; worker process remains alive; executor termination is not tested",
        },
    )


def main():
    ensure_directories()
    save_environment()
    measurements = []
    checks = []
    all_results = {}

    for size in STACK_SIZES:
        reference = None
        for label, script, flags in METHODS:
            for attempt in ATTEMPTS:
                measurement, output = measured(
                    [str(ROOT / "src" / script), *flags], label, size, attempt
                )
                measurements.append(measurement)
                result = json.loads(output.read_text())
                signature = (
                    result["vacancies"],
                    result["vocabulary"],
                    result["counts"],
                )
                if reference is None:
                    reference = signature
                matches = signature == reference
                checks.append(
                    {
                        "stack_size": size or "all",
                        "method": label,
                        "attempt": attempt,
                        "full_equality": matches,
                        "rows_compared": len(result["counts"]),
                    }
                )
                if not matches:
                    raise AssertionError(f"Mismatch for {label}, stack {size}")
                all_results[(label, size)] = result
                write_csv(ROOT / "results/profile_runs.csv", measurements)
                save_json(ROOT / "results/verification.json", checks)

        result = all_results[("mr_optimized", size)]
        frequency_rows = [
            {"k": row["k"], "skills": " | ".join(row["skills"]), "count": row["count"]}
            for row in result["counts"]
        ]
        write_csv(ROOT / f"results/frequency_{size or 'all'}.csv", frequency_rows)

    verify_mapper_retry(all_results[("mr_optimized", 20)])
    write_csv(ROOT / "results/profile_summary.csv", summarize_measurements(measurements))
    growth = []
    for size in STACK_SIZES:
        result = all_results[("mr_optimized", size)]
        growth.append(
            {
                "stack_size": size or "all",
                "vocabulary_size": len(result["vocabulary"]),
                "vacancies": result["vacancies"],
                **{f"events_{size}": result["events"].get(str(size), 0) for size in (1, 2, 3)},
                **{f"unique_{size}": result["unique"].get(str(size), 0) for size in (1, 2, 3)},
            }
        )
    write_csv(ROOT / "results/growth.csv", growth)

    storage = client()
    for path in (ROOT / "results").glob("*"):
        if path.is_file():
            storage.upload_file(str(path), BUCKET, "variant8/results/" + path.name)
    print("VERIFIED all 36 runs and mapper retry; results uploaded to hh-lake", flush=True)


if __name__ == "__main__":
    main()
