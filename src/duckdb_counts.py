import argparse
import json
import os
import time

import duckdb

from common import BUCKET, KEY, ROOT, save_json


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def connection():
    database = duckdb.connect()
    database.execute("SET threads = 2")
    database.execute("SET memory_limit = '1GB'")
    database.execute("INSTALL httpfs; LOAD httpfs")

    endpoint = os.getenv("S3_ENDPOINT", "http://127.0.0.1:19000")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    access_key = sql_literal(os.getenv("S3_ACCESS_KEY", "labstudent"))
    secret_key = sql_literal(os.getenv("S3_SECRET_KEY", "HdfsLab2026LocalOnly"))
    database.execute(
        f"""
        CREATE SECRET rustfs (
            TYPE s3,
            KEY_ID {access_key},
            SECRET {secret_key},
            ENDPOINT {sql_literal(endpoint)},
            URL_STYLE 'path',
            USE_SSL false,
            REGION 'us-east-1'
        )
        """
    )
    database.execute(
        f"""
        CREATE TEMP TABLE input AS
        SELECT *
        FROM read_csv('s3://{BUCKET}/{KEY}', header = true, all_varchar = true)
        """
    )
    return database


def compute(size):
    database = connection()
    stack_limit = size or 1_000_000
    query = (ROOT / "sql/counts.sql").read_text(encoding="utf-8")
    database.execute(query.replace("$stack_limit", str(stack_limit)))
    rows = database.execute("SELECT skills, k, count FROM frequency ORDER BY k, skills").fetchall()
    vocabulary = database.execute(
        "SELECT skill FROM ranking ORDER BY n DESC, skill LIMIT ?", [stack_limit]
    ).fetchall()

    return {
        "method": "duckdb",
        "stack_size": size or "all",
        "vacancies": database.execute("SELECT count(*) FROM segment").fetchone()[0],
        "vocabulary": [row[0] for row in vocabulary],
        "events": dict(
            database.execute("SELECT k, sum(count)::BIGINT FROM frequency GROUP BY k").fetchall()
        ),
        "unique": dict(database.execute("SELECT k, count(*) FROM frequency GROUP BY k").fetchall()),
        "counts": [
            {"skills": combination, "k": size, "count": count} for combination, size, count in rows
        ],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=20)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.size < 0:
        parser.error("--size must be nonnegative; use 0 for the full vocabulary")

    started = time.perf_counter()
    result = compute(args.size)
    result["compute_seconds"] = time.perf_counter() - started
    save_json(args.output, result)
    summary = {key: value for key, value in result.items() if key not in ("counts", "vocabulary")}
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
