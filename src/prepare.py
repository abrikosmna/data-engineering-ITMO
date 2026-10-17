import argparse
import csv
import json
import tarfile
from contextlib import closing
from pathlib import Path

from botocore.exceptions import ClientError

from common import BUCKET, KEY, ROOT, client, ensure_directories, save_json

FIELDS = [
    "vacancy_id",
    "name",
    "role",
    "area",
    "salary_from",
    "salary_to",
    "currency",
    "gross",
    "experience",
    "key_skills",
    "published_at",
    "origin",
]
RECOVERY_BUCKET = "it-vacancies"
RECOVERY_KEY = "dataset/it_vacancies_2024.csv"


def file_stats(path):
    nul_bytes = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            nul_bytes += chunk.count(b"\0")
    return path.stat().st_size, nul_bytes


def ensure_bucket(storage):
    try:
        storage.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        storage.create_bucket(Bucket=BUCKET)


def recover_csv(storage):
    target = ROOT / "data/recovered.csv"
    response = storage.get_object(Bucket=RECOVERY_BUCKET, Key=RECOVERY_KEY)
    with closing(response["Body"]) as body, target.open("wb") as stream:
        for chunk in body.iter_chunks(4 * 1024 * 1024):
            stream.write(chunk)
    return target


def read_csv_records(source):
    records = {}
    total_rows = 0
    duplicates = 0
    with source.open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream, strict=True):
            if None in row or any(value is None for value in row.values()):
                raise ValueError("Incomplete CSV row")
            total_rows += 1
            vacancy_id = row["vacancy_id"]
            if vacancy_id in records:
                duplicates += 1
                continue
            records[vacancy_id] = {field: row.get(field, "") for field in FIELDS}
            records[vacancy_id]["origin"] = "csv"
    return records, total_rows, duplicates


def json_record(vacancy):
    salary = vacancy.get("salary") or {}
    roles = vacancy.get("professional_roles") or []
    role = next(
        (item["name"] for item in roles if item.get("id") == "96"),
        roles[0]["name"] if roles else "",
    )
    key_skills = [
        item["name"] if isinstance(item, dict) else item
        for item in vacancy.get("key_skills", [])
    ]
    return {
        "vacancy_id": str(vacancy.get("vacancy_id") or vacancy["id"]),
        "name": vacancy["name"],
        "role": role,
        "area": vacancy["area"]["name"],
        "salary_from": salary.get("from") or "",
        "salary_to": salary.get("to") or "",
        "currency": salary.get("currency") or "",
        "gross": str(salary.get("gross", "")).lower(),
        "experience": (vacancy.get("experience") or {}).get("name", ""),
        "key_skills": "; ".join(key_skills),
        "published_at": str(vacancy.get("published_at", "")),
        "origin": "hh_json",
    }


def add_json_records(records, directory):
    paths = sorted(directory.glob("*.json"))
    if not paths:
        raise ValueError(f"No JSON files in {directory}")
    overlaps = 0
    added = 0
    for path in paths:
        content = path.read_bytes()
        vacancy = json.loads(content)
        vacancy_id = str(vacancy.get("vacancy_id") or vacancy["id"])
        if vacancy_id in records:
            overlaps += 1
            continue
        records[vacancy_id] = json_record(vacancy)
        added += 1
    return {
        "hh_files": len(paths),
        "hh_overlap": overlaps,
        "hh_added": added,
    }


def make_json_archive(directory):
    target = ROOT / "data/hh.tar.gz"
    with tarfile.open(target, "w:gz") as archive:
        for path in sorted(directory.glob("*.json")):
            archive.add(path, arcname="hh/" + path.name)
    return target


def validate_prepared(source):
    rows = 0
    vacancy_ids = set()
    with source.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, strict=True)
        if reader.fieldnames != FIELDS:
            raise ValueError("Unexpected columns in prepared CSV")
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError("Incomplete prepared CSV record")
            if row["vacancy_id"] in vacancy_ids:
                raise ValueError("Repeated vacancy_id in prepared CSV")
            vacancy_ids.add(row["vacancy_id"])
            rows += 1
    return rows


def main():
    parser = argparse.ArgumentParser()
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--csv", type=Path)
    sources.add_argument("--prepared", type=Path)
    sources.add_argument("--recover-s3", action="store_true")
    parser.add_argument("--hh", type=Path)
    args = parser.parse_args()
    if not args.prepared and (args.hh is None or not args.hh.is_dir()):
        parser.error(
            "--hh must point to the JSON directory when using --csv or --recover-s3"
        )

    ensure_directories()
    storage = client()
    ensure_bucket(storage)
    audit = {"mode": "prepared" if args.prepared else "raw"}
    if args.prepared:
        target = args.prepared
        audit["canonical_rows"] = validate_prepared(target)
        json_archive = args.hh or ROOT / "data/hh.tar.gz"
        if not json_archive.is_file():
            parser.error("Prepared mode requires --hh data/hh.tar.gz")
        with tarfile.open(json_archive, "r:gz") as archive:
            json_files = [
                member
                for member in archive.getmembers()
                if member.isfile() and member.name.endswith(".json")
            ]
            for member in json_files:
                with archive.extractfile(member) as stream:
                    json.load(stream)
        audit["hh_files"] = len(json_files)
    else:
        source = recover_csv(storage) if args.recover_s3 else args.csv
        source_bytes, nul_bytes = file_stats(source)
        if nul_bytes:
            raise ValueError("CSV contains NUL bytes; use an intact source")
        records, total_rows, duplicates = read_csv_records(source)
        audit.update(
            csv_rows=total_rows,
            csv_duplicates=duplicates,
            source_bytes=source_bytes,
            recovered_from_s3=args.recover_s3,
        )
        audit.update(add_json_records(records, args.hh))
        target = ROOT / "data/vacancies.csv"
        with target.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(records.values())
        audit["canonical_rows"] = len(records)
        json_archive = make_json_archive(args.hh)
        storage.upload_file(str(source), BUCKET, "variant8/raw/it_vacancies_2024.csv")

    canonical_bytes, nul_bytes = file_stats(target)
    if nul_bytes:
        raise ValueError("Prepared CSV contains NUL bytes")
    audit["canonical_bytes"] = canonical_bytes
    storage.upload_file(str(target), BUCKET, KEY)
    storage.upload_file(str(json_archive), BUCKET, "variant8/raw/hh.tar.gz")
    audit_path = ROOT / "evidence/data_audit.json"
    save_json(audit_path, audit)
    storage.upload_file(str(audit_path), BUCKET, "variant8/data_audit.json")
    print(json.dumps(audit, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
