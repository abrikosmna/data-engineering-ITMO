import csv
import io
import json
import os
import re
import unicodedata
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
BUCKET = os.getenv("S3_BUCKET", "hh-lake")
KEY = "variant8/input/vacancies.csv"
ROLE = "Программист, разработчик"
PATTERN = r"front[\s-]?end|фронт[\s-]?енд|фронт[\s-]?энд"
SKILL_ALIASES = {
    "react.js": "react",
    "reactjs": "react",
    "vue.js": "vue",
    "vuejs": "vue",
    "nodejs": "node.js",
}


def client():
    config = Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 3},
    )
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://127.0.0.1:19000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "labstudent"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "HdfsLab2026LocalOnly"),
        region_name="us-east-1",
        config=config,
    )


def selected(row):
    return row["role"] == ROLE and bool(re.search(PATTERN, row["name"], re.IGNORECASE))


def skills(text):
    normalized = set()
    for value in text.split(";"):
        skill = unicodedata.normalize("NFC", value).strip().lower()
        if skill:
            normalized.add(SKILL_ALIASES.get(skill, skill))
    return sorted(normalized)


def rows_s3():
    response = client().get_object(Bucket=BUCKET, Key=KEY)
    with io.TextIOWrapper(response["Body"], encoding="utf-8", newline="") as stream:
        yield from csv.DictReader(stream)


def ensure_directories():
    for name in ("data", "results", "evidence"):
        (ROOT / name).mkdir(parents=True, exist_ok=True)


def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path, rows, fields=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    fields = fields or (list(rows[0]) if rows else [])
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
