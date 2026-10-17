import datetime
import json
from contextlib import closing

from common import BUCKET, KEY, ROOT, client, ensure_directories, save_json

RESULT_FILES = (
    "frequency_5.csv",
    "frequency_10.csv",
    "frequency_20.csv",
    "frequency_all.csv",
    "verification.json",
    "fault_verification.json",
    "profile_runs.csv",
    "regions_overview.csv",
    "regions_skills.csv",
    "salary_leaders.csv",
)


def main():
    ensure_directories()
    storage = client()
    response = storage.get_object(Bucket=BUCKET, Key=KEY)
    input_bytes = 0
    local_path = ROOT / "data/vacancies.csv"
    with closing(response["Body"]) as body, local_path.open("rb") as stream:
        for chunk in body.iter_chunks(1024 * 1024):
            if chunk != stream.read(len(chunk)):
                raise AssertionError("S3 input does not match data/vacancies.csv")
            input_bytes += len(chunk)
        if stream.read(1):
            raise AssertionError("S3 input is shorter than data/vacancies.csv")

    checks = []
    for name in RESULT_FILES:
        local_content = (ROOT / "results" / name).read_bytes()
        response = storage.get_object(Bucket=BUCKET, Key="variant8/results/" + name)
        with closing(response["Body"]) as body:
            remote_content = body.read()
        checks.append(
            {
                "key": "variant8/results/" + name,
                "bytes": len(remote_content),
                "matches_local": remote_content == local_content,
            }
        )
    if not all(check["matches_local"] for check in checks):
        raise AssertionError("S3 result objects differ from local files")

    objects = []
    paginator = storage.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="variant8/"):
        objects.extend(
            {"key": item["Key"], "bytes": item["Size"]}
            for item in page.get("Contents", [])
        )
    verification = {
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "bucket": BUCKET,
        "input_key": KEY,
        "input_bytes": input_bytes,
        "input_matches_local": True,
        "object_count": len(objects),
        "checks": checks,
        "objects": objects,
    }
    save_json(ROOT / "evidence/s3_verification.json", verification)
    with (ROOT / "evidence/s3_listing.txt").open("w") as stream:
        stream.write(
            f"$ python src/evidence.py\nBucket: {BUCKET}\nInput matches local bytes: True\n"
        )
        for item in objects:
            stream.write(f"{item['bytes']:>12}  s3://{BUCKET}/{item['key']}\n")
        stream.write(
            f"All {len(checks)} selected result objects match local bytes: True\n"
        )
    summary = {
        key: value
        for key, value in verification.items()
        if key not in ("checks", "objects")
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
