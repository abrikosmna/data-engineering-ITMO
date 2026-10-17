import argparse
import json

from common import BUCKET, ROOT, client, ensure_directories, save_json, write_csv
from duckdb_counts import connection


def query_csv(database, query, filename, parameters=None):
    cursor = database.execute(query, parameters)
    fields = [column[0] for column in cursor.description]
    rows = [dict(zip(fields, values)) for values in cursor.fetchall()]
    for row in rows:
        for field, value in row.items():
            if isinstance(value, list):
                row[field] = " | ".join(value)
    write_csv(ROOT / "results" / filename, rows, fields)
    return rows


def analysis_summary(database):
    fields = [
        "all_rows",
        "variant_rows",
        "with_skills",
        "salary_rub_rows",
        "currency_other_rows",
        "without_salary_rows",
    ]
    values = database.execute("""
        SELECT
            (SELECT count(*) FROM input),
            count(*),
            count(*) FILTER (WHERE len(skills) > 0),
            (SELECT count(*) FROM valid_paid),
            count(*) FILTER (WHERE currency NOT IN ('RUR', 'RUB', '')),
            count(*) FILTER (
                WHERE coalesce(salary_from, '') = ''
                  AND coalesce(salary_to, '') = ''
            )
        FROM segment
    """).fetchone()
    summary = dict(zip(fields, values))
    salary_bounds = database.execute("""
        SELECT
            count(*) FILTER (
                WHERE TRY_CAST(salary_from AS DOUBLE) > 0
                  AND TRY_CAST(salary_to AS DOUBLE) > 0
            ),
            count(*) FILTER (
                WHERE TRY_CAST(salary_from AS DOUBLE) > 0
                  AND coalesce(TRY_CAST(salary_to AS DOUBLE), 0) <= 0
            ),
            count(*) FILTER (
                WHERE coalesce(TRY_CAST(salary_from AS DOUBLE), 0) <= 0
                  AND TRY_CAST(salary_to AS DOUBLE) > 0
            )
        FROM valid_paid
    """).fetchone()
    summary["salary_bounds"] = dict(zip(["both_bounds", "lower_only", "upper_only"], salary_bounds))
    summary["currency"] = database.execute(
        "SELECT currency, count(*) FROM segment GROUP BY currency ORDER BY count(*) DESC"
    ).fetchall()
    summary["gross"] = database.execute(
        "SELECT gross, count(*) FROM valid_paid GROUP BY gross"
    ).fetchall()
    summary["origins"] = database.execute(
        "SELECT origin, count(*) FROM segment GROUP BY origin"
    ).fetchall()
    summary["median_salary_rub"] = database.execute(
        "SELECT median(salary_rub) FROM valid_paid"
    ).fetchone()[0]
    summary["region_selection"] = (
        "Top 3 areas by all variant vacancies; no restriction to paid/with skills"
    )
    summary["regions"] = [
        row[0]
        for row in database.execute("""
        SELECT area, count(*) AS vacancies
        FROM segment
        GROUP BY area
        ORDER BY vacancies DESC, area
        LIMIT 3
    """).fetchall()
    ]
    return summary


def export_regions(database, regions):
    region_list = ",".join("'" + region.replace("'", "''") + "'" for region in regions)
    query_csv(
        database,
        f"""
        SELECT
            s.area,
            count(*) AS vacancies,
            count(*) FILTER (WHERE len(s.skills) > 0) AS with_skills,
            count(p.salary_rub) AS salary_n,
            round(100 * count(p.salary_rub) / count(*), 2) AS salary_pct,
            median(p.salary_rub) AS median_rub,
            quantile_cont(p.salary_rub, .25) AS p25_rub,
            quantile_cont(p.salary_rub, .75) AS p75_rub,
            avg(len(s.skills)) AS mean_skills
        FROM segment s
        LEFT JOIN valid_paid p USING (vacancy_id)
        WHERE s.area IN ({region_list})
        GROUP BY s.area
        ORDER BY vacancies DESC
    """,
        "regions_overview.csv",
    )
    query_csv(
        database,
        f"""
        WITH frequency AS (
            SELECT
                c.area,
                c.combo,
                len(c.combo) AS k,
                count(*) AS vacancies,
                count(p.salary_rub) AS salary_n,
                median(p.salary_rub) AS median_rub
            FROM combos c
            LEFT JOIN valid_paid p USING (vacancy_id)
            WHERE c.area IN ({region_list})
            GROUP BY c.area, c.combo
        ), ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY area, k ORDER BY vacancies DESC, combo
            ) AS rn
            FROM frequency
        )
        SELECT area, k, combo, vacancies, salary_n, median_rub
        FROM ranked
        WHERE rn <= 5
        ORDER BY area, k, rn
    """,
        "regions_skills.csv",
    )
    query_csv(
        database,
        f"""
        SELECT area, experience, gross, count(*) AS salary_n,
               median(salary_rub) AS median_rub
        FROM valid_paid
        WHERE area IN ({region_list})
        GROUP BY area, experience, gross
        ORDER BY area, salary_n DESC
    """,
        "region_experience_gross.csv",
    )


def select_salary_leaders(effects):
    leaders = []
    for size in (2, 3):
        candidates = [row for row in effects if row["k"] == size]
        for direction in ("up", "down"):
            positive = direction == "up"
            candidates_with_sign = [
                row
                for row in candidates
                if (row["delta_rub"] > 0 if positive else row["delta_rub"] < 0)
            ]
            chosen = sorted(
                candidates_with_sign, key=lambda row: row["delta_pct"], reverse=positive
            )[:5]
            leaders.extend({"direction": direction, **row} for row in chosen)
    return leaders


def export_sensitivity(database, leaders):
    sensitivity = []
    stratified = []
    for row in leaders:
        combination = row["combo"].split(" | ")
        base = row["base_combo"].split(" | ")
        added_skill = row["added_skill"]
        base_count, base_median = database.execute(
            """
            SELECT count(*), median(salary_rub)
            FROM valid_paid
            WHERE list_has_all(skills, ?)
              AND NOT list_contains(skills, ?)
        """,
            [base, added_skill],
        ).fetchone()
        sensitivity.append(
            {
                **row,
                "base_without_added_n": base_count,
                "base_without_added_median": base_median,
                "delta_vs_disjoint_rub": row["median_rub"] - base_median if base_median else None,
            }
        )
        groups = database.execute(
            """
            SELECT
                area, experience, gross,
                count(*) FILTER (WHERE list_has_all(skills, ?)) AS combo_n,
                median(salary_rub) FILTER (WHERE list_has_all(skills, ?)) AS combo_median,
                count(*) AS base_n,
                median(salary_rub) AS base_median
            FROM valid_paid
            WHERE list_has_all(skills, ?)
            GROUP BY area, experience, gross
            HAVING count(*) FILTER (WHERE list_has_all(skills, ?)) >= 5
               AND count(*) >= 10
        """,
            [combination, combination, base, combination],
        ).fetchall()
        for (
            area,
            experience,
            gross,
            combo_n,
            combo_median,
            base_n,
            group_median,
        ) in groups:
            stratified.append(
                {
                    "combo": row["combo"],
                    "area": area,
                    "experience": experience,
                    "gross": gross,
                    "combo_n": combo_n,
                    "combo_median": combo_median,
                    "base_n": base_n,
                    "base_median": group_median,
                    "delta_rub": combo_median - group_median,
                }
            )
    write_csv(ROOT / "results/salary_sensitivity.csv", sensitivity)
    write_csv(ROOT / "results/salary_stratified.csv", stratified)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-upload", action="store_true")
    args = parser.parse_args()
    ensure_directories()
    database = connection()
    segment_query = (ROOT / "sql/counts.sql").read_text(encoding="utf-8")
    database.execute(segment_query.split("CREATE TEMP TABLE ranking")[0])
    database.execute((ROOT / "sql/analysis.sql").read_text(encoding="utf-8"))
    summary = analysis_summary(database)
    export_regions(database, summary["regions"])
    query_csv(
        database,
        """
        SELECT * FROM combo_stats
        ORDER BY len(combo), vacancies DESC, combo
    """,
        "salary_combo_stats.csv",
    )
    effects = query_csv(
        database,
        """
        SELECT * FROM salary_effect ORDER BY k, delta_pct DESC, combo
    """,
        "salary_effects.csv",
    )
    leaders = select_salary_leaders(effects)
    write_csv(ROOT / "results/salary_leaders.csv", leaders)
    export_sensitivity(database, leaders)
    query_csv(
        database,
        """
        SELECT vacancy_id, name, role, area, origin
        FROM segment ORDER BY vacancy_id
    """,
        "variant_vacancies.csv",
    )
    query_csv(
        database,
        """
        SELECT skill, count(*) AS vacancies
        FROM segment, UNNEST(skills) AS t(skill)
        GROUP BY skill
        ORDER BY vacancies DESC, skill
    """,
        "skills_top.csv",
    )
    save_json(ROOT / "results/analysis_summary.json", summary)

    if not args.no_upload:
        storage = client()
        for path in (ROOT / "results").glob("*"):
            if path.is_file():
                storage.upload_file(str(path), BUCKET, "variant8/results/" + path.name)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
