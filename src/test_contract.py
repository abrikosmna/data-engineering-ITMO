import unittest

import duckdb

from common import ROOT, selected
from mapreduce import mapper


class ContractTests(unittest.TestCase):
    def test_variant_boundary(self):
        self.assertTrue(
            selected(
                {
                    "role": "Программист, разработчик",
                    "name": "Senior Front-end developer",
                }
            )
        )
        self.assertTrue(
            selected(
                {
                    "role": "Программист, разработчик",
                    "name": "Фронтенд разработчик",
                }
            )
        )
        self.assertFalse(selected({"role": "Тестировщик", "name": "Frontend QA"}))
        self.assertFalse(
            selected(
                {
                    "role": "Программист, разработчик",
                    "name": "Python Backend developer",
                }
            )
        )

    def test_dedup_aliases_and_sql(self):
        rows = [
            {
                "vacancy_id": "1",
                "role": "Программист, разработчик",
                "name": "Frontend",
                "key_skills": "React; React.js; JavaScript; Git; Git",
            },
            {
                "vacancy_id": "2",
                "role": "Программист, разработчик",
                "name": "Backend",
                "key_skills": "Go; SQL",
            },
            {
                "vacancy_id": "3",
                "role": "Программист, разработчик",
                "name": "Frontend",
                "key_skills": "",
            },
        ]
        expected = mapper(rows, None, True)[0]
        self.assertEqual(expected, mapper(rows, None, False)[0])
        self.assertEqual(expected[("react",)], 1)
        pair_count = sum(count for combination, count in expected.items() if len(combination) == 2)
        self.assertEqual(pair_count, 3)
        self.assertEqual(expected[("git", "javascript", "react")], 1)

        database = duckdb.connect()
        database.execute("""
            CREATE TABLE input (
                vacancy_id VARCHAR,
                role VARCHAR,
                name VARCHAR,
                key_skills VARCHAR
            )
        """)
        database.executemany(
            "INSERT INTO input VALUES (?, ?, ?, ?)",
            [(row["vacancy_id"], row["role"], row["name"], row["key_skills"]) for row in rows],
        )
        query = (ROOT / "sql/counts.sql").read_text(encoding="utf-8")
        database.execute(query.replace("$stack_limit", "1000"))
        actual = {
            tuple(combination): count
            for combination, count in database.execute(
                "SELECT skills, count FROM frequency"
            ).fetchall()
        }
        self.assertEqual(dict(expected), actual)

    def test_one_sided_salary(self):
        database = duckdb.connect()
        database.execute("""
            CREATE TABLE segment (
                vacancy_id VARCHAR,
                area VARCHAR,
                experience VARCHAR,
                gross VARCHAR,
                skills VARCHAR[],
                salary_from VARCHAR,
                salary_to VARCHAR,
                currency VARCHAR
            )
        """)
        salary_rows = [
            (
                "1",
                "Москва",
                "От 1 года до 3 лет",
                "false",
                ["javascript"],
                "100000",
                "",
                "RUR",
            ),
            (
                "2",
                "Москва",
                "От 1 года до 3 лет",
                "false",
                ["javascript"],
                "",
                "120000",
                "RUR",
            ),
            (
                "3",
                "Москва",
                "От 1 года до 3 лет",
                "false",
                ["javascript"],
                "90000",
                "110000",
                "RUR",
            ),
        ]
        database.executemany("INSERT INTO segment VALUES (?, ?, ?, ?, ?, ?, ?, ?)", salary_rows)
        database.execute((ROOT / "sql/analysis.sql").read_text(encoding="utf-8"))
        result = database.execute("SELECT count(*), median(salary_rub) FROM valid_paid").fetchone()
        self.assertEqual(result, (3, 100000.0))

    def test_mapper_failure_is_visible(self):
        with self.assertRaisesRegex(RuntimeError, "Injected mapper failure"):
            mapper([], None, True, True)


if __name__ == "__main__":
    unittest.main()
