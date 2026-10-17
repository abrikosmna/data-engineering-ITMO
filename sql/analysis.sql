CREATE TEMP TABLE paid AS
SELECT
    *,
    CASE
        WHEN TRY_CAST(salary_from AS DOUBLE) > 0
            AND TRY_CAST(salary_to AS DOUBLE) > 0
            THEN (
                TRY_CAST(salary_from AS DOUBLE)
                + TRY_CAST(salary_to AS DOUBLE)
            ) / 2
        WHEN TRY_CAST(salary_from AS DOUBLE) > 0
            THEN TRY_CAST(salary_from AS DOUBLE)
        ELSE TRY_CAST(salary_to AS DOUBLE)
    END AS salary_rub
FROM segment
WHERE currency IN ('RUR', 'RUB');

CREATE TEMP TABLE valid_paid AS
SELECT *
FROM paid
WHERE salary_rub > 0
    AND NOT coalesce(
        TRY_CAST(salary_from AS DOUBLE) > 0
            AND TRY_CAST(salary_to AS DOUBLE) > 0
            AND TRY_CAST(salary_from AS DOUBLE) > TRY_CAST(salary_to AS DOUBLE),
        false
    );

CREATE TEMP TABLE combos AS
SELECT vacancy_id, area, experience, gross, [first_skill] AS combo
FROM segment, UNNEST(skills) AS first(first_skill)

UNION ALL

SELECT vacancy_id, area, experience, gross, [first_skill, second_skill]
FROM segment,
    UNNEST(skills) AS first(first_skill),
    UNNEST(skills) AS second(second_skill)
WHERE first_skill < second_skill

UNION ALL

SELECT vacancy_id, area, experience, gross, [first_skill, second_skill, third_skill]
FROM segment,
    UNNEST(skills) AS first(first_skill),
    UNNEST(skills) AS second(second_skill),
    UNNEST(skills) AS third(third_skill)
WHERE first_skill < second_skill
    AND second_skill < third_skill;

CREATE TEMP TABLE combo_stats AS
SELECT
    combo,
    count(*) AS vacancies,
    count(p.salary_rub) AS salary_n,
    median(p.salary_rub) AS median_rub
FROM combos c
LEFT JOIN valid_paid p USING (vacancy_id)
GROUP BY combo;

CREATE TEMP TABLE chosen_base AS
WITH candidates AS (
    SELECT combo, list_filter(combo, (skill, position) -> position <> idx) AS base_candidate
    FROM combo_stats, UNNEST(range(1, len(combo) + 1)) AS t(idx)
    WHERE len(combo) IN (2, 3)
)
SELECT
    full_set.combo,
    base.combo AS base_combo,
    base.vacancies AS base_vacancies,
    base.salary_n AS base_salary_n,
    base.median_rub AS base_median_rub,
    row_number() OVER (
        PARTITION BY full_set.combo
        ORDER BY base.vacancies DESC, base.combo
    ) AS rn
FROM candidates full_set
JOIN combo_stats base ON base.combo = full_set.base_candidate;

CREATE TEMP TABLE salary_effect AS
SELECT
    f.combo,
    len(f.combo) AS k,
    f.vacancies,
    f.salary_n,
    f.median_rub,
    b.base_combo,
    b.base_vacancies,
    b.base_salary_n,
    b.base_median_rub,
    list_filter(f.combo, skill -> NOT list_contains(b.base_combo, skill)) AS added_skill,
    f.median_rub - b.base_median_rub AS delta_rub,
    100 * (f.median_rub / b.base_median_rub - 1) AS delta_pct
FROM combo_stats f
JOIN chosen_base b USING (combo)
WHERE b.rn = 1
    AND f.salary_n >= 20
    AND b.base_salary_n >= 50;
