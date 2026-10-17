CREATE TEMP TABLE segment AS
SELECT
    *,
    list_sort(
        list_distinct(
            list_filter(
                list_transform(
                    string_split(key_skills, ';'),
                    skill -> CASE lower(trim(nfc_normalize(skill)))
                        WHEN 'react.js' THEN 'react'
                        WHEN 'reactjs' THEN 'react'
                        WHEN 'vue.js' THEN 'vue'
                        WHEN 'vuejs' THEN 'vue'
                        WHEN 'nodejs' THEN 'node.js'
                        ELSE lower(trim(nfc_normalize(skill)))
                    END
                ),
                skill -> skill <> ''
            )
        )
    ) AS skills
FROM input
WHERE role = 'Программист, разработчик'
    AND regexp_matches(
        lower(name),
        'front[\s-]?end|фронт[\s-]?енд|фронт[\s-]?энд'
    );

CREATE TEMP TABLE ranking AS
SELECT skill, count(*) AS n
FROM segment, UNNEST(skills) AS t(skill)
GROUP BY skill;

CREATE TEMP TABLE vocabulary AS
SELECT skill
FROM ranking
ORDER BY n DESC, skill
LIMIT $stack_limit;

CREATE TEMP TABLE stacks AS
SELECT vacancy_id, list(skill ORDER BY skill) AS s
FROM segment, UNNEST(skills) AS t(skill)
WHERE skill IN (SELECT skill FROM vocabulary)
GROUP BY vacancy_id;

CREATE TEMP TABLE frequency AS
SELECT [first_skill] AS skills, 1 AS k, count(*) AS count
FROM stacks, UNNEST(s) AS first(first_skill)
GROUP BY first_skill

UNION ALL

SELECT [first_skill, second_skill], 2, count(*)
FROM stacks,
    UNNEST(s) AS first(first_skill),
    UNNEST(s) AS second(second_skill)
WHERE first_skill < second_skill
GROUP BY first_skill, second_skill

UNION ALL

SELECT [first_skill, second_skill, third_skill], 3, count(*)
FROM stacks,
    UNNEST(s) AS first(first_skill),
    UNNEST(s) AS second(second_skill),
    UNNEST(s) AS third(third_skill)
WHERE first_skill < second_skill
    AND second_skill < third_skill
GROUP BY first_skill, second_skill, third_skill;
