-- 1

DROP FUNCTION IF EXISTS get_transferred_points_table();
CREATE OR REPLACE FUNCTION get_transferred_points_table()
RETURNS TABLE (
    Peer1 varchar,
    Peer2 varchar,
    PointsAmount integer
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        tp.checkingpeer as Peer1,
        tp.checkedpeer as Peer2,
        tp.pointsamount as PointsAmount
    FROM
        TransferredPoints tp;

END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_transferred_points_table();

-- 2

DROP FUNCTION IF EXISTS get_successful_checks();
CREATE OR REPLACE FUNCTION get_successful_checks()
RETURNS TABLE (peer VARCHAR, task VARCHAR, xp BIGINT) AS
$$
    SELECT peer, split_part(task, '_', 1) AS task, xpamount AS xp
    FROM checks JOIN p2p
    ON checks.id = p2p."Check"
    LEFT JOIN verter
    ON checks.id = verter."Check"
    JOIN xp
    ON checks.id = xp."Check"
    WHERE p2p.state = 'Success' AND verter.state = 'Success';
$$LANGUAGE sql;

SELECT * FROM get_successful_checks();

-- 3

DROP FUNCTION IF EXISTS get_pirates_inside_campus(p_date date);
CREATE OR REPLACE FUNCTION get_pirates_inside_campus(p_date date)
RETURNS TABLE (
    Peer varchar
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        tt.peer AS Peer
    FROM
        TimeTracking tt
    WHERE
        tt.date = p_date
        AND NOT EXISTS (
            SELECT 1
            FROM
                TimeTracking tt2
            WHERE
                tt2.peer = tt.peer
                AND tt2.date = p_date
                AND tt2.state = '2'::character varying
        );

END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_pirates_inside_campus('2023-11-01'::date);


-- 4

DROP FUNCTION IF EXISTS get_points_change();
CREATE OR REPLACE FUNCTION get_points_change()
RETURNS TABLE (
    Peer varchar,
    PointsChange bigint
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.nickname AS Peer,
        COALESCE(SUM(CASE WHEN tp.CheckingPeer = p.nickname THEN tp.PointsAmount ELSE -tp.PointsAmount END), 0) AS PointsChange
    FROM
        Peers p
    LEFT JOIN
        TransferredPoints tp ON p.nickname = tp.CheckingPeer OR p.nickname = tp.CheckedPeer
    GROUP BY
        p.nickname
    ORDER BY
        PointsChange DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_points_change();

-- 5

DROP FUNCTION IF EXISTS get_points_change_alternative();
CREATE OR REPLACE FUNCTION get_points_change_alternative()
RETURNS TABLE (
    Peer varchar,
    PointsChange bigint
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.nickname AS Peer,
        COALESCE(SUM(CASE WHEN tp.Peer1 = p.nickname THEN tp.pointsamount ELSE -tp.pointsamount END), 0) AS PointsChange
    FROM
        Peers p
    LEFT JOIN
        get_transferred_points_table() tp ON p.nickname = tp.Peer1 OR p.nickname = tp.Peer2
    GROUP BY
        p.nickname
    ORDER BY
        PointsChange DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_points_change_alternative();

-- 6

DROP FUNCTION IF EXISTS get_most_checked_task_per_day();
CREATE OR REPLACE FUNCTION get_most_checked_task_per_day()
RETURNS TABLE (
    Day text,
    Task text
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        DISTINCT ON (pc.date) TO_CHAR(pc.date, 'DD.MM.YYYY') AS Day,
        SPLIT_PART(pc.task, '_', 1) AS Task
    FROM
        Checks pc
    JOIN
        P2P p2p ON pc.id = p2p."Check"
    GROUP BY
        pc.date, pc.task
    ORDER BY
        pc.date, COUNT(p2p.id) DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_most_checked_task_per_day();

-- 7

DROP FUNCTION IF EXISTS find_completed_block_peers(p_block_name VARCHAR); --исправил тип данных чтобы совпадало
CREATE OR REPLACE FUNCTION find_completed_block_peers(IN block_name VARCHAR)
RETURNS TABLE(peer VARCHAR, day DATE) AS
$$
    SELECT peer, "date" AS Day
    FROM checks JOIN p2p
    ON checks.id = p2p."Check"
    FULL JOIN verter
    ON checks.id = verter."Check"
    WHERE p2p.state = 'Success' AND (verter.state = 'Success')
        AND task = (SELECT MAX(title) FROM tasks
                                        WHERE title ~ ('^' || $1))
    ORDER BY Day DESC;
$$LANGUAGE SQL;

SELECT * FROM find_completed_block_peers('A');

-- 8

DROP FUNCTION IF EXISTS get_most_recommended_peer();
CREATE OR REPLACE FUNCTION get_most_recommended_peer()
RETURNS TABLE (
        peer VARCHAR,
        recommendedpeer VARCHAR
        ) AS $$
  BEGIN
        RETURN QUERY
          WITH all_friends AS (
               SELECT peer1, peer2
                 FROM friends

                UNION

               SELECT peer2, peer1
                 FROM friends

                ORDER BY peer1, peer2
               ),

               sub_rec AS (
               SELECT peer1, r.recommendedpeer, COUNT(*) AS Count
                 FROM recommendations AS r

                 JOIN all_friends
                   ON r.peer = peer2

                GROUP BY peer1, r.recommendedpeer
               HAVING peer1 != r.recommendedpeer
               )

        SELECT sr1.peer1 AS peer, sr1.recommendedpeer
          FROM sub_rec AS sr1
         WHERE sr1.Count = (
               SELECT MAX(Count)
                 FROM sub_rec AS sr2
                WHERE sr1.peer1 = sr2.peer1
               );
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_most_recommended_peer();

-- 9
DROP FUNCTION IF EXISTS calculate_block_percentage(p_block1 varchar, p_block2 varchar);
CREATE OR REPLACE FUNCTION calculate_block_percentage(
    p_block1 varchar,
    p_block2 varchar
)
RETURNS TABLE (
    started_block1_percent numeric,
    started_block2_percent numeric,
    started_both_blocks_percent numeric,
    didnt_start_any_block_percent numeric
)
AS $$
DECLARE
    total_peers_count int;
BEGIN
    SELECT COUNT(DISTINCT peer) INTO total_peers_count
    FROM Checks;

    started_block1_percent := (
        SELECT COUNT(DISTINCT peer)
        FROM Checks
        WHERE task LIKE p_block1 || '%'
    ) * 100.0 / total_peers_count;

    started_block2_percent := (
        SELECT COUNT(DISTINCT peer)
        FROM Checks
        WHERE task LIKE p_block2 || '%'
    ) * 100.0 / total_peers_count;

    started_both_blocks_percent := (
        SELECT COUNT(DISTINCT peer)
        FROM Checks
        WHERE task LIKE p_block1 || '%'
        AND peer IN (
            SELECT DISTINCT peer
            FROM Checks
            WHERE task LIKE p_block2 || '%'
        )
    ) * 100.0 / total_peers_count;

    didnt_start_any_block_percent := 100.0 - started_block1_percent - started_block2_percent - started_both_blocks_percent;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM calculate_block_percentage('CPP', 'A');

-- 10

DROP PROCEDURE IF EXISTS get_percent_of_birthday_checked_peers;
CREATE OR REPLACE PROCEDURE get_percent_of_birthday_checked_peers(
        OUT successfulchecks   INTEGER,
        OUT unsuccessfulchecks INTEGER
        ) AS $$
DECLARE
        all_peers INTEGER;
  BEGIN
        SELECT COUNT(*)
          INTO all_peers
          FROM peers;

        CREATE TEMPORARY TABLE bd_success_checked_peers AS
        SELECT DISTINCT nickname
          FROM peers
          JOIN checks
               ON nickname = peer
               AND  EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM "date")
               AND EXTRACT(DAY FROM birthday) = EXTRACT(DAY FROM "date")
          JOIN p2p
               ON checks.id = p2p."Check"
         WHERE p2p.state = 'Success';

        CREATE TEMPORARY TABLE bd_failure_checked_peers AS
        SELECT DISTINCT nickname
          FROM peers
          JOIN checks
               ON nickname = peer
               AND  EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM "date")
               AND EXTRACT(DAY FROM birthday) = EXTRACT(DAY FROM "date")
          JOIN p2p
               ON checks.id = p2p."Check"
         WHERE p2p.state = 'Failure';

        SELECT ((SELECT COUNT(*)
               FROM bd_success_checked_peers)
               * 100 / all_peers)
               INTO successfulchecks;

        SELECT ((SELECT COUNT(*)
          FROM bd_failure_checked_peers
          ) * 100 / all_peers)
          INTO unsuccessfulchecks;

        DROP TABLE IF EXISTS bd_success_checked_peers;
        DROP TABLE IF EXISTS bd_failure_checked_peers;
    END;
$$ LANGUAGE plpgsql;

CALL get_percent_of_birthday_checked_peers(successfulchecks := 0, unsuccessfulchecks := 0);

-- 11

DROP FUNCTION IF EXISTS peers_did_given_task(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR);
CREATE OR REPLACE FUNCTION peers_did_given_task(IN task1 VARCHAR, IN task2 VARCHAR, IN task3 VARCHAR)
RETURNS SETOF VARCHAR AS $$
    WITH success_task1 AS (
    SELECT peer
    FROM ((SELECT peer, "Check", state AS p2p_state
           FROM checks JOIN p2p
                        ON checks.id = p2p."Check"
           WHERE task ~ ('^' || $1 || '$') AND state = 'Success') AS p2p_success
          LEFT JOIN
           (SELECT "Check", state AS vert_state FROM verter) AS verter_success
                ON p2p_success."Check" = verter_success."Check") AS res_tab
    WHERE res_tab.vert_state = 'Success' OR res_tab.vert_state IS NULL
    ),
    success_task2 AS (
        SELECT peer
        FROM ((SELECT peer, "Check", state AS p2p_state
               FROM checks JOIN p2p
                            ON checks.id = p2p."Check"
               WHERE task ~ ('^' || $2 || '$') AND state = 'Success') AS p2p_success
              LEFT JOIN
               (SELECT "Check", state AS vert_state FROM verter) AS verter_success
                    ON p2p_success."Check" = verter_success."Check") AS res_tab
        WHERE res_tab.vert_state = 'Success' OR res_tab.vert_state IS NULL
    ),
    fail_task3 AS (
        (SELECT peer
        FROM checks JOIN p2p
                    ON checks.id = p2p."Check"
        WHERE task ~ ('^' || $3 || '$') AND state = 'Failure')
        UNION
        (SELECT peer
         FROM checks JOIN verter
                        ON checks.id = verter."Check"
        WHERE task ~ ('^' || $3 || '$') AND state = 'Failure')
    ),
    not_pass_task AS (
        SELECT nickname FROM peers
        WHERE NOT EXISTS (SELECT peer FROM checks WHERE peer = nickname AND task ~ ('^' || $3 || '$'))
    )

    SELECT * FROM success_task1
    INTERSECT
    SELECT * FROM success_task2
    INTERSECT
    (SELECT * FROM fail_task3
    UNION
    SELECT * FROM not_pass_task);

$$ LANGUAGE sql;

SELECT * FROM peers_did_given_task('A2_SimpleNavigator_v1.0', 'A1_Maze', 'C2_SimpleBashUtils');

-- 12

WITH RECURSIVE TaskHierarchy AS (
    SELECT
        t.title AS Task,
        0 AS PrevCount
    FROM
        Tasks t
    WHERE
        t.parenttask IS NULL

    UNION ALL

    SELECT
        t.title AS Task,
        th.PrevCount + 1 AS PrevCount
    FROM
        Tasks t
    JOIN
        TaskHierarchy th ON t.parenttask = th.Task
)

SELECT
    Task,
    PrevCount
FROM
    TaskHierarchy
ORDER BY
    Task;

-- 13

DROP FUNCTION IF EXISTS lucky_days(count INT);
CREATE OR REPLACE FUNCTION lucky_days(IN count INT)
RETURNS SETOF DATE AS
$$
    WITH res_t AS (
        SELECT checks.id AS checks_id, p2p.id AS p2p_id, date, p2p.time AS time,
           p2p.state AS p2p_state, verter.state AS vert_state, (xpamount * 100 / maxxp) AS percent_xp
    FROM checks
            JOIN p2p
                 ON checks.id = p2p."Check"
            LEFT JOIN verter
                 ON checks.id = verter."Check"
            LEFT JOIN xp
                      ON p2p."Check" = xp."Check"
            JOIN tasks
                 ON task = title
    WHERE p2p.state != 'Start' AND (verter.state = 'Success' OR verter.state = 'Failure' OR verter.state IS NULL)
    ORDER BY date, p2p.time
    ),
    row1_tab AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY time) AS row_num, *
        FROM res_t
    ),
    row2_tab AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY time) AS row2_num, *
        FROM row1_tab
        WHERE p2p_state = 'Success'
                AND (vert_state = 'Success' OR vert_state IS NULL)
                AND percent_xp >= 80
    )

    SELECT date
    FROM row2_tab
    WHERE row2_num - row_num = 0
    GROUP BY date
    HAVING count(*) >= $1;

$$LANGUAGE SQL;

SELECT * FROM lucky_days(2);

-- 14

DROP FUNCTION IF EXISTS getTopPeerXP();
CREATE OR REPLACE FUNCTION getTopPeerXP()
RETURNS TABLE (
    Peer VARCHAR,
    XP BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.nickname AS Peer,
        COALESCE(SUM(xp.xpamount), 0) AS XP
    FROM
        Peers p
    LEFT JOIN
        Checks c ON p.nickname = c.peer
    LEFT JOIN
        XP xp ON c.id = xp."Check"
    GROUP BY
        p.nickname
    ORDER BY
        XP DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM getTopPeerXP();

-- 15

DROP FUNCTION IF EXISTS get_early_comers(p_time TIME, p_count_threshold INTEGER);
CREATE OR REPLACE FUNCTION get_early_comers(
    p_time TIME,
    p_count_threshold INTEGER
)
RETURNS TABLE (
    peer_name VARCHAR
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.peer
    FROM
        TimeTracking t
    WHERE
        t.time < p_time
    GROUP BY
        t.peer
    HAVING
        COUNT(DISTINCT t.date) >= p_count_threshold;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_early_comers('12:00:00'::TIME, 1);

-- 16

DROP FUNCTION IF EXISTS get_frequent_leavers(p_days_interval INTEGER, p_count_threshold INTEGER);
CREATE OR REPLACE FUNCTION get_frequent_leavers(
    p_days_interval INTEGER,
    p_count_threshold INTEGER
)
RETURNS TABLE (
    peer_name VARCHAR
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.peer::VARCHAR
    FROM
        TimeTracking t
    WHERE
        t.date >= CURRENT_DATE - p_days_interval
        AND t.state = '2'::char
    GROUP BY
        t.peer
    HAVING
        COUNT(DISTINCT t.date) > p_count_threshold;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_frequent_leavers(40, 0);

-- 17

DROP FUNCTION IF EXISTS calculate_early_entries_percentage();
CREATE OR REPLACE FUNCTION calculate_early_entries_percentage()
RETURNS TABLE (
    month text,
    early_entries_percentage varchar
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        TO_CHAR(date_trunc('month', generate_series), 'Month') AS month,
        COALESCE(
            (100.0 * COUNT(t.time) FILTER (WHERE EXTRACT(HOUR FROM t.time) < 12) / COUNT(*))::varchar,
            '0'
        ) AS early_entries_percentage
    FROM
        generate_series(
            (SELECT MIN(date_trunc('month', date)) FROM TimeTracking),
            (SELECT MAX(date_trunc('month', date)) FROM TimeTracking),
            interval '1 month'
        ) AS generate_series
    LEFT JOIN
        TimeTracking t ON date_trunc('month', t.date) = date_trunc('month', generate_series)
    GROUP BY
        generate_series
    ORDER BY
        generate_series;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM calculate_early_entries_percentage();
