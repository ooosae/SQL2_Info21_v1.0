
/*
-------------------------------TODO---------------------------------------
настроить время. важно помнить, что все проверки проходят в один день(ТЗ)
*/

---------------------------------------------------------------------------
------------------------AUXILIARY FUNCTION FOR ADD_P2P_CHECK---------------

DROP FUNCTION IF EXISTS is_started_p2p(nickname varchar, taskname varchar);


CREATE OR REPLACE FUNCTION is_started_p2p(nickname varchar, taskname varchar)
RETURNS BOOLEAN
AS $$
DECLARE
    result_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO result_count
    FROM (SELECT checks.id
    FROM p2p
    JOIN checks ON p2p."Check" = checks.id
    WHERE checks.peer = nickname AND checks.task = taskname AND state = 'Start'
    EXCEPT ALL
    SELECT checks.id
    FROM p2p
    JOIN checks ON p2p."Check" = checks.id
    WHERE checks.peer = nickname AND checks.task = taskname AND state != 'Start'
    ) AS tmp;
    RETURN result_count > 0;
END;
$$ LANGUAGE plpgsql;


-- 1 ----------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE add_p2p_check(
    p_peer_to_check varchar,
    p_checking_peer varchar,
    p_task_name varchar,
    p_p2p_status checkstatus,
    p_timestamp time
)
AS $$
    DECLARE
        current_check integer;

BEGIN
BEGIN
    IF p_p2p_status = 'Start' THEN
        IF is_started_p2p(p_peer_to_check, p_task_name) THEN
            RAISE EXCEPTION 'This P2P check already started';
        END IF;
        SELECT MAX(id) + 1
        INTO current_check
        FROM checks;
        INSERT INTO Checks (id, peer, task, date)
        VALUES (current_check, p_peer_to_check, p_task_name, now());
    ELSE
        IF NOT is_started_p2p(p_peer_to_check, p_task_name) THEN
            RAISE EXCEPTION 'This P2P check already finished';
        END IF;
        SELECT c.id
        INTO current_check
        FROM Checks c
        JOIN P2P p ON p."Check" = c.id
        WHERE p.state = 'Start'
        ORDER BY c.date DESC, p.time DESC  --МОЖЕТ БЫТЬ сортировка по c.date лишняя
        LIMIT 1;
    END IF;
    INSERT INTO P2P (id, "Check", checkingPeer, state, time)
    VALUES ((SELECT MAX(id) + 1 FROM P2P), current_check, p_checking_peer, p_p2p_status, p_timestamp);
EXCEPTION
    WHEN others THEN
        RAISE NOTICE 'Error: %', SQLERRM;
        ROLLBACK;
END;
END;
$$ LANGUAGE plpgsql;


CALL add_p2p_check(
    'opqkaldc',
    'akdlfoqk',
    'C5_s21_decimal',
    'Start',
    CAST(now() AS TIME)
);

CALL add_p2p_check(
    'opqkaldc',
    'akdlfoqk',
    'C5_s21_decimal',
    'Success',
    CAST(now() AS TIME)
);

-- CALL add_p2p_check(
--     'opqkaldc',
--     'akdlfoqk',
--     'C5_s21_decimal',
--     'Failure',
--     CAST(now() AS TIME)
-- );



-- 2------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE add_verter_check(
    p_peer_to_check varchar,
    p_task_name varchar,
    p_verter_status checkstatus,
    p_verter_time time
)
AS $$
    DECLARE
        last_p2p_status checkstatus;
        last_verter_status checkstatus;
        last_check int;
BEGIN
BEGIN
    SELECT MAX(id)
    INTO last_check
    FROM checks c
    WHERE p_peer_to_check = c.peer AND p_task_name = c.task;

    SELECT p.state
    INTO last_p2p_status
    FROM P2P p
    WHERE p."Check" = last_check
    ORDER BY time DESC
    LIMIT 1;

    SELECT v.state
    INTO last_verter_status
    FROM verter v
    WHERE v."Check" = last_check
    ORDER BY time DESC
    LIMIT 1;

    IF last_p2p_status IS NULL THEN
        RAISE EXCEPTION 'Not found P2P!';
    ELSIF last_p2p_status = 'Failure' THEN
        RAISE EXCEPTION 'P2P check is failed!';
    ELSIF last_p2p_status = 'Start' THEN
        RAISE EXCEPTION 'P2P check is not finished!';
    ELSIF last_verter_status IS NULL AND p_verter_status != 'Start' THEN
        RAISE EXCEPTION 'Verter check is not started!';
    ELSIF last_verter_status = 'Start' AND p_verter_status = 'Start' THEN
        RAISE EXCEPTION 'Verter check is already started!';
    ELSIF last_verter_status != 'Start' AND p_verter_status != 'Start' THEN
        RAISE EXCEPTION 'Verter check is already finished!';
    END IF;


    CASE
        WHEN p_verter_status = 'Start' THEN
            IF last_verter_status IS NULL THEN
                INSERT INTO verter (id, "Check", state, time)
                VALUES ((SELECT MAX(id) + 1 FROM verter),
                        last_check,
                        p_verter_status,
                        p_verter_time);
            ELSE
                RAISE EXCEPTION 'Not found success P2P check or verter start already exist';
            END IF;
        WHEN p_verter_status IN ('Success', 'Failure') THEN
            IF last_verter_status = 'Start' THEN
                INSERT INTO verter (id, "Check", state, time)
                VALUES ((SELECT MAX(id) + 1 FROM verter),
                        last_check,
                        p_verter_status,
                        p_verter_time);
            ELSE
                RAISE EXCEPTION 'This check already finished';
            END IF;
    END CASE;
EXCEPTION
    WHEN others THEN
        RAISE NOTICE 'Error: %', SQLERRM;
END;
END;
$$ LANGUAGE plpgsql;


CALL add_verter_check(
     'opqkaldc',
     'C5_s21_decimal',
      CAST('Start' AS checkstatus),
     CAST(now() AS TIME)
);

CALL add_verter_check(
     'opqkaldc',
     'C5_s21_decimal',
      CAST('Success' AS checkstatus),
     CAST(now() AS TIME)
);

-- CALL add_verter_check(
--      'opqkaldc',
--      'C5_s21_decimal',
--       CAST('Failure' AS checkstatus),
--      CAST(now() AS TIME)
-- );

----------------------------------------------- 3 ---------------------------------------------------------

CREATE OR REPLACE FUNCTION trg_p2p_insert_transfer_points()
RETURNS TRIGGER AS $$
    DECLARE
        record_id int;
        checked_peer varchar;
        checking_peer varchar := NEW.checkingpeer;
BEGIN
    IF NEW.state = 'Start' THEN
        SELECT c.peer
        INTO checked_peer
        FROM checks c
        WHERE c.id = NEW."Check";

        SELECT t.id
        INTO record_id
        FROM transferredpoints t
        WHERE checked_peer = t.checkedpeer AND checking_peer = t.checkingpeer;
        IF record_id IS NULL THEN
            record_id := (SELECT MAX(id) + 1 FROM transferredpoints);
            INSERT INTO transferredpoints (id, checkingpeer, checkedpeer, pointsamount)
            VALUES (record_id, checking_peer, checked_peer, 0);
        END IF;
        UPDATE transferredpoints SET pointsamount = pointsamount + 1 WHERE id = record_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_p2p_insert_transfer_points
AFTER INSERT ON P2P
FOR EACH ROW
EXECUTE FUNCTION trg_p2p_insert_transfer_points();

CALL add_p2p_check(
    'opqkaldc',
    'akdlfoqk',
    'C5_s21_decimal',
    'Start',
    CAST(now() AS TIME)
);
SELECT * FROM transferredpoints;
SELECT * FROM p2p;
SELECT * FROM checks;

----------------------------------------------------4     -----------------------------------------------------------

CREATE OR REPLACE FUNCTION trg_before_insert_xp()
RETURNS TRIGGER AS $$
DECLARE
    max_xp int;
    verter_state checkstatus;
BEGIN
BEGIN
    SELECT t.maxxp
    INTO max_xp
    FROM tasks t
    JOIN checks c ON t.title = c.task
    WHERE c.id = NEW."Check";

    SELECT v.state
    INTO verter_state
    FROM verter v
    JOIN checks c ON v."Check" = c.id
    WHERE c.id = NEW."Check" AND v.state != 'Start';

    IF NEW."Check" IN (SELECT "Check" FROM XP) THEN
        RAISE EXCEPTION 'XP for this check already exists!';
    ELSIF NEW.xpamount > max_xp THEN
        RAISE EXCEPTION 'Uncorrect XP!';
    ELSIF verter_state = 'Failure' THEN
        RAISE EXCEPTION 'Verter were failed!';
    END IF;
EXCEPTION
    WHEN others THEN
        RAISE NOTICE 'Error: %', SQLERRM;
        RETURN NULL;
END;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_before_insert_xp
BEFORE INSERT ON XP
FOR EACH ROW
EXECUTE FUNCTION trg_before_insert_xp();

SELECT * FROM checks;
INSERT INTO xp (id, "Check", xpamount)
VALUES ((SELECT MAX(id) + 1 FROM xp),
        13,
        300);

SELECT * FROM xp;
DELETE FROM XP WHERE "Check" = 13;