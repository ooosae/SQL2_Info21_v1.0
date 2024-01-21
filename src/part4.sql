-- 1

CREATE TABLE tmp_table_1
(
    col1 VARCHAR,
    col2 VARCHAR
);

CREATE TABLE tmp_table_2
(
    col1 VARCHAR,
    col2 VARCHAR
);

DROP PROCEDURE IF EXISTS prc_drop_table CASCADE;

CREATE OR REPLACE PROCEDURE prc_drop_table(IN tablename VARCHAR) AS
$$
BEGIN
    FOR tablename IN (SELECT table_name
                      FROM information_schema.tables
                      WHERE table_name LIKE concat(tablename, '%')
                        AND table_schema LIKE 'public')
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || tablename || ' CASCADE';
        END LOOP;
END ;
$$
    LANGUAGE plpgsql;

SELECT * FROM tmp_table_1;
CALL prc_drop_table('tmp_table');
SELECT * FROM tmp_table_1;                  -- Должна быть ошибка тк таблицу удалили!

-- 2

DROP PROCEDURE IF EXISTS prc_get_scalar_functions CASCADE;
CREATE OR REPLACE PROCEDURE prc_get_scalar_functions(
    OUT num_functions INT,
    OUT function_list TEXT
) AS
$$
DECLARE
    func_name text;
    func_params text;
    func record;
BEGIN
    num_functions := 0;
    function_list := '';

    FOR func IN
        SELECT p.proname AS function_name, pg_get_function_arguments(p.oid) AS function_params
        FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'public'
          AND p.proargtypes = ''
        LOOP
            func_name := func.function_name;
            func_params := func.function_params;

            IF func_params != '' THEN
                num_functions := num_functions + 1;
                function_list := function_list || func_name || '(' || func_params || '), ';
            END IF;
        END LOOP;

    function_list := SUBSTRING(function_list, 1, LENGTH(function_list) - 2);
END;
$$
    LANGUAGE plpgsql;

DO
$$
    DECLARE
        num_functions INT;
        function_list TEXT;
    BEGIN
        CALL prc_get_scalar_functions(num_functions, function_list);
        RAISE NOTICE 'Found % scalar functions: %', num_functions, function_list;
    END
$$;

-- 3

DROP PROCEDURE IF EXISTS prc_destroy_all_triggers CASCADE;

CREATE OR REPLACE PROCEDURE prc_destroy_all_triggers(OUT count_destroy_triggers INT) AS
$$
DECLARE
    trg_name   text;
    table_name text;
BEGIN
    SELECT COUNT(DISTINCT trigger_name)
    INTO count_destroy_triggers
    FROM information_schema.triggers
    WHERE trigger_schema = 'public';
    FOR trg_name, table_name IN (SELECT DISTINCT trigger_name, event_object_table
                                 FROM information_schema.triggers
                                 WHERE trigger_schema = 'public')
        LOOP
            EXECUTE concat('DROP TRIGGER ', trg_name, ' ON ', table_name);
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

CALL prc_destroy_all_triggers(NULL);

-- 4

DROP PROCEDURE IF EXISTS prc_search_objects CASCADE;

CREATE OR REPLACE PROCEDURE prc_search_objects(
    IN search_string text,
    IN cursor refcursor default 'cursor') AS
$$
BEGIN
    OPEN cursor FOR
        SELECT routine_name AS object_name,
               routine_type AS object_type
        FROM information_schema.routines
        WHERE specific_schema = 'public'
          AND routine_definition LIKE concat('%', search_string, '%');
END;
$$
    LANGUAGE plpgsql;

BEGIN;
CALL prc_search_objects('Peer');
FETCH ALL IN "cursor";
END;