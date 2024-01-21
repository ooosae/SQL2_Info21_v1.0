-- DO $$
--     DECLARE
--         namesArray varchar[] := ARRAY['Peers', 'Tasks', 'Checks', 'Friends', 'P2P', 'Recommendations', 'TimeTracking',
--                                       'TransferredPoints', 'Verter', 'XP'];
--     BEGIN
--         FOR i IN ARRAY_LOWER(namesArray, 1)..ARRAY_UPPER(namesArray, 1) LOOP
--             EXECUTE 'TRUNCATE TABLE ' || namesArray[i] || ' CASCADE';
--         END LOOP;
--     END;
-- $$;

DO $$
    DECLARE
        namesArray varchar[] := ARRAY['Peers', 'Tasks', 'Checks', 'Friends', 'P2P', 'Recommendations', 'TimeTracking',
                                      'TransferredPoints', 'Verter', 'XP'];
    BEGIN
        FOR i IN ARRAY_LOWER(namesArray, 1)..ARRAY_UPPER(namesArray, 1) LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || namesArray[i] || ' CASCADE';
        END LOOP;
    --DROP TYPE IF EXISTS checkstatus;
    END;
$$;

-- DO $$
-- DECLARE
--     tabname text;
-- BEGIN
--     FOR tabname IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
--     LOOP
--         EXECUTE 'DROP TABLE IF EXISTS public.' || tabname || ' CASCADE';
--         RAISE NOTICE '%', tabname;
--     END LOOP;
-- END $$;


