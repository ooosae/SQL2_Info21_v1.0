BEGIN;
CREATE TABLE Peers (
    Nickname varchar PRIMARY KEY NOT NULL,
    Birthday date NOT NULL
);

CREATE TABLE Tasks (
    Title varchar PRIMARY KEY NOT NULL,
    ParentTask varchar DEFAULT NULL,
    MaxXP integer NOT NULL,
    CONSTRAINT fk_ParentTask FOREIGN KEY (ParentTask) REFERENCES Tasks(Title)
);

--CREATE TYPE CheckStatus AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE Friends (
    ID int  PRIMARY KEY,
    Peer1 varchar NOT NULL,
    Peer2 varchar NOT NULL,
    CONSTRAINT fk_Peer1 FOREIGN KEY (Peer1) REFERENCES Peers(Nickname),
    CONSTRAINT fk_Peer2 FOREIGN KEY (Peer2) REFERENCES Peers(Nickname)
);

CREATE TABLE Recommendations (
    ID int  PRIMARY KEY,
    Peer varchar NOT NULL,
    RecommendedPeer varchar NOT NULL,
    CONSTRAINT fk_Peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    CONSTRAINT fk_RecommendedPeer FOREIGN KEY (RecommendedPeer) REFERENCES Peers(Nickname)
);

CREATE TABLE TimeTracking (
    ID int  PRIMARY KEY,
    Peer varchar NOT NULL,
    Date date NOT NULL DEFAULT NOW(),
    Time time NOT NULL DEFAULT NOW(),
    State char(1) NOT NULL,
    CONSTRAINT fk_Peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    CONSTRAINT ch_State CHECK(State IN('1', '2'))
);

CREATE TABLE TransferredPoints (
    ID int  PRIMARY KEY,
    CheckingPeer varchar NOT NULL,
    CheckedPeer varchar NOT NULL,
    PointsAmount int NOT NULL,
    CONSTRAINT fk_CheckingPeer FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname),
    CONSTRAINT fk_CheckedPeer FOREIGN KEY (CheckedPeer) REFERENCES Peers(Nickname),
    CONSTRAINT ch_PointsAmount CHECK(PointsAmount >= 0)
);

CREATE TABLE Checks (
    ID int  PRIMARY KEY,
    Peer varchar NOT NULL,
    Task varchar NOT NULL,
    Date date NOT NULL,
    CONSTRAINT fk_Peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    CONSTRAINT fk_Task FOREIGN KEY (Task) REFERENCES Tasks(Title)
);

CREATE TABLE P2P (
    ID int  PRIMARY KEY,
    "Check" int NOT NULL,
    CheckingPeer varchar NOT NULL,
    State CheckStatus NOT NULL,
    Time time NOT NULL,
    CONSTRAINT fk_Check FOREIGN KEY ("Check") REFERENCES Checks(ID),
    CONSTRAINT fk_CheckingPeer FOREIGN KEY  (CheckingPeer) REFERENCES Peers(Nickname)
);

CREATE TABLE Verter (
    ID int  PRIMARY KEY,
    "Check" int NOT NULL,
    State CheckStatus NOT NULL,
    Time time NOT NULL,
    CONSTRAINT fk_Check FOREIGN KEY ("Check") REFERENCES Checks(ID)
);

CREATE TABLE XP (
    ID int  PRIMARY KEY,
    "Check" int NOT NULL,
    XPAmount int NOT NULL,
    CONSTRAINT fk_Check FOREIGN KEY ("Check") REFERENCES Checks(ID)
);


CREATE OR REPLACE PROCEDURE export(tablename varchar, path text, separator char) AS $$
    BEGIN
        EXECUTE format('COPY %s TO ''%s'' DELIMITER ''%s'' CSV HEADER;',
            tablename, path, separator);
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE import(tablename varchar, path text, separator char) AS $$
BEGIN
    BEGIN
        EXECUTE format('COPY %I FROM %L DELIMITER %L CSV HEADER;', tablename, path, separator);
    EXCEPTION
        WHEN others THEN
            RAISE EXCEPTION 'Error importing data into table % from file %: %', tablename, path, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;


--  /Users/jarlygri/Documents/SQL/SQL2_Info21_v1.0-1/src/data/
-- C:\Users\theca\OneDrive\Рабочий стол\21 school\SQL\Info\src\data\
-- /Users/maganand/Desktop/SQL/Info/src/data/

DO $$
    DECLARE
        exportPath varchar := 'C:\Users\theca\OneDrive\Рабочий стол\21 school\SQL\Info\src\data\taskData\';
        importPath varchar := 'C:\Users\theca\OneDrive\Рабочий стол\21 school\SQL\Info\src\data\taskData\';
        tablesArray varchar[] := ARRAY['peers', 'tasks','checks',  'friends', 'p2p', 'recommendations', 'timetracking',
                                      'transferredpoints', 'verter', 'xp'];
    BEGIN
        FOR i IN ARRAY_LOWER(tablesArray, 1)..ARRAY_UPPER(tablesArray, 1) LOOP
            CALL import(tablesArray[i], importPath || tablesArray[i] || '.csv', ';');
        END LOOP;
    END;
$$;
END;


ROLLBACK;
SELECT * FROM Peers;
SELECT * FROM Tasks;
SELECT * FROM Checks;
SELECT * FROM Friends;
SELECT * FROM P2P;
SELECT * FROM Recommendations;
SELECT * FROM TimeTracking;
SELECT * FROM TransferredPoints;
SELECT * FROM Verter;
SELECT * FROM XP;
