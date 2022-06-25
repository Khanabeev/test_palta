CREATE TABLE source_table
(
    id          INTEGER,  -- первичный ключ
    column_1    VARCHAR(128),
    column_2    VARCHAR(128),
    column_3    VARCHAR(128),
    column_4    VARCHAR(128),
    t_insert_ts TIMESTAMP -- метка времени вставки записи
);

CREATE TABLE target_table
(
    id                  INTEGER,   -- первичный ключ
    column_1            VARCHAR(128),
    column_2            VARCHAR(128),
    column_3            VARCHAR(128),
    column_4            VARCHAR(128),
    t_effective_from_ts TIMESTAMP, -- начало действия версии
    t_effective_to_ts   DATETIME   -- окончание действия версии
);

INSERT INTO source_table
VALUES (1, 'A', 'A', NULL, NULL, '2022-06-01 12:01:10');
INSERT INTO source_table
VALUES (1, 'B', 'None', 'B', NULL, '2022-06-01 12:01:25');
INSERT INTO source_table
VALUES (1, NULL, NULL, 'C', 'C', '2022-06-01 12:01:35');
INSERT INTO source_table
VALUES (2, NULL, NULL, NULL, NULL, '2022-06-01 12:01:15');
INSERT INTO source_table
VALUES (3, 'A', 'A', 'A', 'A', '2022-06-01 12:01:20');
INSERT INTO source_table
VALUES (3, NULL, NULL, NULL, NULL, '2022-06-01 12:01:32');
INSERT INTO source_table
VALUES (3, 'B', NULL, 'B', NULL, '2022-06-01 12:01:40');
INSERT INTO source_table
VALUES (4, 'A', 'A', 'A', 'A', '2022-06-01 12:01:16');
INSERT INTO source_table
VALUES (4, 'None', 'None', 'None', 'None', '2022-06-01 12:01:33');
INSERT INTO source_table
VALUES (5, NULL, NULL, NULL, NULL, '2022-06-01 12:01:12');
INSERT INTO source_table
VALUES (5, 'A', NULL, NULL, NULL, '2022-06-01 12:01:17');
INSERT INTO source_table
VALUES (5, NULL, 'B', NULL, NULL, '2022-06-01 12:01:22');
INSERT INTO source_table
VALUES (5, NULL, NULL, 'C', NULL, '2022-06-01 12:01:27');
INSERT INTO source_table
VALUES (5, NULL, NULL, NULL, 'D', '2022-06-01 12:01:32');


INSERT INTO target_table
SELECT id,
       CASE
           WHEN t1.column_1 IS NOT NULL AND t1.column_1 != 'None' THEN t1.column_1
           WHEN t1.column_1 IS NULL AND t1.column_1_lag IS NOT NULL AND t1.column_1_lag != 'None' THEN t1.column_1_lag
           WHEN t1.column_1 IS NULL AND t1.column_1_lag = 'None' THEN NULL
           WHEN t1.column_1 = 'None' THEN NULL
           END                                                                                                as column_1,
       CASE
           WHEN t1.column_2 IS NOT NULL AND t1.column_2 != 'None' THEN t1.column_2
           WHEN t1.column_2 IS NULL AND t1.column_2_lag IS NOT NULL AND t1.column_2_lag != 'None' THEN t1.column_2_lag
           WHEN t1.column_2 IS NULL AND t1.column_2_lag = 'None' THEN NULL
           WHEN t1.column_2 = 'None' THEN NULL
           END                                                                                                as column_2,
       CASE
           WHEN t1.column_3 IS NOT NULL AND t1.column_3 != 'None' THEN t1.column_3
           WHEN t1.column_3 IS NULL AND t1.column_3_lag IS NOT NULL AND t1.column_3_lag != 'None' THEN t1.column_3_lag
           WHEN t1.column_3 IS NULL AND t1.column_3_lag = 'None' THEN NULL
           WHEN t1.column_3 = 'None' THEN NULL
           END                                                                                                as column_3,
       CASE
           WHEN t1.column_4 IS NOT NULL AND t1.column_4 != 'None' THEN t1.column_4
           WHEN t1.column_4 IS NULL AND t1.column_4_lag IS NOT NULL AND t1.column_4_lag != 'None' THEN t1.column_4_lag
           WHEN t1.column_4 IS NULL AND t1.column_4_lag = 'None' THEN NULL
           WHEN t1.column_4 = 'None' THEN NULL
           END                                                                                                as column_4,
       T_EFFECTIVE_FROM_TS,
       IF(T_EFFECTIVE_TO_TS IS NULL, '2100-01-01 00:00:00',
          DATE_SUB(T_EFFECTIVE_FROM_TS, INTERVAL 1 SECOND))                                                   as T_EFFECTIVE_TO_TS
FROM (
         SELECT id,
                column_1,
                column_2,
                column_3,
                column_4,
                t_insert_ts                                                   as T_EFFECTIVE_FROM_TS,
                lead(t_insert_ts) over (partition by id order by t_insert_ts) as T_EFFECTIVE_TO_TS,
                lag(column_1) over (partition by id order by t_insert_ts)     as column_1_lag,
                lag(column_2) over (partition by id order by t_insert_ts)     as column_2_lag,
                lag(column_3) over (partition by id order by t_insert_ts)     as column_3_lag,
                lag(column_4) over (partition by id order by t_insert_ts)     as column_4_lag
         FROM source_table
     ) t1

