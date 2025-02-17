CREATE OR REPLACE PROCEDURE YOUR_DB.YOUR_SCHEMA.CHECK_DATA_QUALITY_PROC()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    --variables with definitions
    tables ARRAY := (SELECT ARRAY_AGG(FULL_TABLE_LINK_NAME) FROM YOUR_DB.YOUR_SCHEMA.DATA_QUALITY_PROVIDED_TBL); -- Add your table names here
    complete_record_sql_array ARRAY := (SELECT ARRAY_AGG(COMPLETE_RECORD_SQL_LIST) FROM YOUR_DB.YOUR_SCHEMA.DATA_QUALITY_PROVIDED_TBL); --This pulls SQL used to define a complete record
    null_column_check_array ARRAY := (SELECT ARRAY_AGG(NULL_COLUMN_CHECK) FROM YOUR_DB.YOUR_SCHEMA.DATA_QUALITY_PROVIDED_TBL); --This pulls SQL identifier column to define a null count
    
    check_date DATE := CURRENT_DATE() - INTERVAL '1 DAY'; -- Define the interval (day, month, week, etc) you want here, this is the date of the data you are checking (this example is checking previous day data)
    check_date_fresh DATE := CURRENT_DATE() - INTERVAL '2 DAY'; -- Defined for Freshness query, modify as needed by regular expected cadence of your data
    date_column STRING := 'DATE_COLUMN_NAME'; -- Modify this to the actual date column name in tables ideally a uniform col name
    primary_row_key STRING := 'PRIMARY_KEY_COLUMN_NAME'; --Modify this to the the actual primary key column that you use to identify a unique row

    --variables results
    ---duplicate variables
    cond_count INT DEFAULT 0;
    duplicate_count_result INT DEFAULT 0;
    duplicate_row_key ARRAY;
    duplicate_row_key_string STRING;
    sql_dup_statement STRING;
    ---null variables
    null_count_result INT DEFAULT 0;
    sql_null_statement STRING;
    ---freshness variables
    fresh_count_result INT DEFAULT 0;
    sql_fresh_statement STRING;
    ---completeness variables
    completeness_result FLOAT DEFAULT 0.00;
    sql_completeness_statement STRING;

    --other variables
    table_name STRING;
    complete_record_sql STRING;
    null_column_check STRING;
    
    
BEGIN

    -- loop through tables in the array
    FOR i IN 0 TO ARRAY_SIZE(tables) - 1 DO

        -- array of tables to loop     
        table_name := tables[i];
        complete_record_sql := complete_record_sql_array[i];
        null_column_check := null_column_check_array[i];

        -- Use this section to build your data quality check statements
        
        -- 1. Check for DUPLICATES with date filtering
        ----- Run duplicate Check
        sql_dup_statement := 'CREATE OR REPLACE TEMPORARY TABLE YOUR_DB.YOUR_SCHEMA.temp_result_table_dups
                              AS WITH dup_check AS (
                              SELECT *, row_number() OVER (PARTITION BY ' || primary_row_key || ' ORDER BY ' || date_column || ') AS rn
                              FROM ' || table_name || ' 
                              WHERE ' || date_column || ' = ''' || check_date || ''' 
                              ),
                              result_rows AS (
                              SELECT COUNT(* EXCLUDE rn) as duplicate_count, ' || primary_row_key || '
                              FROM dup_check
                              WHERE rn > 1
                              GROUP BY ' || primary_row_key || ')
                              SELECT 0 as duplicate_count, ''no result found'' AS primary_row_key 
                              WHERE NOT EXISTS (SELECT * FROM result_rows)
                              UNION ALL
                              SELECT duplicate_count, cast(' || primary_row_key || ' as varchar) AS primary_row_key FROM result_rows';
        
        --Execute dynamic SQL
        EXECUTE IMMEDIATE :sql_dup_statement;

        SELECT sum(duplicate_count) 
        INTO :duplicate_count_result
        FROM YOUR_DB.YOUR_SCHEMA.temp_result_table_dups;

        LET cond_count := duplicate_count_result;
        IF (cond_count > 0) THEN
            SELECT array_agg(primary_row_key)
            INTO :duplicate_row_key
            FROM YOUR_DB.YOUR_SCHEMA.temp_result_table_dups;
        ELSEIF (cond_count = 0) THEN
            SELECT array_agg(primary_row_key)
            INTO :duplicate_row_key
            FROM YOUR_DB.YOUR_SCHEMA.temp_result_table_dups;
        END IF;

        --for return statement testing
        duplicate_row_key_string := ARRAY_TO_STRING(duplicate_row_key, ', ');
    

        -- 2. Check for NULLs with date filtering
        ----- Run Null Check
         sql_null_statement := 'CREATE OR REPLACE TEMPORARY TABLE YOUR_DB.YOUR_SCHEMA.temp_result_table_nulls
                              AS WITH null_check AS (
                              SELECT *
                              FROM ' || table_name || ' 
                              WHERE ' || date_column || ' = ''' || check_date || ''' AND ' || null_column_check || ' IS NULL
                              ),
                              result_rows AS (
                              SELECT COUNT(*) AS null_count
                              FROM null_check
                              )
                              SELECT null_count FROM result_rows';

        --Execute dynamic SQL
        EXECUTE IMMEDIATE :sql_null_statement;

        SELECT sum(null_count) 
        INTO :null_count_result
        FROM YOUR_DB.YOUR_SCHEMA.temp_result_table_nulls;

        -- 3. Check for FRESHNESS with date filtering
        ----- Run Freshness Check
         sql_fresh_statement := 'CREATE OR REPLACE TEMPORARY TABLE YOUR_DB.YOUR_SCHEMA.temp_result_table_fresh
                              AS WITH fresh_check AS (
                              SELECT ' || date_column || ', COUNT(*) AS ROWS_ADDED
                              FROM ' || table_name || ' 
                              WHERE ' || date_column || ' BETWEEN ''' || check_date_fresh || ''' AND ''' || check_date || '''
                              GROUP BY ' || date_column || '
                              ),
                              num_days_updates AS (
                              SELECT ' || date_column || ', ' || date_column || ' - lag(' || date_column || ') OVER(
                                        ORDER BY ' || date_column || '
                                        ) AS DAYS_SINCE_LAST_UPDATE
                              FROM fresh_check
                              )
                              SELECT ' || date_column || ', DAYS_SINCE_LAST_UPDATE FROM num_days_updates
                              WHERE DAYS_SINCE_LAST_UPDATE >= 1';

        --Execute dynamic SQL
        EXECUTE IMMEDIATE :sql_fresh_statement;

        SELECT days_since_last_update 
        INTO :fresh_count_result
        FROM YOUR_DB.YOUR_SCHEMA.temp_result_table_fresh;

        -- 4. Check for COMPLETENESS with date filtering
        ----- Run completeness Check
         sql_completeness_statement := 'CREATE OR REPLACE TEMPORARY TABLE YOUR_DB.YOUR_SCHEMA.temp_result_table_completeness
                              AS WITH select_rows AS (
                              SELECT *
                              FROM ' || table_name || ' 
                              WHERE ' || date_column || ' = ''' || check_date || ''' 
                              ),
                              select_complete_rows AS (
                              SELECT *
                              FROM select_rows
                              WHERE ' || complete_record_sql || '
                            )
                            SELECT 
                                CASE 
                                    WHEN (select count(*) from select_rows) = 0 THEN NULL
                                    ELSE count(*) * 100 / (select count(*) from select_rows) END AS complete_percent 
                            FROM select_complete_rows';

        --Execute dynamic SQL
        EXECUTE IMMEDIATE :sql_completeness_statement;

        SELECT complete_percent 
        INTO :completeness_result
        FROM YOUR_DB.YOUR_SCHEMA.temp_result_table_completeness;

        -- 4. Insert results into the data quality results table
        INSERT INTO YOUR_DB.YOUR_SCHEMA.DATA_QUALITY_RESULTS_TBL (
            TABLE_NAME, 
            CHECK_DATE,
            ROW_KEY,
            DUPLICATE_COUNT,
            NULL_COUNT,
            FRESHNESS_DAYS_SINCE_LAST_UPDATE,
            COMPLETENESS_PCT,
            CHECK_RUN_TIMESTAMP
        )
        SELECT
            :table_name, 
            :check_date, 
            :duplicate_row_key,
            :duplicate_count_result,
            :null_count_result,
            :fresh_count_result,
            :completeness_result,
            CURRENT_TIMESTAMP();

        -- Clear temp tables
        TRUNCATE TABLE YOUR_DB.YOUR_SCHEMA.temp_result_table_dups;
        TRUNCATE TABLE YOUR_DB.YOUR_SCHEMA.temp_result_table_nulls;
        TRUNCATE TABLE YOUR_DB.YOUR_SCHEMA.temp_result_table_fresh;
        TRUNCATE TABLE YOUR_DB.YOUR_SCHEMA.temp_result_table_completeness;
        
    END FOR;

    --drop temp tables
    DROP TABLE IF EXISTS YOUR_DB.YOUR_SCHEMA.temp_result_table_dups;
    DROP TABLE IF EXISTS YOUR_DB.YOUR_SCHEMA.temp_result_table_nulls;
    DROP TABLE IF EXISTS YOUR_DB.YOUR_SCHEMA.temp_result_table_fresh;
    DROP TABLE IF EXISTS YOUR_DB.YOUR_SCHEMA.temp_result_table_completeness;

    --RETURN 'Data Quality Check Completed';
    --OR use below for preview output
    RETURN 'Table: ' || table_name || ', Duplicate Count: ' || duplicate_count_result || ', Row Key: ' || duplicate_row_key_string;
    
END;
$$;
