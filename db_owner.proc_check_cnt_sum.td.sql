REPLACE PROCEDURE DB_OWNER.proc_check_cnt_sum(IN v_DatabaseName VARCHAR(128), IN v_TableName VARCHAR(128), IN v_BEGIN_TIME VARCHAR(8), IN v_END_TIME VARCHAR(8))
DYNAMIC RESULT SETS 1
LMAIN: BEGIN
  DECLARE v_CreateTableStmt VARCHAR(8192);
  DECLARE v_CreateTableField VARCHAR(8192);
  DECLARE v_SelectStmt VARCHAR(8192);
  DECLARE v_SelectField VARCHAR(8192);
  DECLARE v_PPIColumnName VARCHAR(50);
  DECLARE v_PPIColumnType VARCHAR(50);


  CALL DB_OWNER.p_DROP_TABLE('#DEBUG');
  CREATE MULTISET VOLATILE TABLE #DEBUG(
    V  VARCHAR(50),    /* Variable */
    M  VARCHAR(10240), /* Message  */
    DT TIMESTAMP       /* DateTime */
  ) PRIMARY INDEX (V) ON COMMIT PRESERVE ROWS;

  CALL DB_OWNER.p_DROP_TABLE('#RESULT');
  CREATE MULTISET VOLATILE TABLE #RESULT(
    C  VARCHAR(50)
  ) NO PRIMARY INDEX ON COMMIT PRESERVE ROWS;
  DROP TABLE #RESULT;

  SELECT TRIM(TRAILING ',' FROM (XMLAGG(ColumnName||' DECIMAL(38, 4)'||',' ORDER BY ColumnId) (VARCHAR(4096)))) INTO v_CreateTableField
    FROM dbc.ColumnsV c
   WHERE DatabaseName = v_DatabaseName
     AND TABLENAME = v_TableName
     AND ColumnType IN ('D', 'F', 'I', 'I1', 'I2', 'I8')
   GROUP BY TABLENAME;
  IF v_CreateTableField IS NULL THEN
    call syslib.p_Raise_Error_i(88888, v_DatabaseName||'.'||v_TableName||' has no numeric columns');
  END IF;

  SET v_CreateTableStmt = 'CREATE MULTISET VOLATILE TABLE #RESULT (_count_ DECIMAL(38), '||v_CreateTableField||') ON COMMIT PRESERVE ROWS;';
  INSERT INTO #DEBUG VALUES ('v_CreateTableStmt', :v_CreateTableStmt, CURRENT_TIMESTAMP);
  EXECUTE IMMEDIATE v_CreateTableStmt;

  SELECT TRIM(TRAILING ',' FROM (XMLAGG('SUM(CAST(COALESCE('||TRIM(ColumnName)||', 0) as DECIMAL(38, 4))) as sum_'||ColumnName||',' ORDER BY ColumnId) (VARCHAR(4096)))) INTO v_SelectField
    FROM dbc.ColumnsV c
   WHERE DatabaseName = v_DatabaseName
     AND TABLENAME = v_TableName
     AND ColumnType IN ('D', 'F', 'I', 'I1', 'I2', 'I8')
   GROUP BY TABLENAME;

  IF LOWER(v_BEGIN_TIME) = 'all' AND LOWER(v_END_TIME) = 'all' THEN
    SET v_SelectStmt = 'INSERT INTO #RESULT SELECT COUNT(*) as _count_, '||v_SelectField||' FROM '||v_DatabaseName||'.'||v_TableName||';';
  ELSE
    SET v_PPIColumnName = '';
    SET v_PPIColumnType = '';
    SELECT PPIColumnName, PPIColumnType INTO v_PPIColumnName, v_PPIColumnType
      FROM TD_ETL.PPI_Criteria
     WHERE SubjectName = v_DatabaseName AND TableName = v_TableName;
    INSERT INTO #DEBUG VALUES ('v_PPIColumnName', :v_PPIColumnName, CURRENT_TIMESTAMP);
    INSERT INTO #DEBUG VALUES ('v_PPIColumnType', :v_PPIColumnType, CURRENT_TIMESTAMP);

    IF v_PPIColumnType LIKE 'CHAR%' THEN
      SET v_SelectStmt = 'INSERT INTO #RESULT SELECT COUNT(*) as _count_, '||v_SelectField||' FROM '||v_DatabaseName||'.'||v_TableName||' WHERE '||v_PPIColumnName||' BETWEEN '''||v_BEGIN_TIME||''' AND '''||v_END_TIME||''';';
    ELSEIF v_PPIColumnType LIKE 'TIMESTAMP%' THEN
      SET v_SelectStmt = 'INSERT INTO #RESULT SELECT COUNT(*) as _count_, '||v_SelectField||' FROM '||v_DatabaseName||'.'||v_TableName||' WHERE TO_CHAR('||v_PPIColumnName||', ''YYYYMMDD'') BETWEEN '''||v_BEGIN_TIME||''' AND '''||v_END_TIME||''';';
    ELSEIF v_PPIColumnType = 'NUMERIC' THEN
      SET v_SelectStmt = 'INSERT INTO #RESULT SELECT COUNT(*) as _count_, '||v_SelectField||' FROM '||v_DatabaseName||'.'||v_TableName||' WHERE CAST(CAST('||v_PPIColumnName||' AS INT) AS VARCHAR(8)) BETWEEN '''||v_BEGIN_TIME||''' AND '''||v_END_TIME||''';';
    ELSE
      SET v_SelectStmt = 'INSERT INTO #RESULT SELECT COUNT(*) as _count_, '||v_SelectField||' FROM '||v_DatabaseName||'.'||v_TableName||';';
    END IF;
  END IF;
  INSERT INTO #DEBUG VALUES ('v_SelectStmt', :v_SelectStmt, CURRENT_TIMESTAMP);
  EXECUTE IMMEDIATE v_SelectStmt;


LMAIN_RTS: BEGIN
  DECLARE C1 CURSOR WITH RETURN ONLY TO CALLER FOR
   SELECT * FROM #RESULT;

  OPEN C1;
END LMAIN_RTS;
END LMAIN;