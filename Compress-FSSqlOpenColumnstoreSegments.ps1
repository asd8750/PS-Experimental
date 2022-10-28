#Import-Module dbatools
#Import-Module sqlserver

Get-DbaDatabase -SqlInstance 'AZR1SQL01T904' | Where-Object {($_.Name -iLike "Performance*") -or ($_.Name -ieq "GlobalFED")} | Sort-Object Name | Select-Object  {

    $_.Name
    $sql_OpenSegments = @"
		USE [$($_.Name)];
		
		DECLARE @DatabaseName VARCHAR(256) = DB_NAME()
        DECLARE	@MinimumRows	INT = 5000	-- Mimimum number of rows in a rowgroup to trigger a forced reorg
        DECLARE @debug INT = 0  -- Set non-zero to debug (no execute)

        IF (OBJECT_ID('tempdb..#PT') IS NOT NULL)
	        DROP TABLE #PT;

        SELECT	DISTINCT
		        CONCAT( QUOTENAME(OBJECT_SCHEMA_NAME(RG.[object_id]), '['), '.',
				        QUOTENAME(OBJECT_NAME(RG.[object_id]), '['), '.',
				        QUOTENAME(SIDX.[name], '[')) AS ObjectName,
		        OBJECT_SCHEMA_NAME(RG.[object_id]) AS SchemaName,
		        OBJECT_NAME(RG.[object_id]) AS TableName,
		        SIDX.[name] AS IndexName,
		        SFNC.[name] AS PtFunc,
		        --SIDXC.column_id AS ColOrd,
		        --(SELECT [name] FROM sys.types STY WHERE (STY.system_type_id = COL.system_type_id) ) AS PtColData,
		        RG.partition_number,
				SUM(RG.[total_rows]) AS OpenRows,
		        --RG.total_rows ,

		         CONCAT('ALTER INDEX ',
				        QUOTENAME(SIDX.[name],'['),
				        ' ON ',
				        QUOTENAME(OBJECT_SCHEMA_NAME(RG.[object_id]),'['),
				        '.',
				        QUOTENAME(OBJECT_NAME(RG.[object_id]),'['),
				        ' REORGANIZE PARTITION = ',
						CASE WHEN SFNC.fanout > 1 THEN RIGHT('     ' + CONVERT(VARCHAR(6), RG.partition_number), 6) ELSE 'ALL' END,
				        ' WITH ( COMPRESS_ALL_ROW_GROUPS = ON)',
				        CHAR(13) + CHAR(10),
				        --'GO',
				        --CHAR(13) + CHAR(10), 
				        '',CHAR(13) + CHAR(10)) AS RebuildStmt
	        INTO  #PT
	        FROM sys.column_store_row_groups RG
		        INNER JOIN sys.indexes SIDX
			        ON (RG.[object_id] = SIDX.[object_id]) AND (RG.index_id = SIDX.index_id)
		        LEFT OUTER JOIN 
			        (sys.index_columns SIDXC 
				        INNER JOIN sys.columns COL
					        ON (COL.object_id = SIDXC.object_id) AND (COL.column_id = SIDXC.column_id)
				        INNER JOIN sys.types STY 
					        ON (STY.system_type_id = COL.system_type_id))
				        ON (SIDXC.object_id = SIDX.object_id) AND (SIDXC.index_id = SIDX.index_id) AND (SIDXC.partition_ordinal <> 0)
		        LEFT OUTER JOIN sys.partitions SPAR  
			        ON (SIDX.object_id = SPAR.object_id) AND (SIDX.index_id = SPAR.index_id) AND (RG.partition_number = SPAR.partition_number)
		        LEFT OUTER JOIN sys.partition_schemes SSCH  
			        ON (SIDX.data_space_id = SSCH.data_space_id)
		        LEFT OUTER JOIN sys.partition_functions SFNC  
			        ON (SSCH.function_id = SFNC.function_id)
	        WHERE (RG.state_description = 'OPEN')
		        AND (STY.[name] LIKE '%DATE%')
		        AND (RG.[total_rows] > @MinimumRows)
		        AND (SIDX.[type] IN (5,6))
			GROUP BY RG.[object_id], SIDX.[name], SFNC.[name], SFNC.fanout, RG.partition_number
			HAVING (SUM(RG.[total_rows]) >= @MinimumRows)
			ORDER BY ObjectName, RG.partition_number	        ;

	        --SELECT * FROM  #PT;
	        DECLARE @PtSelect VARCHAR(MAX);

	        WITH PTL AS (
	        SELECT DISTINCT PtFunc
		        FROM #PT
		        )
	        SELECT @PtSelect = STUFF((SELECT 'UNION ALL SELECT ' + QUOTENAME(PtFunc, '''') + ' AS PtFunc, ' + 
						        '`$PARTITION.' + QUOTENAME(PtFunc, '[') + '(GETDATE()) AS CurPtNum ' 
			        FROM PTL
			        FOR XML PATH('')), 1,10, '');

            --PRINT @PtSelect;

	        DECLARE @CurPtList TABLE ( PtFunc VARCHAR(256), CurPtNum INT);
	        INSERT INTO @CurPtList (PtFunc, CurPtNum)
	        EXEC (@PtSelect);

	        --SELECT * FROM @CurPtList;

	
        DECLARE @curDB CURSOR;
        SET @curDB = CURSOR FORWARD_ONLY FOR 
	        SELECT	PT.ObjectName, PT.RebuildStmt
		        FROM #PT PT
			        INNER JOIN @CurPtList CPT
				        ON (PT.PtFunc = CPT.PtFunc)
		        WHERE (PT.partition_number <> CPT.CurPtNum)
		        --AND (PT.RebuildStmt LIKE '%DBA-Stg%')
		        ORDER BY ObjectName;

        OPEN @curDB;
		
        IF (OBJECT_ID('tempdb..#CMD') IS NOT NULL)
			DROP TABLE [#CMD];
		CREATE TABLE [#CMD] (
			ID	INT NOT NULL IDENTITY(1,1),
			DatabaseName  NVARCHAR(256)  NOT NULL,
			TSql	NVARCHAR(2000) NOT NULL
		);

        DECLARE @ErrMsg  VARCHAR(2000);

        DECLARE	@ObjectName VARCHAR(512), @ReorgCmd	VARCHAR(2000);
        FETCH NEXT FROM @curDB INTO @ObjectName, @ReorgCmd;
        WHILE (@@FETCH_STATUS = 0)
	        BEGIN
		        --RAISERROR ('Reorg: %s', 0, 1, @ReorgCmd) WITH NOWAIT;
		        BEGIN TRY
			        IF (@debug = 0)
				        BEGIN
						--EXEC (@ReorgCmd);
						INSERT INTO [#CMD] (DatabaseName, TSql)
							VALUES (@DatabaseName, @ReorgCmd);
				        --WAITFOR DELAY '00:00:05'
				        END;
		        END TRY
		        BEGIN CATCH
			        SELECT @ErrMsg = ERROR_MESSAGE();
			        --RAISERROR ('  Error: %s', 0, 1, @ErrMsg) WITH NOWAIT;
		        END CATCH;
		        FETCH NEXT FROM @curDB INTO @ObjectName, @ReorgCmd;
	        END;

        CLOSE @curDB;
        DEALLOCATE @curDB;

		DROP TABLE #PT;
		
		SELECT [ID], DatabaseName, [TSql]
			FROM [#CMD]
			ORDER BY [ID];
"@

    # $sql_OpenSegments
 #   $sqlCmd = Invoke-Sqlcmd -Query $sql_OpenSegments  -ServerInstance 'AZR1SQL01T904.fs.local' # -OutputSqlErrors # -Verbose 2>&1
 #   $sqlCmd | Format-Table
 }
