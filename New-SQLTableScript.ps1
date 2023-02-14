Import-Module dbatools
Import-Module SqlServer


$OutFileDir= "C:\Temp\"
$SrcInstance = "PBG1SQL20L203"



$sql_GetTableList = "
USE [MesLogging];

SELECT	OBJECT_SCHEMA_NAME(TBL.[object_id]) AS SchemaName,
		TBL.[name] AS TableName,
		PTSCH.[name] AS PtScheme,
		PTFUNC.fanout,
		COL.[name] AS PtCol,
		IDXC.partition_ordinal,
		IDXC.column_id
	FROM sys.tables TBL
		INNER JOIN sys.indexes IDX
			ON (TBL.[object_id] = IDX.[object_id])
		INNER JOIN sys.partition_schemes PTSCH
			ON (IDX.data_space_id = PTSCH.data_space_id)
		INNER JOIN sys.partition_functions PTFUNC
			ON (PTSCH.function_id = PTFUNC.function_id)
		INNER JOIN sys.index_columns IDXC
			ON (TBL.[object_id] = IDXC.[object_id])
				AND (IDX.index_id = IDXC.index_id)
		INNER JOIN sys.columns COL
			ON (TBL.[object_id] = COL.[object_id])
				AND (IDXC.column_id = COL.column_id)
	WHERE (OBJECT_SCHEMA_NAME(TBL.[object_id]) = 'Logging')
		AND (TBL.is_ms_shipped = 0)
		AND (IDX.[type] IN (0,1,5))
		AND (IDXC.partition_ordinal > 0)
	ORDER BY SchemaName, TableName
"
$TableList = Invoke-Sqlcmd -ServerInstance $SrcInstance -Database "MesLogging" -Query $sql_GetTableList;

$OutFileName = "$($OutFileDir)CreateLoggingStgTables_$($SrcInstance).sql"

"" | Out-File -LiteralPath $OutFileName -Force

foreach ($tbl in $TableList) {

    #   Source Table
    #
    $srcInstance = $SrcInstance         # Source instance
    $srcDB       = "MesLogging"          # Source database name
    $srcSchema   = "Logging"                    # Source table schema name

    $srcTable    = $tbl.TableName      # Source table name

    $srcKeys     = @("RowID")        # A string array containing the unique columns keys for the table copy fetch
                                            # if blank, use the columns from an existing unique index
                                            # Example: @("Col1", "Col2")

    #   Destination Table
    #
    $destInstance = $srcInstance            # Destination instance
    #$destInstance = "PBG1SQL01V001.fs.local"      # Destination instance
    $destDB       = $srcDB                  # Destination database
    #$destSchema   = $srcSchema             # Destination table schema name
    $destSchema   = "DBA-Stg"               # Destination table schema name
    $destTable    = $srcTable               # Source table name
    #$destTable    = if ($srcSchema -ieq "dbo") {$srcTable} else {"$($srcSchema)$($SrcTable)"};

    $destFG       = ""                                      # Set value if changing the filegroup name
    $destPtScheme = "PtSch_$($SchemaName)_$($TableName)"      # 'NONE', 'UseSource', or specifiy a partition scheme
    $destPtColumn = $tbl.PtScheme                   # The partition column name or blank.  If blank, then use the same column as the source table

    $destIncremental = $true
    $destSamplePct = 100

    #
    #   Calculated Values
    #
    $srcFullTableName = "[$($tbl.SchemaName)].[$($tbl.TableName)]"


    #
    #   Information queried from the instance and database
    #
    $srcDBInfo = Get-DbaDatabase -SqlInstance $srcInstance -Database $srcDB   -Verbose
    $srcDBGuid = $srcDBInfo.DatabaseGuid

    $srcTabInfo = $srcDBInfo.Tables | Where-Object {($_.Schema -ieq $srcSchema) -and ($_.Name -ieq $srcTable)}

    #
    #   Calculate variables based on both specified and queried information
    #
    $IsSameDB   = if ($srcDBGuid -eq $destDBGuid) {$true} else {$false}
    $IsSameTable = $false
    if ($IsSameDB -and ($null -ne $destTabInfo)) {
        $IsSameTable = if ($srcTabInfo.ID -eq $destTabInfo.ID) {$true} else {$false}
    }

    $destON = " ON [$($tbl.PtScheme)]([$($tbl.PtCol)])"
    #$destON

    #   
    #   SMO Processing
    #
    $scriptOpts = New-DbaScriptingOption  # Setup the SMO scfipting options block

    $scriptOpts.AppendToFile = $True
    $scriptOpts.AllowSystemObjects = $False
    $scriptOpts.ClusteredIndexes = $True
    $scriptOpts.DriDefaults = $True
    $scriptOpts.DriAll = $False
    $scriptOpts.DriForeignKeys = $False
    $scriptOpts.DriIndexes = $True
    $scriptOpts.ScriptDrops = $False
    $scriptOpts.IncludeHeaders = $False
    $scriptOpts.Indexes = $True
    $scriptOpts.NoCollation = $True
    $scriptOpts.Permissions = $False
    $scriptOpts.ScriptDataCompression = $True
    $scriptOpts.Statistics = $false # $True
    $scriptOpts.ToFileOnly = $False
    $scriptOpts.Triggers = $false  # $True
    $scriptOpts.WithDependencies = $False
    $scriptOpts.NoIndexPartitioningSchemes = $False
    $scriptOpts.NoTablePartitioningSchemes = $False

    $scrOutput = Export-DbaScript -InputObject $srcTabInfo -ScriptingOptionsObject $scriptOpts -Path $OutFileDir   # -Passthru

    $sOut2 = Get-Content -Path $scrOutput.FullName 
    $srcFullTableNameMask = "\[$($srcSchema)\].\[$($srcTable)\]"  # Add escape character to allow matching brackets

    $destFullTableName = "[$($destSchema)].[$($destTable)]"

    #   Combine the script lists and perform text substitutions
    #
    $sOut3 = $sOut2 + $cStsOut

    $cScript = ""
    $sOut3 | ForEach-Object { 
        $cline = $_
        $cLine = $cLine -replace $srcFullTableNameMask, $destFullTableName
        if ($cline -match " ON (\[(?<PTS>[^]]+?)\])(\((?<PTC>[^)]+?)\))") {
            #$Matches | FT
            $cLine = $cline -replace " ON (\[(?<PTS>[^]]+?)\])(\((?<PTC>[^)]+?)\))", $destON;
            #"PT: $($cline)"
        } elseif ($cline -match " ON (\[([^]]+?)\])(?!\.)") {
            #$Matches | FT
            $cLine = $cline -replace " ON (\[(?<FG>[^]]+?)\])(?!\.)", $destON;
            #"FG: $($cline)"
        } else {
            #"--: $($cline)"
        }

        if ( $flgChangeConstraintName ) {
            $cLine = $cLine -replace "CONSTRAINT \[", "CONSTRAINT [PTC_"
        }
        $cScript = $cScript + $cLine  + "`r`n";

    }

    #$OutTableFilePath = "$($OutFileDir)Create_$($tbl.TableName).sql"

    $cScript | Out-File -FilePath $OutFileName -Append   # Save the modified script output file

    Remove-item -Path $scrOutput.FullName   # Remove the original script text file 
}