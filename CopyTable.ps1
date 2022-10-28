#  CopyTable.ps1
#
#   Re-create an existing table in a different location and copy the contents to that location.
#
#   This process is accomplished in a series of phases.  Each phase is called separately
#   
#   Update History:
#   2020-04-09 - F. LaForest - Intial version
#

#Import-Module dbatools

#   Directories
#
$outputDirectory = "C:\Temp"

#
#   Debugging statements
#
$BackfillDllPath = "E:\System\Backfill\DLL";
$BackfillLogPath = "E:\Backup\JobLogs"

if ($env:computername -eq "FS-21402") {
    $BackfillDllPath = "C:\Projects\DBBackfill\bin\Debug";
    $BackfillLogPath = "C:\Temp"   
} 
# FS.LOCAL
elseif (($env:computername -eq "PBG1SQL01T011") -or ($env:computername -eq "PBG2SQL01T011")) {
    $BackfillDllPath = "E:\System\Backfill\DLL";
    $BackfillLogPath = "E:\Backup\JobLogs"   
} # 
elseif (($env:computername -eq "PBG1SQL20T001")) {
    $BackfillDllPath = "D:\Backfill\DLL";
    $BackfillLogPath = "D:\Backfill\JobLogs"   
} # 
elseif (($env:computername -eq "PBG1SQL01T104") -or ($env:computername -eq "PBG2SQL01T104")) {
    $BackfillDllPath = "E:\System\Backfill\DLL";
    $BackfillLogPath = "E:\Backup\JobLogs"   
} # 
elseif (($env:computername -eq "EDR1SQL01V700") ) {
    $BackfillDllPath = "E:\Vi700_System\Backfill\DLL";
    $BackfillLogPath = "E:\Vi700_System\JobLogs"   
} 
elseif (($env:computername -eq "PBG1SQL01T101") -or ($env:computername -eq "PBGSQL01T101")) {
    $BackfillDllPath = "E:\SYS\BackFill\DLL";
    $BackfillLogPath = "E:\LOGS\JobLogs"   
} 
elseif ($env:computername -eq "PBG1SQL01T204") {
    $BackfillDllPath = "E:\SYS\BackFill\DLL";
    $BackfillLogPath = "E:\LOGS\JobLogs"   
} # E:\SYS\BackFill\DLL
# MFG
elseif (($env:computername -eq "DMT1SQL20T101") -or  ($env:computername -eq "DMT2SQL20T101")) {
    $BackfillDllPath = "E:\SYS\BackFill\DLL";
    $BackfillLogPath = "E:\BU\BackfillLogs"   
}
elseif (($env:computername -eq "PBG1SQL20T211") -or  ($env:computername -eq "PBG2SQL20T211")) {
    $BackfillDllPath = "E:\System\Backfill\DLL";
    $BackfillLogPath = "E:\Backup\JobLogs"   
}
elseif (($env:computername -eq "KLM1SQL20T101") ) {
    $BackfillDllPath = "E:\SYS\BackFill\DLL";
    $BackfillLogPath = "E:\BU\JobLogs"   
}

#
#   Processing control
#
$Phase = "Script"    # Script, Create, Backfill
#$Phase = "BackfillInitiate"    # Script, Create, Backfill

$BackfillID = "RDB"  # This can be anything, used as part the file naming scheme
   
#$BackfillBatchSize = 104858             # Max number of rows fetched in one iteration.  (If columnstore then set to 1048576)
#$BackfillBatchSize = 262144             # Max number of rows fetched in one iteration.  (If columnstore then set to 1048576)
#$BackfillBatchSize = 524288             # Max number of rows fetched in one iteration.  (If columnstore then set to 1048576)
$BackfillBatchSize = 1048576            # Max number of rows fetched in one iteration.  (If columnstore then set to 1048576)


#   Source Table
#
$srcInstance = "PGT1SQL01V001"         # Source instance
#$srcInstance = "PBG1SQL01V105"          # Source instance
$srcDB       = "ReliabilityDB"          # Source database name
$srcSchema   = "FED"                    # Source table schema name
#$srcSchema   = "DBA-Post"              # Source table schema name
$srcTable    = "ImportedTempCoeffData"      # Source table name

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

$destFG       = ""                      # Set value if changing the filegroup name
$destPtScheme = "PtSch_dbo_FED_TempCoeffReading_V2"                   # 'NONE', 'UseSource', or specifiy a partition scheme
$destPtColumn = "ReadTime"                      # The partition column name or blank.  If blank, then use the same column as the source table

$destIncremental = $true
$destSamplePct = 100

#
#   Calculated Values
#
$srcFullTableName = "[$($srcSchema)].[$($srcTable)]"

#$destTable    = if ($srcSchema -eq "dbo") {$srcTable} else {"$($srcSchema)$($srcTable)"} # Destination table name
$destFullTableName = "[$($destSchema)].[$($destTable)]"

$destScriptFileName = "Create_$destSchema_$destTable.sql"

$flgChangeConstraintName = if (($srcInstance -eq $destInstance) -and ($srcDB -eq $destDB) -and ($srcSchema -eq $destSchema)) {$true} else {$false}


#
#   Information queried from the instance and database
#
$srcDBInfo = Get-DbaDatabase -SqlInstance $srcInstance -Database $srcDB   -Verbose
$srcDBGuid = $srcDBInfo.DatabaseGuid

$srcTabInfo = $srcDBInfo.Tables | Where-Object {($_.Schema -ieq $srcSchema) -and ($_.Name -ieq $srcTable)}

$destDBInfo = Get-DbaDatabase -SqlInstance $destInstance -Database $destDB
$destDBGuid = $destDBInfo.DatabaseGuid

$destTabInfo = $destDBInfo.Tables | Where-Object {($_.Schema -ieq $destSchema) -and ($_.Name -ieq $destTable)} 

#
#   Calculate variables based on both specified and queried information
#
$IsSameDB   = if ($srcDBGuid -eq $destDBGuid) {$true} else {$false}
$IsSameTable = $false
if ($IsSameDB -and ($null -ne $destTabInfo)) {
    $IsSameTable = if ($srcTabInfo.ID -eq $destTabInfo.ID) {$true} else {$false}
}



"Same Database: $($IsSameDB)"
"Same Table   : $($IsSameTable)"


#   ================================================================================================================
#   Determine the destination filegroup and partitioning settings
#
if ($srcTabInfo.IsPartitioned) {
    $srcPtScheme = $srcTabInfo.PartitionScheme
    $srcPtColumn = $srcTabInfo.PartitionSchemeParameters[0].Name
}
else {
    $srcPtScheme = ""
    $srcPtColumn = ""
}

$destON = ""        # Default to no explicit filegroup placement

if ($destPtScheme -ieq "UseSource") {
    if ($srcTabInfo.IsPartitioned) {
        $destPtScheme = $srcPtScheme
        $destPtColumn = $srcPtColumn
        $destON = " ON [$($destPtScheme)]([$destPtColumn])"        
    } 
    else {
        $destFG = if (-not [string]::IsNullOrEmpty($destFG)) {$destFG} else {$srcTabInfo.FileGroup}
        $destON = "[$($destFG)]"
    }
}

elseif (($destPtScheme -ine "NONE") -and (-not [string]::IsNullOrEmpty($destPtScheme))) {
    $destPtColumn = if ([string]::IsNullOrEmpty($destPtColumn)) {$srcPtColumn} else {$destPtColumn}
    $destON = " ON [$($destPtScheme)]([$destPtColumn])"        
} 

else {
    $destPtScheme = ""
    $destPtColumn = ""
    if (-not [string]::IsNullOrEmpty($destFG)) {
        $destON = " ON [$($destFG)]"
    }
} 

#   ================================================================================================================
#   Determine the unique keys of the source table for the backfill row move.  Use the lowest ID unique index
#
if (($null -eq $srcKeys) -or ($srcKeys -eq "") -or ($srcKeys.Count -eq 0)) {
    $srcKeys = ($srcTabInfo.Indexes | Where-Object IsUnique | Sort-Object ID | Select-Object IndexedColumns -First 1 ).IndexedColumns | Foreach-Object { $_.Name }
}



#   ================================================================================================================
#   Below are the coding for each of the processing phases
#


#   ================================================================================================================
#   Phase "Script" -- Script out the table so it can be re-created with a different name
#   ================================================================================================================
#
if (($Phase -ieq "Script") -or ($Phase -ieq "Create")) {

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

    $scrOutput = Export-DbaScript -InputObject $srcTabInfo -ScriptingOptionsObject $scriptOpts -Path $outputDirectory   # -Passthru

    $sOut2 = Get-Content -Path $scrOutput.FullName 
    $srcFullTableNameMask = "\[$($srcSchema)\].\[$($srcTable)\]"  # Add escape character to allow matching brackets

    #   Script out the statistics create commands
    #
    $cStsOut = $srcTabInfo.Statistics | Sort-Object ID | foreach-Object { 
        $stsName = $_.name
        $stsCols = ($_.StatisticColumns | Sort-Object ID | Select-Object Name).Name -join ", "
        if ($stsName -ilike "_WA_Sys_*") { $stsName = "STS_$($srcTabInfo.Name)_$(($stsCols -replace ',','_'))"}
        $thisIndex = ($srcTabInfo.Indexes | Where-Object { $_.Name -ieq $stsName  } )  # Get the index info if this statistic is created by the index itself
        if ($null -eq $thisIndex) {
            $stsCreate = "CREATE STATISTICS [$($stsName)] ON [$($srcTabInfo.Schema)].[$($srcTabInfo.Name)]"
            $stsCreate = $stsCreate + "  (" + $stsCols + ") "
            if ($destIncremental) {
                $stsCreate = $stsCreate + " WITH INCREMENTAL = ON, PERSIST_SAMPLE_PERCENT = ON, SAMPLE $($destSamplePct) PERCENT "
            $stsCreate = $stsCreate + ";"
            }
            $stsCreate    
        } elseif (-not $thisIndex[0].IndexType -ilike "*ColumnStore*") {
            $stsCreate = "UPDATE STATISTICS [$($srcTabInfo.Schema)].[$($srcTabInfo.Name)]([$($stsName)]) "
            if ($destIncremental) {
                $stsCreate = $stsCreate + " WITH INCREMENTAL = ON, PERSIST_SAMPLE_PERCENT = ON, SAMPLE $($destSamplePct) PERCENT "
            $stsCreate = $stsCreate + ";" 
            }           
            $stsCreate    
        } 

    }

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

    $cScript | Out-File -FilePath "$($outputDirectory)\$($destScriptFileName)" -Force -Encoding utf8  # Save the modified script output file

    Remove-item -Path $scrOutput.FullName   # Remove the original script text file 
}



#   ================================================================================================================
#   Phase "Create" -- Drop any exiisting destination table and run the generated script to create a new dstination table
#   ================================================================================================================
#
if (($Phase -ieq "Script") -or ($Phase -ieq "Create")) {


}




#   ================================================================================================================
#   Phase "Backfill" -- Begin copying the rows from the source to the destination table
#   ================================================================================================================
#
if (($Phase -ieq "BackfillInitiate") -or ($Phase -ieq "Backfill")) {

    #
    #  DBBackfill - Backfill template script
    #
    # This script will copy table rows from one table into another table. 
    # Make a copy of this file and save it in your working directory used for backfill scripts under a name identifying  
    # the source table. 
    # 
    #   Set the following variables with the proper information 
    #  -   
    #  (Restart only) after the setup variable section, there is a "restart" section.  
    #     Two variables control the restart point.  Both values are from the job log file.  Look at the 
    #     last batch completed information line. 
    #  -- RestartPartition -- Insert the partition number where the last batch selected rows from.  
    # --  RestartKeys -  Find the key(s) information displayed on the last batch completed in the log.  
    #           (NOTE!!  Until I correct this, add one to the final key column restart value)
    #
    #  History:
    #  2019-03-10 - F. LaForest - Initial version
    #  2019-03-13 - F. LaForest - Correct restart coding.  User no longer accesses FetchKey object directly
    #  2020-04-17 - F. LaForest - Incororated the latest iteration into the this script
    #

    $now = (get-date).ToString("yyyyMMdd-hhmmss")    # Create a date string.  Used for file naming

    $BkfDebugRoot = "$($BackfillLogPath)\Backfill-$($BackfillID)-$($srcSchema)-$($srcTable)"
    $BkfDebugFile = "$($BkfDebugRoot)-$($now).log"

    [System.Reflection.Assembly]::LoadFile("$($BackfillDllPath)\DBBackfill.dll")

    [DBBackfill.BackfillCtl]$bkfl = New-Object DBBackfill.BackfillCtl $BackfillID, 1
    
    $srcKeyList = "{0}" -f ($srcKeys -join ',' )
    
    $bkfl.CommandTimeout = 3600     # Command timeout in seconds
    $FlgSelectByPartition = if ($srcTabInfo.IsPartitioned) {1} else {0};      # Set non-zero if source rows are selected by partition (Leave at 1) 

    #
    $HasCSI = ($srcTabInfo.Indexes | Where-Object { $_.IndexType.ToString() -ilike "*Columnstore*"} | Foreach-Object IndexType).Count -gt 0
    $HasCSI = $true
        
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #
    # Backfill restart information
    #
    $RestartKeys = @();             # Create an empty array for restart keys
    
    $RestartPartition = if ($Phase -ieq "BackfillInitiate") {1} ELSE {1}
    $RestartPartition = 1  # Partition No;
    #$RestartKeys += [Int64] 685515334 # Restart key[0] - [PdrEquipmentStateId];

    #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    #
    #  Setup is complete -- the remaining code will begin the backfill
    #  
    $bkfl.DebugToConsole = $true
    $bkfl.DebugFile = "$($BackfillLogPath)\Backfill-$($srcSchema)-$($srcTable)-$($now).log"
    $bkfl.DebugOutput("Opening instance: $($srcInstance)")
    $bkfl.OpenInstance($srcInstance)
    
    $bkfl.DebugOutput("Opening instance: $($destInstance)")
    $bkfl.OpenInstance($destInstance)
    
    $bkfl.DebugOutput("Open source table")
    [DBBackfill.DatabaseInfo] $diSrcDB = $bkfl.GetInstance($srcInstance).GetDatabase($srcDB)
    [DBBackfill.TableInfo] $srcTbl1 = $diSrcDB.GetTable($srcSchema, $srcTable)
    #$srcTbl1 | FT
    
    $bkfl.DebugOutput("Open destination table")
    [DBBackfill.DatabaseInfo] $diDestDB = $bkfl.GetInstance($destInstance).GetDatabase($destDb)
    [DBBackfill.TableInfo] $dstTbl1 = $diDestDB.GetTable($destSchema, $destTable)
    #$dstTbl1 | FT
    
    $bkfl.DebugOutput("Build FetchKey")
    [DBBackfill.FetchKeyBoundary] $fkb1 = [DBBackfill.FetchKeyHelpers]::CreateFetchKeyComplete($srcTbl1, $srcKeyList)
    
    #
    #  Special Instructions for this backfill
    #
    #$fkb1.IgnoreFetchCol("VersionNumber");   # Do Not copy this NVARCHAR(MAX) field
    #$fkb1.IgnoreFetchCol("AbsorptionCurve_Wavelengths");          # Do Not copy this NVARCHAR(MAX) field
    #$fkb1.IgnoreFetchCol("WeightedAbsorptionCurve_SpectraIntensities");   # Do Not copy this NVARCHAR(MAX) field
    #$fkb1.IgnoreFetchCol("WeightedAbsorptionCurve_Wavelengths");  # Do Not copy this NVARCHAR(MAX) field
    
    #
    # Add row fetch constraints via the AndWHere property
    #
    $fkb1.TableHint = "NOLOCK";
    $fkb1.AndWhere = @"
"@;
    
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #
    # Backfill restart information
    #
    #$restartTime = Get-Date -Date "2015/04/30 15:21:16.000"
    
    $fkb1.RestartPartition = $RestartPartition;
    foreach ($rk in $RestartKeys) {
        $fkb1.AddRestartKey($rk);
    }
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    #
    #$fkb1.FillType = 1;
    #
    $fkb1.FillTypeName = "BulkInsert"
    $fkb1.FlgSelectByPartition = $FlgSelectByPartition; # Set non-zero if source rows are selected by partition (Leave at 1) 
    
    $bkfl.DebugOutput("Start backfill")
    $bkfl.BackfillData($srcTbl1, $dstTbl1, $null, $fkb1, $BackfillBatchSize)
    
}
