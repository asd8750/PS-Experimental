Import-Module dbatools
    
#
$outputDirectory = "C:\Temp"

#   Source Table
#
$srcInstance = "PBG1SQL01L305"         # Source instance
#$srcInstance = "PBG1SQL01V105"          # Source instance
$srcDB       = "MesLogging"          # Source database name
$srcSchema   = "Logging"                    # Source table schema name
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

$destFG       = ""                                      # Set value if changing the filegroup name
$destPtScheme = "PtSch_$($srcSchema)_$($srcTable)"      # 'NONE', 'UseSource', or specifiy a partition scheme
$destPtColumn = "LastModifiedTimeUtc"                   # The partition column name or blank.  If blank, then use the same column as the source table

$destIncremental = $true
$destSamplePct = 100

#
#   Calculated Values
#
$srcFullTableName = "[$($srcSchema)].[$($srcTable)]"


#
#   Information queried from the instance and database
#
$srcDBInfo = Get-DbaDatabase -SqlInstance $srcInstance -Database $srcDB   -Verbose
$srcDBGuid = $srcDBInfo.DatabaseGuid

$srcTabInfo = $srcDBInfo.Tables | Where-Object {($_.Schema -ieq $srcSchema) -and ($_.Name -ieq $srcTable)}

# $destDBInfo = Get-DbaDatabase -SqlInstance $destInstance -Database $destDB
# $destDBGuid = $destDBInfo.DatabaseGuid

# $destTabInfo = $destDBInfo.Tables | Where-Object {($_.Schema -ieq $destSchema) -and ($_.Name -ieq $destTable)} 




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

$scrOutput = Export-DbaScript -InputObject $srcTabInfo -ScriptingOptionsObject $scriptOpts -Path $outputDirectory   # -Passthru

$sOut2 = Get-Content -Path $scrOutput.FullName 
$srcFullTableNameMask = "\[$($srcSchema)\].\[$($srcTable)\]"  # Add escape character to allow matching brackets


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
