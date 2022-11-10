#
#  Get-FSSQLTableCreateScript.ps1
#  Vers: 1.0
#  History:
#  2019-03-05 F.LaForest - Initial version
#
#  This function uses the SQL Management Objects to create the TSQL script needed to recreate 
#  an existing table and indexes, but within a newly specified schema.
#
#  Use the Powershell "." (dot) syntax to import the function
#
#  Example:
#  . {Insert the path holding the function script}\Get-FSSQLTableCreateScript.ps1 -force
#  . E:\SYS\Backfill\Scripts\Get-FSSQLTableCreateScript.ps1 -force
#
# $tblScripts = Get-FSSQLTableCreateScript -InstanceName "KLM1SQL20V104" -DatabaseName "ProcessData" `
#                            -TableSchema "ProcessHistory" -TableName 'ScribeDeadZone' `
#                            -StgTableSchema "DBA-Stg" `
#                            -PtSchemeName "PtSch_NEW" -PtColumnName "MyId"
#
#  -InstanceName   - Specify the fully qualified instance name. (server\instance)
#  -DatabaseName   - The source/destination database  
#  -TableSchema    - Source table schema
#  -TableName      - Source table name
#  -StgTableSchema - Destination table schema name
#  -StgTableName   - Destination table name
#  -PtSchemeName   - Destination table partition scheme (if different from the source table)
#  -PtColumnName   - Destination table column used for partitioning
#                       
#  Output object contains two string properties:
#     Drop   -- Script used to drop the destination table
#     Create -- Script used to create the destination table
#
#

[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null ; # Load SQL Server Management Objects 
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMOExtended") | Out-Null; 

function Get-FSSQLTableCreateScript {
    param(
        [Parameter(Mandatory=$True)][string] $InstanceName,
        [Parameter(Mandatory=$True)][string] $DatabaseName,
        [Parameter(Mandatory=$True)][string] $TableSchema,
        [Parameter(Mandatory=$True)][string] $TableName,
        [Parameter(Mandatory=$True)][string] $StgTableSchema,
        [Parameter(Mandatory=$False)][string] $StgTableName = $TableName,
        [Parameter(Mandatory=$False)][string] $FileGroup,
        [Parameter(Mandatory=$False)][string] $PtSchemeName, 
        [Parameter(Mandatory=$False)][string] $PtColumnName
    )

    try {
        $connStr = "Server=$InstanceName; "
        if ($Database -ne "") {
                $connStr = $connStr + "Database=$DatabaseName; "
        }

        #if ($TrustedConnection) {
            $connStr = $connStr + "Trusted_Connection=True; "
        #}
        #else {
        #    $connStr = $connStr + "Uid=$LoginName; Pwd=$Password; "
        #}
        #$connStr = $connStr + ";Connect Timeout=$Timeout ";
        $Connection = New-Object System.Data.SqlClient.SqlConnection
        $Connection.ConnectionString = $connStr
        $Connection.Open()
        if ($Connection.State -ne 'Open' ) { return; }        
    }
    catch {
        return;
    }


    # Setup flag for controlling name changes
    #
    $flgSameSchema = ( $TableSchema -ieq $StgTableSchema)
    $flgSameTableName = ( $TableName -ieq $StgTableName )
    $flgChangeTableName = ( $flgSameSchema -and $flgSameTableName )
    $flgChangeConstraintName = ( $flgSameSchema )

    # Prepare the destination table name an patterns
    #
    if ( $flgChangeTableName ) {
        $dstTableName = "[$($StgTableSchema)].[$($TableName)]" 
        }
    else {
        $dstTableName = "[$($StgTableSchema)].[$($StgTableName)]"
        }

    $patSrcTableName = "\[$($TableSchema)\]\.\[$($TableName)\]"


    # Script out the drop and create scripts
    #
    $SMOserver = New-Object -TypeName Microsoft.SqlServer.Management.Smo.Server -ArgumentList $connection.DataSource
    $SMOdb = $SMOserver.Databases| Where-Object { $_.Name -ieq $DatabaseName }
    #
    $SMOtbl = $SMOdb.Tables | Where-Object { $_.Schema -ieq $TableSchema -AND $_.Name -ieq $TableName -AND !($_.IsSystemObject) }

    $SMOcols = $SMOtbl.Columns

    $ColNames = $SMOcols | Sort-Object ID | Select-Object -property Name 

    # Script out the source table and indexes
    #
    $SmoScr = $SMOtbl[0]

    # Determine the primary filegroup name
    #
    if ($FileGroup -ieq "") { 
        if (!$SmoScr.IsPartitioned) {
            $FileGroup = $SmoScr.FileGroup
        }
        else {
            $FileGroup = $SmoScr.PhysicalPartitions[$SmoScr.PhysicalPartitions.Count-1].FileGroupName
        }
    }
    $onFG = "[$($FileGroup)]"

    #   Determine the partition scheme of the staging table
    #
    if ($PtSchemeName -ieq "" -OR  $PtSchemeName -ieq "source") { 
        if ($SmoScr.IsPartitioned) {
            $PtSchemeName = $SmoScr.PartitionScheme;
            $FileGroup = $SmoScr.PhysicalPartitions[$SmoScr.PhysicalPartitions.Count-1].FileGroupName
            $PtColumnName = $SmoScr.PartitionSchemeParameters[0].Name # Use existing partiting column
            $onFG = "[$($PtSchemeName)]($($PtColumnName))"
        }
        else {
            $PtSchemeName = ""
        }
    }
    else {
        $onFG = "[$($PtSchemeName)]($($PtColumnName))"
    }

                        
    #Script the Drop commands
    #
    $ScriptDrop = new-object ('Microsoft.SqlServer.Management.Smo.Scripter') ($SMOserver)
    $ScriptDrop.Options.AppendToFile = $False
    $ScriptDrop.Options.AllowSystemObjects = $False
    $ScriptDrop.Options.ClusteredIndexes = $True
    $ScriptDrop.Options.DriAll = $True
    $ScriptDrop.Options.ScriptDrops = $True
    $ScriptDrop.Options.IncludeIfNotExists = $True
    $ScriptDrop.Options.IncludeHeaders = $False
    $ScriptDrop.Options.ToFileOnly = $False
    $ScriptDrop.Options.Indexes = $True
    $ScriptDrop.Options.WithDependencies = $False

    $dScript = ""
    $sOut1 = $ScriptDrop.Script($SmoScr)
    $sOut1 | ForEach-Object {
       $dScript = $dScript + ($_ -replace $patSrcTableName, $dstTableName) + "`n"; 
    }
    #Write-Output $dScript;

    #
    #   Script the table objects
    #
    $scriptrCreate = new-object ('Microsoft.SqlServer.Management.Smo.Scripter') ($SMOserver)
    $scriptrCreate.Options.AppendToFile = $True
    $scriptrCreate.Options.AllowSystemObjects = $False
    $scriptrCreate.Options.ClusteredIndexes = $True
    $scriptrCreate.Options.DriAll = $False
    $scriptrCreate.Options.DriForeignKeys = $False
    $scriptrCreate.Options.DriIndexes = $True
    $scriptrCreate.Options.ScriptDrops = $False
    $scriptrCreate.Options.IncludeHeaders = $False
    $scriptrCreate.Options.ToFileOnly = $False
    $scriptrCreate.Options.NoIdentities = $true
    #$scriptrCreate.Options.NoFileGroup = $true
    $scriptrCreate.Options.Indexes = $True
    $scriptrCreate.Options.Permissions = $True
    $scriptrCreate.Options.WithDependencies = $False
    $scriptrCreate.Options.ScriptDataCompression = $true;
    $scriptrCreate.Options.Statistics = $True
    $scriptrCreate.Options.NoIndexPartitioningSchemes = $False
    $scriptrCreate.Options.NoTablePartitioningSchemes = $False

    #$scriptrCreate.Options.NoIndexPartitioningSchemes = $True
    #$scriptrCreate.Options.NoTablePartitioningSchemes = $True

    $patSrcTableName = "\[$($TableSchema)\]\.\[$($tableName)\]"

    $cScript = ""
    $sOut2 = $scriptrCreate.Script($SmoScr)
    foreach ($cLine in $sOut2) {
     
        $cLine = $cLine -replace "ON \[[^]]+?\](\(.+?\))?(?!\.)", "ON $($onFG)";
        $cLine = $cLine -replace $patSrcTableName, $dstTableName;

        if ( $flgChangeConstraintName ) {
            $cLine = $cLine -replace "CONSTRAINT \[", "CONSTRAINT [PTC_"
        }
        $cScript = $cScript + $cLine  + "`n";

        #    $cScript = $cScript + 
        #    ((($_ -replace "ON \[[^]]+?\](\(.+?\))?(?!\.)", "ON $($fileGroup)") -replace 
        #            $patSrcTableName, $dstTableName) -replace 
        #                " INDEX \[", " INDEX [PTI_") -replace 
        #                    "CONSTRAINT \[", "CONSTRAINT [PTC_") + "`n";
    }

    if ($connection) {
        if ($Connection.State -eq 'Open' ) { 
            $Connection.Close(); 
        }    
        $Connection.Dispose();
    }
    
    # This ends the object scripting loop.
    #
    $OFS = "`r`n"
    $retValue = [PSCustomObject]@{
        DestFullTable = $dstTableName
        Drop    = $dScript
        Create  = $cScript
        dstTable = $dstTableName
        sOut1   = $sOut1 
        sOut2   = $sOut2 
        ColNames = $ColNames
    }

    return $retValue
}



function Install_PVTables {
    param(
        [Parameter(Mandatory=$True)][string] $InstanceName,
        [Parameter(Mandatory=$True)][string] $DatabaseName,
        [Parameter(Mandatory=$True)][string] $TableSchema,
        [Parameter(Mandatory=$True)][string] $TableName,
        [Parameter(Mandatory=$True)][string] $StgTableSchema,
        [Parameter(Mandatory=$False)][string] $StgTableName = $TableName,
        [Parameter(Mandatory=$False)][string] $FileGroup,
        [Parameter(Mandatory=$False)][string] $PtSchemeName, 
        [Parameter(Mandatory=$False)][string] $PtColumnName
    )

    try {

    }
    catch {

    }
    finally {

    }
    #
    #




}









Get-FSSQLTableCreateScript -InstanceName "PBG1SQL01V001.fs.local" -DatabaseName "ReliabilityDB" `
                            -TableSchema "dbo" -TableName 'RCOL_TemperatureMap' `
                            -StgTableSchema "DBA-Stg" `
                            -PtSchemeName "source" -PtColumnName "DateUpdated"