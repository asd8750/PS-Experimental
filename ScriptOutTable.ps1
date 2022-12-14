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

if ($null -eq (Get-Module SqlServer)) { Import-Module SqlServer}        # Ensure the SqlServer module is loaded

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


    $srcTableName = "[$($TableSchema)].[$($TableName)]"

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

    #$ColNames = $SMOcols | Sort-Object ID | Select-Object -property Name 

    $ColInfo = $SMOcols | Sort-Object ID | Select-Object -Property Name, ID, Identity, InPrimaryKey, Nullable, DataType

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
    $scriptrCreate.Options.Default = $true
    $scriptrCreate.Options.DriAll = $true
    #$scriptrCreate.Options.DriForeignKeys = $False
    $scriptrCreate.Options.DriIndexes = $True
    $scriptrCreate.Options.ScriptDrops = $False
    $scriptrCreate.Options.IncludeHeaders = $False
    $scriptrCreate.Options.ToFileOnly = $False
    $scriptrCreate.Options.NoIdentities = $true
    #$scriptrCreate.Options.NoFileGroup = $true
    $scriptrCreate.Options.Indexes = $True
    $scriptrCreate.Options.Permissions = $True
    $scriptrCreate.Options.WithDependencies = $False
    $scriptrCreate.Options.SchemaQualify = $true;
    $scriptrCreate.Options.SchemaQualifyForeignKeysReferences = $true 
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
        SrcTableSchema = $TableSchema
        SrcTableName   = $TableName
        SrcFullTable  = $srcTableName
        DestFullTable = $dstTableName
        Drop    = $dScript
        Create  = $cScript
        dstTable = $dstTableName
        sOut1   = $sOut1 
        sOut2   = $sOut2 
        #ColNames = $ColNames
        ColInfo  = $ColInfo
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
        [Parameter(Mandatory=$False)][string] $PtColumnName,
        [Parameter(Mandatory=$False)][int] $PVCycle = 1
    )

    #  ###############################################################################################
    #
    #  Local function to generate PV table with PVCycle
    #
    #  ###############################################################################################
    function local:GenPVTable {
        param(
            [string] $CTScript,
            [string] $TableSchema,
            [string] $TableName,
            [int] $PVCycle,
            [int] $Seq,
            [string] $ConnStr
        )

        $PVSchema = "PV_$($TableSchema)"
        $OutScript = $CTScript -replace '<<SCHEMA>>', $PVSchema
        $PVTableName = "$($TableName)_PV_$($PVCycle)_$($Seq)"
        $OutScript = $Outscript -replace '<<TABLE>>', $PVTableName

        $ObjectID = (Invoke-Sqlcmd -ConnectionString $ConnStr -Query "SELECT ISNULL(OBJECT_ID('[$($PVSchema)].[$($PVTableName)]'), 0) AS ObjectID").ObjectID

        return [PSCustomObject]@{
            PVCreate = $OutScript
            PVSchema = $PVSchema
            PVTable  = $PVTableName
            PVCycle  = $PVCycle
            PVSeq    = $Seq
            ObjectID = $ObjectID
        }
    }

    #   Setup the Output object to hold information on the objects built
    #
    $PVOutput = [PSCustomObject]@{
        Cycle       = $PVCycle
        ViewName    = $null
        ViewScript  = $null
        SeqName     = $null
        SeqScript   = $null
        SeqALter    = $null
        PVTableList = @()
    }
        

    #   Build the table creation script template
    #
    try {
        #
        #   Validate the 
        $tblScripts = Get-FSSQLTableCreateScript -InstanceName $InstanceName -DatabaseName $DatabaseName `
                                                -TableSchema $TableSchema -TableName $TableName `
                                                -StgTableSchema '<<SCHEMA>>' -StgTableName '<<TABLE>>' `
                                                -FileGroup $FileGroup -PtSchemeName $PtSchemeName -PtColumnName $PtColumnName
    }
    catch {
        return $null
    }
        $ConnString = "Server=$($InstanceName);Database=$($DatabaseName);Integrated Security=True"

    #   Insert a Comment seperation at the top of the "create table" script as a visual cue"
    #
    $tblScripts.Create = "-- ################################################ `r`n--  Table: [<<SCHEMA>>].[<<TABLE>>] `r`n-- ################################################ `r`n" + `
                            $tblScripts.Create

    #   TODO: Create Identity ==> Sequence
    #
    $IdentityColInfo = $tblScripts.ColInfo | Where-object Identity 
    if ($IdentityColInfo) {
        # Script out the SEQUENCE build command
        #
        $sqlCreateSequence = "CREATE SEQUENCE [$($TableSchema)].[seq_$($TableName)] AS BIGINT START WITH 1 INCREMENT BY 1 MINVALUE 1; `r`n"
        $PVOutput.SeqScript = $sqlCreateSequence

        #   Each PV table will need the previous IDENTITY column altered to use a DEFAULT value from the new SEQUENCE
        #
        $sqlAddDefaultSequence = "
                ALTER TABLE [<<SCHEMA>>].[<<TABLE>>] ADD  CONSTRAINT [DF_SEQ_$($IdentityColInfo.Name)] DEFAULT NEXT VALUE FOR [$($TableSchema)].[seq_$($TableName)] FOR [$($IdentityColInfo.Name)]`r`n"
        $tblScripts.Create = $tblScripts.Create + $sqlAddDefaultSequence
        $PVOutput.SeqALter = $sqlAddDefaultSequence
    }

    #   TODO: Load the proposed partitioning plan for the table creation phase
    # 
    $sqlPVPlan = "
    SELECT  [ID]
            ,[DatabaseName]
            ,[SchemaName]
            ,[TableName]
            ,[ColumnName]
            ,[PVCycle]
            ,[Seq]
            ,[MinCheckVal]
            ,[MaxCheckVal]
            ,[CheckExpr]
            ,[RepCnt]
            ,[RowCnt]
        FROM [ReliabilityDB].[DBA].[PV_Config]"

    $rsPVPlan = Invoke-SqlCmd -ServerInstance "PBG1SQL01V001.fs.local" -Query $sqlPVPlan

    #$minMax = $rsPVPlan | Select-Object -Property MinValue, MaxValue -first 1
    $minValue = $rsPVPlan | Measure-Object -Property MinCheckVal -Minimum
    $maxValue = $rsPVPlan | Measure-Object -Property MaxCheckVal -Maximum


    
    #   TODO: Create PV Table PRE
    #
    $TablePRE = GenPVTable -CTScript $tblScripts.Create -TableSchema $TableSchema -TableName $TableName -PVCycle $PVCycle -Seq 0 -ConnStr $ConnString
    $PVOutput.PVTableList += $TablePRE

    #   TODO: Create PV Tables 1 - X
    #
    foreach ($pv in ($rsPVPlan | Sort-Object MinValue)) {

    }


    #   TODO: Create PV Table POST
    #
    $TablePOST = GenPVTable -CTScript $tblScripts.Create -TableSchema $TableSchema -TableName $TableName -PVCycle $PVCycle -Seq 9999

    #   TODO: Create PV View (Schemabinding)
    #
    #   TODO: Create synonym to the PV View
    #
    #   TODO: Create PV table Check Constraints and Check Enables
    #

    return $tblScripts
}



# Get-FSSQLTableCreateScript -InstanceName "PBG1SQL01V001.fs.local" -DatabaseName "ReliabilityDB" `
#                             -TableSchema "dbo" -TableName 'RCOL_TemperatureMap' `
#                             -StgTableSchema "DBA-Stg" `
#                             -PtSchemeName "source" -PtColumnName "DateUpdated";

Install_PVTables -InstanceName "PBG1SQL01V001.fs.local" -DatabaseName "ReliabilityDB" `
                            -TableSchema "dbo" -TableName 'RCOL_TemperatureMap' `
                            -StgTableSchema "DBA-Stg" `
                            -PtSchemeName "source" -PtColumnName "DateUpdated";