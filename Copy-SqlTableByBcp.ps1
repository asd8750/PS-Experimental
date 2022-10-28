function Copy-SqlTableByBcp  {
    <#
.SYNOPSIS

Generate a TSQL script to recreate the logins, roles and permissions at the instance level.

.DESCRIPTION

.PARAMETER InstanceName
Specifies the SQL instance to test and modify

.PARAMETER OutputDirectory
Directory path to contain the generated script

.OUTPUTS

PS Object

.EXAMPLE

PS> Generate-LoginsRoles -InstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $SrcInstanceName ,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $SrcDatabaseName ,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $DestInstanceName ,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $DestDatabaseName ,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $ExtractDir,
  
        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [int] $Phase = 0
    )

    Process {
        #
        #   Obtain a list of all user tables already created in the source and destination databases
        #
        $sqlObjList = "
            SELECT	OBJ.[object_id],
                    OBJ.create_date AS ObjCreateDate,
                    OBJ.[type],
                    OBJECT_SCHEMA_NAME(OBJ.[object_id]) AS SchemaName,
                    OBJECT_NAME(OBJ.[object_id]) AS TableName,
                    IDX.[type] AS IndexType,
                    CONCAT('[', OBJECT_SCHEMA_NAME(OBJ.[object_id]), '].[', OBJECT_NAME(OBJ.[object_id]), ']') AS FullTableName,
                    (SELECT SUM(PT.[rows]) FROM sys.partitions PT WHERE (PT.[object_id] = OBJ.[object_id]) AND (PT.index_id = IDX.index_id)) AS [RowCount],
                    ISNULL((SELECT	IC.last_value
                                FROM sys.identity_columns IC
                                WHERE (IC.[object_id] = OBJ.[object_id])), 0) AS IdentityValue,
                    (SELECT MAX(CAST(TC.is_identity AS TINYINT)) 
                                FROM sys.columns TC
                                WHERE (TC.[object_id] = OBJ.[object_id])) AS is_identity
                FROM sys.objects OBJ
                    LEFT OUTER JOIN sys.indexes IDX
                        ON (OBJ.[object_id] = IDX.[object_id])
                WHERE (OBJ.[type] IN ('U')) AND
                    (OBJ.is_ms_shipped = 0)
                    AND (IDX.[type] IN (0,1,5))
        "

        #   Get Source object list
        #
        try {
            $SrcTableObjects = Invoke-Sqlcmd -ServerInstance $SrcInstanceName -Database $SrcDatabaseName -Query $sqlObjList
        }
        catch {
            Write-Host "Cannot connect to Source: $($SrcInstanceName)   Database: $($SrcDatabaseName)"
            exit
        }

        $SrcObjs = @{};  #     Use hash table for source list
        foreach ($sObj in $SrcTableObjects) {
            $FullTableName = $sObj.FullTableName   # Create the full table name
            $SrcObjs[$FullTableName] = [PSCustomObject]@{                   # Collect object info into a single hash object
                ObjectID    = $sObj.object_id
                CreateDate  = $sObj.ObjCreateDate
                IdxType     = $sObj.type
                FullTableName = $sObj.FullTableName
                SchemaName  = $sObj.SchemaName
                TableName   = $sObj.TableName
                IndexName   = $sObj.IndexName
                IndexType   = $sObj.IndexType
                RowCount    = $sObj.RowCount
                Identity    = $sObj.IdentityValue
            }
        }

        #   Get Destination object list
        #
        try {
            $DestTableObjects = Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $sqlObjList
        }
        catch {
            Write-Host "Cannot connect to Destination: $($DestInstanceName)   Database: $($DestDatabaseName)"
            exit
        }

        #   Extract Foreign Key information from the destination DB
        #
        $sqlForeignKeys = "
            ;WITH FKL AS
            (
                SELECT FK.[name] AS [FKConstName],
                    FK.[object_id] AS constraint_object_id,
                    FK.referenced_object_id,
                    FK.parent_object_id,
                    OBJECT_SCHEMA_NAME (FK.referenced_object_id) AS [PK_SchemaName],
                    OBJECT_NAME (FK.referenced_object_id) AS [PK_TableName],
                    OBJECT_SCHEMA_NAME (FK.parent_object_id) AS [FK_SchemaName],
                    OBJECT_NAME (FK.parent_object_id) AS [FK_TableName],
                    ( STUFF (
                            (SELECT ',[' + COLREF.[name] + ']'
                                FROM sys.foreign_key_columns FKC
                                    INNER JOIN sys.columns COLREF
                                        ON ( COLREF.object_id = FKC.referenced_object_id )
                                        AND ( COLREF.column_id = FKC.referenced_column_id )
                                WHERE
                                ( FKC.constraint_object_id = FK.object_id )
                                AND ( FK.referenced_object_id = FKC.referenced_object_id )
                                ORDER BY FKC.constraint_column_id
                            FOR XML PATH ('')
                        ), 1, 1, '') ) AS REFCols,
                        ( STUFF (
                            (SELECT ',[' + COLPR.[name] + ']'
                                FROM sys.foreign_key_columns FKC
                                    INNER JOIN sys.columns COLPR
                                        ON ( COLPR.object_id = FKC.parent_object_id )
                                        AND ( COLPR.column_id = FKC.parent_column_id )
                                WHERE
                                ( FKC.constraint_object_id = FK.object_id )
                                AND ( FK.parent_object_id = FKC.parent_object_id )
                                ORDER BY FKC.constraint_column_id
                            FOR XML PATH ('')
                        ), 1, 1, '') ) AS PRCols
                FROM sys.foreign_keys FK
            )

            SELECT	CONCAT('[',FK.PK_SchemaName,'].[',FK.PK_TableName,']') AS ParentTable,
                    CONCAT ('ALTER TABLE [', OBJECT_SCHEMA_NAME (FK.parent_object_id), '].[', OBJECT_NAME (FK.parent_object_id), '] ',
                                    'DROP CONSTRAINT [', FK.FKConstName, ']') AS ConstDrop,
                    CONCAT ('ALTER TABLE [', OBJECT_SCHEMA_NAME (FK.parent_object_id), '].[', OBJECT_NAME (FK.parent_object_id), '] ', 
                            ' WITH NOCHECK ADD  CONSTRAINT [', FK.FKConstName, '] FOREIGN KEY(',FK.PRCols ,') ',
                            ' REFERENCES [', OBJECT_SCHEMA_NAME (FK.referenced_object_id), '].[', OBJECT_NAME (FK.referenced_object_id), '] (',FK.REFCols,')') AS ConstCreate,
                    CONCAT('ALTER TABLE [', OBJECT_SCHEMA_NAME (FK.parent_object_id), '].[', OBJECT_NAME (FK.parent_object_id), '] ',
                                    'WITH CHECK CHECK CONSTRAINT [', FK.FKConstName, ']') AS ConstCheck,
                FK.*
            FROM FKL FK
            ORDER BY ParentTable
        "
        $SrcFKeys = Invoke-Sqlcmd -ServerInstance $SrcInstanceName -Database $SrcDatabaseName -Query $sqlForeignKeys  
        $DestFKeys = Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $sqlForeignKeys  


        # #############################################################################################################
        #
        #   Phase 1 --- Drop FK Constraints, truncate tables, copy data, and re-create FK constraints
        #
        # #############################################################################################################

        if ($Phase -eq 1) {
            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Phase 1 ----------------------------------------"

            #   Drop all existing foreign key constraints based on this table
            #
            $DestTableFKeys =  $DestFKeys 

            foreach ($tfk IN $DestFKeys) {
                "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  $($tfk.ConstDrop)"
                $result = Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $tfk.ConstDrop
            }

            foreach ($dTab in ($DestTableObjects | Sort-Object CreateDate)) {
                try {
                    $sqlTruncateTable = "TRUNCATE TABLE [$($DestDatabaseName)].$($dTab.FullTableName); "
                    "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  $($sqlTruncateTable)"
                    Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $sqlTruncateTable                
                }
                catch {
                    "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Skipping import"
                    continue;
                }
            }


            #   Spin through each table and perform export/import
            #
            $tCnt = 0
            foreach ($dTab in ($DestTableObjects | Sort-Object CreateDate)) {
                $tCnt++
                "  [$($tCnt) of $($DestTableObjects.Count)]:  $($dTab.FullTableName)"
                if ($null -eq $SrcObjs[$dTab.FullTableName]) {
                    "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) -- Not found in source database "
                    continue;
                }

                $sObj = $SrcObjs[$dTab.FullTableName]
                $FullTableName = $dTab.FullTableName

                #   Test source row count
                #
                # if ($sObj.RowCount -eq 0) { 
                #     "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Skipping Empty Table"
                #     continue;
                # }        
                    
                #if ($sObj.RowCount -lt 10000000) { 
                #    "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Skipping < threshold"           # DEBUG!!!!
                #    continue;
                #}

                #   Export the source table to a datafile
                #
                $diExportDir = Get-Item -LiteralPath $ExtractDir 
                $TName = "$($dTab.SchemaName)-$($dTab.TableName)"
                $ExportFile = "$($ExtractDir)\$($TName).bcp"

                $cmdBcpExport = "bcp `"[$($SrcDatabaseName)].$($dTab.FullTableName)`" OUT `"$($ExportFile)`" -h 'ROWS_PER_BATCH=100000' -T -n -S $($SrcInstanceName)"
                "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  $($cmdBcpExport)"
                cmd.exe /c $cmdBcpExport

                #   Import back to the destination database
                #
                $sqlImportFile = "BULK INSERT [$($DestDatabaseName)].$($FullTableName) FROM '$($ExportFile)' WITH (DATAFILETYPE = 'native', BATCHSIZE=100000, KEEPIDENTITY);"
                "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  $($sqlImportFile)"
                Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $sqlImportFile -QueryTimeout 3600
            }

        }
        


        # #############################################################################################################
        #
        #   Phase 1.3 ---  Recreate all Foreign Key constraints
        #
        # #############################################################################################################

        if (($Phase -eq 1) -or ($Phase -eq 13)) {
            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Phase 1.3  ----------------------------------------"


            #   Recreate all dropped Foreign Keys
            #
            foreach ($tfk IN ($SrcFKeys | Sort ParentTable)) {
                "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  $($tfk.ConstCreate)"
                $result = Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $tfk.ConstCreate
                "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  $($tfk.ConstCheck)"
                $result = Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $tfk.ConstCheck
            }
        }




        # #############################################################################################################
        #
        #   Phase 2 --- Perform row count test between dataases
        #
        # #############################################################################################################

        if ($Phase -eq 2) {
            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Phase 2 ----------------------------------------"

            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Fetching current database table status"
            $SrcTableObjects = (Invoke-Sqlcmd -ServerInstance $SrcInstanceName -Database $SrcDatabaseName -Query $sqlObjList | Sort SchemaName,TableName)

            $DestTabs = @{}
            $DestTableObjects = (Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $sqlObjList | Sort SchemaName,TableName)
            foreach ($dTab in $DestTableObjects) {
                $DestTabs.Add($dTab.FullTableName, $dTab)
            } 

            foreach ($sTab in $SrcTableObjects) {
                $dTab = $DestTabs[$sTab.FullTableName]
                $SrcRC = (Invoke-Sqlcmd -ServerInstance $SrcInstanceName -Database $SrcDatabaseName -Query "SELECT COUNT(*) AS RC FROM $($sTab.FullTableName)").RC

                if ($null -eq $dTab) {
                    "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  RowCount: $($sTab.FullTableName) --  $($SrcRC)  ???  -- Missing"
                }
                else {
                    $DestRC = (Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query "SELECT COUNT(*) AS RC FROM $($sTab.FullTableName)").RC

                    if ($sTab.RowCount -gt 0) {
                        [float]$delta = ([float]$SrcRC - [Math]::Abs([float]$SrcRC - [float]$DestRC)) / [float]$SrcRC   
                    }
                    else {$Delta = 1}

                    if ($SrcRC -eq $DestRC) {
                        "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  RowCount: $($sTab.FullTableName) --  $($SrcRC)  $($DestRC)  "
                    }
                    elseif ($SrcRC -lt $DestRC) {
                        if ($delta -gt 0.1) {
                            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  RowCount: $($sTab.FullTableName) --  $($SrcRC)  $($DestRC)  <<<<<<<<"             
                        }
                        else {
                            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  RowCount: $($sTab.FullTableName) --  $($SrcRC)  $($DestRC)  < < <"
                        }
                    }
                    else {
                        if ($delta -gt 0.1) {
                            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  RowCount: $($sTab.FullTableName) --  $($SrcRC)  $($DestRC)  >>>>>>>>"             
                        }
                        else {
                            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  RowCount: $($sTab.FullTableName) --  $($SrcRC)  $($DestRC)  > > >"
                        }
                    }

                }

            }

            $a = $Phase

        }
        


        # #############################################################################################################
        #
        #   Phase 3 ---  Test current identity seeds and correct if needed
        #
        # #############################################################################################################

        if ($Phase -eq 3) {
            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Phase 3  ----------------------------------------"

            "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Fetching current database table status"
            $SrcTableObjects = (Invoke-Sqlcmd -ServerInstance $SrcInstanceName -Database $SrcDatabaseName -Query $sqlObjList | Sort SchemaName,TableName)

            $DestTabs = @{}
            $DestTableObjects = (Invoke-Sqlcmd -ServerInstance $DestInstanceName -Database $DestDatabaseName -Query $sqlObjList | Sort SchemaName,TableName)
            foreach ($dTab in $DestTableObjects) {
                $DestTabs.Add($dTab.FullTableName, $dTab)
            } 

            foreach ($sTab in $SrcTableObjects) {
                $SrcID = $sTab.IdentityValue
                $dTab = $DestTabs[$sTab.FullTableName]
                if ($null -eq $dTab) {
                    "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Identity: $($sTab.FullTableName) --  $($SrcID)  ???  -- Missing"
                }
                elseif ($sTab.is_identity -eq 0) {
                    "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Identity: $($sTab.FullTableName) --  No Idenity"
                }
                elseif ($sTab.is_identity -ne $dTab.is_identity) {
                    "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Identity: $($sTab.FullTableName) --  Destination should be identity"
                }
                else {
                    $DestID = $dTab.IdentityValue
                    if ($DestID -eq 0) { $DestID = $SrcID }

                    if ($SrcID -eq $DestID) {
                        "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Identity: $($sTab.FullTableName) --  $($DestID)  OK "
                    }
                    elseif ($SrcID -lt $DestID) {
                        "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Identity: $($sTab.FullTableName) --  $($SrcID)  $($DestID)  < < <"
                    }
                    else {
                        "   $((Get-Date -Format "yyyy/MM/dd HH:mm:ss")) --  Identity: $($sTab.FullTableName) --  $($SrcID)  $($DestID)  > > >"
                    }
                }
            }

            $A = $phase
        }
    }
}


if (-not $FSDeploymentIsLoading){
cls
    #Copy-SqlTableByBcp -SrcInstanceName 'PGT3MesSqlOds.fs.local' -SrcDatabaseName 'ModuleAssembly' -DestInstanceName 'PGT3MesSqlOds.fs.local' -DestDatabaseName 'Test_MA' -ExtractDir 'c:\Temp\Extract' -Phase 2
    # Copy-SqlTableByBcp -SrcInstanceName 'PGT3MesSqlOds.fs.local' -SrcDatabaseName 'ModuleAssembly' -DestInstanceName 'PGT3MesSqlOds.fs.local' -DestDatabaseName 'Test_MA' -ExtractDir 'E:\Data_ML\Extract'    
    #Copy-SqlTableByBcp -SrcInstanceName 'PGT3MesSqlOds.fs.local' -SrcDatabaseName 'ProcessData' -DestInstanceName 'PGT3MesSqlOds.fs.local' -DestDatabaseName 'Test_ProcessData' -ExtractDir 'E:\Data_ML\ExtractPD' -Phase 3
    #Copy-SqlTableByBcp -SrcInstanceName 'PGT3MesSqlProd.mfg.fs' -SrcDatabaseName 'ModuleAssembly_Old' -DestInstanceName 'PGT3MesSqlProd.mfg.fs' -DestDatabaseName 'ModuleAssemblyV2' -ExtractDir 'E:\Data_MS\ExtractMA'    
    Copy-SqlTableByBcp -SrcInstanceName 'PGT3MesSqlProcessData.mfg.fs' -SrcDatabaseName 'ProcessData_OLD' -DestInstanceName 'PGT3MesSqlProcessData.mfg.fs' -DestDatabaseName 'ProcessDataV2' -ExtractDir 'E:\DATA_PD3\ExtractPD' -Phase 1

}