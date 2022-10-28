
#
#   Get Backup Volume Stats .ps1
#
#   Purpose:  Scan a server for SQL Server instances, get volume capacity info and log backup data volumes.
#
#   Author:  F. LaForest
#   History:
#   - 2019-05-16 - v1.00 - Initial version
#
#

Import-Module SqlServer

. $PSScriptRoot\Out-DataTable.ps1

#
#   Repository info
#
$RepoInstance = "PBG1SQL01S536.fs.local\DBA_130_S536"
$RepoDatabase = "Imported"
#$RepoInstance = "(local)\S6"
#$RepoDatabase = "DBInfo"
$RepoSchema = "dbo"
$RepoTableBackupVolume = "LogBackupVolume"
$RepoTableVolumes = "VolInfo"


#   Load the list of servers to scan
$serverList = @();      # Create an empty array
$serverList += New-Object PSObject -Property @{
    name = $env:computername
    Domain = $env:userdnsdomain
}            

#
#   Get the list of servers
#
#$ImportedList = Import-Csv -Path C:\Temp\Servers-MFG.csv #)   #) | Select-Object -Property name
#$ImportedList = Import-Csv -Path C:\Temp\Servers.csv #)   #) | Select-Object -Property name

#$serverList = Select-Object -InputObject $ImportedList -ExpandProperty name { $_.Domain -ieq 'fs.local'}  

#$serverList = $ImportedList  | Where-Object -property Domain -ieq 'fs.local' | Where-Object -property name -imatch 'pbg\dsql01' 
#$serverList = $ImportedList  | Where-Object -property Domain -ieq 'fs.local' | Where-Object -property name -imatch 'pbg\dsql01v401' 

#   Loop through server list
#
foreach ($sObj in $serverList ) {

    $DomainName = $sObj.Domain
    if ($sObj.name -imatch "\\") {
        $serverNameDns = ($sObj.name -replace "\\.*$", "") + "." + $sObj.Domain
        $sInst = $sObj.name -replace "^.+?\\", ""
        $sInst = $sInst -replace "MSSQLSERVER", ""
    }
    else {
        $serverNameDns = $sObj.name + '.' + $sObj.Domain
        $sInst = ""
    }
    $FullInstanceName = $serverNameDns
    if ($sInst) { $FullInstanceName = "$($FullInstanceName)\$($sInst)" }

    $instances = @();
    if (Test-Connection $serverNameDns -quiet 6> $null )
    {
        # 
        #   Get the list of possible SQL Server Instances
        #
        try {
            $instList = New-Object 'Collections.Generic.List[Tuple[string,string,string]]'
            $RmtReg = [Microsoft.Win32.RegistryKey]::OpenRemoteBaseKey('LocalMachine', $serverNameDns)
            [Microsoft.Win32.RegistryKey] $RegInstKey = $RmtReg.OpenSubKey("SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL")
            $regSqlLabels = $RegInstKey.GetValueNames()
            foreach ($regSqlLabel in $regSqlLabels) {
                $regSqlSubKey = $RegInstKey.GetValue($regSqlLabel)
                
                # Determine the SQL Data engine and SQL Agent service names
                if ($regSqlLabel -eq "MSSQLSERVER") {
                    $svcSqlEngine = "MSSQLSERVER"
                    $svcSqlAgent = "SQLSERVERAGENT"
                }
                else {
                    $svcSqlEngine = 'MSSQL$' + $regSqlLabel
                    $svcSqlAgent = 'SQLAgent$' + $regSqlLabel                   
                }

                # Get the Cluster virtual name if applicable
                [Microsoft.Win32.RegistryKey] $RegKey = $RmtReg.OpenSubKey("SOFTWARE\Microsoft\Microsoft SQL Server\$($regSqlSubKey)\Cluster")
                if ($RegKey) {
                    $iServer = $RegKey.GetValue("ClusterName") + "." + $DomainName
                }
                else {
                    $iServer = $serverNameDns 
                }
                $iInst = $regSqlLabel -replace "^MSSQLSERVER$", ""
                if ($iInst) {
                    $SqlInstanceName = $iServer + "\" + $iInst 
                }
                else {
                    $SqlInstanceName = $iServer                     
                }

                # Get the SQL Serer Agent service status
                [Microsoft.Win32.RegistryKey] $RegKeyAGNT = $RmtReg.OpenSubKey("SYSTEM\CurrentControlSet\Services\$($svcSqlAgent)")
                if ($RegKeyAGNT) {
                    $iAStart = $RegKeySVC.GetValue("Start") 
                    if ($iASTart -eq 2) { $strAStart = 'Auto'}
                    elseif ($iASTart -eq 3) { $strAStart = 'Manual'}
                    elseif ($iASTart -eq 4) { $strAStart = 'Disabled'}
                    else { $strAStart = 'Unknown'}
                }
                else {
                    $strAStart = 'Unknown' 
                }
                $strAStatus = (Get-Service $svcSqlAgent).Status

                # Get the SQL Server Data Engine service status
                [Microsoft.Win32.RegistryKey] $RegKeySVC = $RmtReg.OpenSubKey("SYSTEM\CurrentControlSet\Services\$($svcSqlEngine)")
                if ($RegKeySVC) {
                    $iDStart = $RegKeySVC.GetValue("Start") 
                    if ($iDSTart -eq 2) { $strDStart = 'Auto'}
                    elseif ($iDSTart -eq 3) { $strDStart = 'Manual'}
                    elseif ($iDSTart -eq 4) { $strDStart = 'Disabled'}
                    else { $strDStart = 'Unknown'}
                }
                else {
                    $strDStart = 'Unknown' 
                }
                $strDStatus = (Get-Service $svcSqlEngine).Status

                
                # Build the instance information object
                $instInfo = New-Object PSObject -Property @{
                    RegLabel = $regSqlLabel
                    RegSubKey = $regSqlSubKey
                    InstanceName = $iInst
                    ServerNameDNS = $serverNameDns
                    FullInstanceName = $SqlInstanceName
                    SvcSqlEngineName  = $svcSqlEngine
                    SvcSqlEngineStart = $strDStart
                    SvcSqlEngineStatus = $strDStatus
                    SvcSqlAgentName = $svcSqlAgent
                    SvcSqlAgentStart = $strAStart
                    SvcSqlAgentStatus = $strAStatus
                }            

                $instances += $instInfo
            }
            $instances | FT            
        }
        catch {
            "Registry fetch Error:"
            $_.Exception
        }


        #
        #   Get the list of volumes and mountpoints
        #

        try {
            $Volumes = Get-CimInstance  -namespace root/cimv2 Win32_Volume -ComputerName $serverNameDns -Filter "DriveType='3'" -ErrorAction Continue

            $vols = @()
            foreach ($vol in ($Volumes | Sort-Object Name)) {           
                if ($vol.Name -match "^[A-Z]:\\") {
                    $vols += New-Object PSObject -Property @{
                        Name = $vol.Name
                        Label = "" + $vol.Label
                        FreeSpaceGB = [System.Decimal]([Math]::Round($vol.FreeSpace /1GB,2))
                        TotalSizeGB = [System.Decimal]([Math]::Round($vol.Capacity /1GB,2))
                        ComputerName = $vol.SystemName + '.' + $DomainName
                        DateInserted = [System.DateTime](Get-Date -Format "yyyy-MM-dd HH:mm:ss")
                    }            
                }   
            }
            $Vols | Export-Csv 'C:\Temp\VolInfo.csv' -Append  -Delimiter ','     
            #$vols | FT
            #$vols2 = Out-DataTable -InputObject $vols
            #$vols2 | FT
            #Write-SqlTableData -ServerInstance $RepoInstance -Database $RepoDatabase -SchemaName $RepoSchema -TableName $RepoTableVolumes $vols2 -Passthru
        }
        catch {
            "Get-CimInstance Error:"
            $_.Exception
        }

        #
        #   Query each instance to get the backup history and paths
        #
        $logBackupSql = @"
        DECLARE @LogInfo TABLE ( DBName VARCHAR(128), LogSizeMB Decimal(18,2), LogPct Decimal(7,2), [Status] INT);
        INSERT INTO @LogInfo ( DBName, LogSizeMB, LogPct, [Status] )
        EXEC ('DBCC SQLPERF (LOGSPACE) WITH NO_INFOMSGS;');
        WITH SVR
            AS
            (
                SELECT CASE DEFAULT_DOMAIN()
                           WHEN 'FS' THEN
                               '.fs.local'
                           WHEN 'QA' THEN
                               '.qa.fs'
                           WHEN 'DEV' THEN
                               '.dev.fs'
                           WHEN 'MFG' THEN
                               '.mfg.fs'
                           WHEN 'NPQ' THEN
                               '.npq.mfg'
                           ELSE
                               ''
                       END AS DnsDomain,
                       CAST(SERVERPROPERTY('MachineName') AS VARCHAR(128)) AS ServerName,
                       CAST(SERVERPROPERTY('InstanceName') AS VARCHAR(128)) AS InstanceName,
                       CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS VARCHAR(128)) AS NetBIOSName
            ),
               LBK
            AS
            (
                SELECT CAST(BKS.backup_start_date AS DATE) AS BkDate,
                       BKS.database_name,
                       SUM(BKS.backup_size) AS TotalSize
                   FROM msdb.dbo.backupset BKS
                   WHERE
                    (BKS.[type] = 'L')
                    AND (BKS.is_copy_only = 0)
                    AND (CAST(BKS.backup_start_date AS DATE)
                    BETWEEN DATEADD(DAY, -14, CAST(GETDATE() AS DATE)) AND CAST(GETDATE() AS DATE)
                        )
                   GROUP BY
                    CAST(BKS.backup_start_date AS DATE),
                    BKS.database_name
            ),
               LBK2
            AS
            (
                SELECT LBK.[database_name],
                       MAX(LBK.TotalSize) AS MaxDailySize
                   FROM LBK
                   GROUP BY LBK.[database_name]
            )
             SELECT SVR.ServerName + SVR.DnsDomain +
                          CASE WHEN (SVR.InstanceName IS NULL) THEN ''
                            WHEN (SVR.InstanceName = 'MSSQLSERVER') THEN ''
                              ELSE '\' + SVR.InstanceName
                              END AS InstanceName,
                    SVR.NetBIOSName + SVR.DnsDomain AS NetBiosName,
                    DB.[name] AS DatabaseName,
                    CAST(ISNULL(LBK2.MaxDailySize, 0) / (1024 * 1024 * 1024) AS DECIMAL(18, 2)) AS DailyLogBackupGB,
                    LEFT(DBF.physical_name, LEN(DBF.physical_name) - (CHARINDEX('\', REVERSE(DBF.physical_name))-1)) AS LogFileDirectory,
                    COUNT(DBF.physical_name) OVER (PARTITION BY DBF.database_id) AS LogFileCount,
                    ISNULL(LI.LogSizeMB, 0) AS LogSizeMB,
                    ISNULL(LI.LogPct, 0) AS LogPct,
                    GETDATE() AS LastRetrievedTime
                FROM sys.databases DB
                    INNER JOIN sys.master_files DBF
                        ON (DB.database_id = DBF.database_id)
                    LEFT OUTER JOIN LBK2
                        ON (DB.[name] = LBK2.[database_name])
                    LEFT OUTER JOIN @LogInfo LI
                        ON (DB.[name] = LI.[DBName])
                  CROSS JOIN SVR
                WHERE
                 (DBF.type_desc = 'LOG');
"@

        foreach ($instName in $instances) {
            $instName = $instName -replace '\\MSSQLSERVER', ''
            "Querying ... $($instName)"
            #$logBackupSql2 = $logBackupSql -replace '<<>>', $instName

            try {
                $SqlBckVolume = Invoke-Sqlcmd -ServerInstance $instName -Database "master" -Query $logBackupSql -OutputAs "DataTables"
                #$SqlBckVolume | FT               
            }
            catch {
                "Problem with query"
            }

           # $Cols = ""
            #foreach ($Column in $SqlBckVolume.Columns) {
            #    $Cols = $Cols + $Column.ColumnName + ',' 
            #}
            #$Cols = $Cols.Substring(0, $Cols.Length - 1)

            Write-SqlTableData -ServerInstance $RepoInstance -Database $RepoDatabase -SchemaName $RepoSchema -TableName $RepoTableBackupVolume -InputData $SqlBckVolume
        }
    } 

}