#Import-Module   SqlServer

#Import-Module -Name  C:\Projects\DBA-Deployments\FS_Deployment\1.0\FS_Deployment.psd1
Import-Module -Name  FS_Deployment

#
#  ############################################################################################
#   Setup information about the Data Repository
#  ############################################################################################
$TSXMasterServer = "EDR1SQL01S004.fs.local\DBA"
$RepoInstance = "EDR1SQL01S004.fs.local,50003"
$RepoDatabase = "RepoInstanceInfo"
$RepoSchema   = 'MonitorData'

#
#  ############################################################################################
#   Build the array list of SQL Server instances to be queried
#  ############################################################################################
$TargetInstanceList = @();

$MSXTSXServers = Invoke-Sqlcmd -ServerInstance $TSXMasterServer -Database "msdb" -Query "
    SELECT  DISTINCT server_name
        FROM (
        SELECT  @@SERVERNAME AS Server_name
        UNION ALL
        SELECT	server_name 
            FROM msdb.dbo.systargetservers
            WHERE (last_poll_date > DATEADD(DAY, -1, GETDATE()))
        ) SRVS
        ORDER BY server_name; "
$TargetInstanceList = ($MSXTSXServers | Select-Object -ExpandProperty server_name)
#$MSXTSXServers | FT
#$TargetInstanceList | FT

#   Get the list of valid tables in the Repo instance

$RepoTableLists = @{};
$RepoTableLists[$RepoDatabase] = @{}  # Create an empty hash list of tables for the current repo-database

#
#   Finally, get the ListNames of queries to be used
#
$MLists = Invoke-SQLCmd -ServerInstance $RepoInstance -Database $RepoDatabase -Query "
    SELECT  *
        FROM [RepoInstanceInfo].[Monitor].[FetchInfoList]
        WHERE (ListName NOT LIKE '%Test%')"
#
#   Loop through each ListName
#
foreach ($ml in $MLists) {
    Invoke-MonitorRunner -TargetInstanceList $TargetInstanceList -RepoInstance $RepoInstance -RepoDatabase RepoInstanceInfo -RepoSchema $RepoSchema -ListName $ml.ListName 
}
