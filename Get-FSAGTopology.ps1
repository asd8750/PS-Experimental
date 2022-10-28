# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#Import-Module -Name dbatools
#Import-Module SqlServer

function global:Get-FSAGTopology  {
    <#
.SYNOPSIS

Obtain the full AG/DAG HA status of the S6 server/instance status

.DESCRIPTION

When executed from an S6 plant ODS instance, this script will determine the AG/DAG structure in this cluster.  Then a remote query 
is submitted to each component SQL instance for additional status information.  
Once obtained, the combination status is saved to a central database.


.EXAMPLE

PS> Get-FSDagStatus -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $S6OdsInstance,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $RepoInstance = "",

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [System.Management.Automation.PSCredential] 
        $Credential 

        # [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        # [string] 
        # $RepoUserName = "",

        # [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        # [securestring] 
        # $RepoPassword = ""
    )

    Process {
        $VerbosePreference="Continue"

        $FetchList = @{};   # Array for servers to query
        $FetchCompleteCnt = 0 # Number of completed fetchs

        $FetchList[$S6OdsInstance] = New-Object PSObject -Property @{
            Instance = $S6OdsInstance
            Location = "ODS"
            Status = "Fetch"
        }

        $InstCompleted = @{};
        $DBRows = @();
        #   Loop until all queued fetch requests are processed
        #
        while ($FetchList.Count -gt $FetchCompleteCnt) {
            $FetchList.GetEnumerator().Where({($_.Value).Status -ieq "Fetch" } ) | ForEach-Object {
                $fetchSvr = $_.Value
                #$fetchSvr
                try {
                    Write-Verbose "Fetching  : $($fetchSvr.Instance)"
                    $SourceInst = $fetchSvr.Instance
                    if ($fetchSvr.Location -eq "ODS") {
                        $instResults = Get-HADRInfo -instance $fetchSvr.Instance 
                    }
                    else {
                        $instResults = Get-HADRInfo -instance $fetchSvr.Instance -credential $credential                
                    }
                    $fetchSvr.Status = "Completed"
                    $fetchSvr | Add-Member -MemberType NoteProperty -Name "ResultSet" -Value $instResults

                    #   If rows were returned from the query, check the SourceInst column for the name of the instance.
                    if ($instResults -ne $null) {
                        $rowCount = $instResults.Count  
                        if ($rowCount -gt 0) { $SourceInst = $instResults[0].SourceInst }             
                        Write-Verbose "Processing: $($fetchSvr.Instance) ==> $($SourceInst) ($($rowCount) Rows)" 
                        if (($rowCount -gt 0) -and ($InstCompleted[$SourceInst] -eq $null)) {
                            $InstCompleted[$SourceInst] = $instResults[0].SourceInst
                            foreach ($db in $instResults) {
                                #   Check for the other AG replicas 
                                if (($FetchList[$db.AGReplServer] -eq $null) -and (-not $db.DBIsLocal)) {
                                    Write-Verbose "Queue     : $($db.AGReplServer) ==> $($db.EndPointServer) - ($($fetchSvr.Location))"
                                    $FetchList[$db.AGReplServer] = New-Object PSObject -Property @{
                                        Instance = $db.EndPointServer
                                        Location = $fetchSvr.Location
                                        Status = "Fetch"
                                    }
                                }
            
                                #   Check for other DAG replicas on the MFG side of the firewall
                                if ($db.InDAG) {
                                    if (($fetchSvr.Location -eq "ODS") -and ($FetchList[$db.DAGRmtSvr] -eq $null)) {
                                        Write-Verbose "Queue     : $($db.DAGRmtSvr) -- (MFG)"
                                        $FetchList[$db.DAGRmtSvr] = New-Object PSObject -Property @{
                                            Instance = $db.DAGRmtSvr
                                            Location = "MFG"
                                            Status = "Fetch"
                                        }
                                    }
                                }
                        
                                if ($db.DBIsLocal) { $DBRows += $db}
                            }
                        }
            
                    }
                }
                catch {
                    $fetchSvr.Status = "Error"
                    $fetchSvr | Add-Member -MemberType NoteProperty -Name "ErrorMsg" -Value $_.Exception.Message
                }
                $FetchCompleteCnt += 1
            }
        }

        $DBRows | Sort-Object -Property InDAG, DAGName, AGName, DatabaseName, SourceInst | FT
    }    
}



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#   Private function to query a SQL Server about HA/DR settings
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
function Get-HADRInfo {
    Param ([string] $instance, [System.Management.Automation.PSCredential] $credential)
    $sqlAgInfo = @"
    DECLARE @RegValue NVARCHAR(256);
    EXEC [master].[dbo].[xp_regread] @rootkey=N'HKEY_LOCAL_MACHINE', @key=N'Cluster', @value_name=N'ClusterName', @value=@RegValue OUTPUT;
    SELECT	TOP (1000000)
            CONVERT(VARCHAR(128), @@SERVERNAME) AS SourceInst,
            AG.[name] AS AGName,
            AG.[group_id] AS AGID,
            AR.replica_server_name AS AGReplServer,
            AR.failover_mode_desc AS AGFailMode,
            AR.availability_mode_desc AS AGAvlMode,
            AR.seeding_mode_desc AS AGSeeding,
            ISNULL(HARS.role_desc, 'UNKNOWN') AS AGReplRole,
            --URL1.ServerName AS EndPointServer,
            SUBSTRING(LEFT(AR.[endpoint_url], CHARINDEX(':', AR.[endpoint_url], 6) - 1), 7, 128) AS EndPointServer,
            IIF(DAG.[name] IS NULL, 0, 1) AS InDAG,
            ISNULL(HDRS.database_id, 0) AS DatabaseID,
            HDRCS.[database_name] AS DatabaseName,
            HDRCS.is_failover_ready AS IsFailoverReady,
            ISNULL(HDRS.is_local, 0) AS DBIsLocal,
            HDRS.synchronization_health_desc AS DBSyncHealth,
            HDRS.synchronization_state_desc AS DBSyncState,
            ISNULL(HDRS.log_send_queue_size, 0) AS DBLogSendQueueSize,
            ISNULL(HDRS.log_send_rate, 0) AS DBLogSendRate,
            ISNULL(HDRS.redo_queue_size, 0) AS DBRedoQueueSize,
            ISNULL(HDRS.redo_rate, 0) AS DBRedoRate,
            ISNULL(HDRS.low_water_mark_for_ghosts, 0) AS DBLowWaterMark,
            HDRS.last_hardened_lsn AS DBLastHardenedLsn,
            HDRS.last_received_lsn AS DBLastReceivedLsn,
            --AG.is_distributed AS IsDAG,
            DAG.[name] AS DAGName,
            DAG.group_id AS DAGID,
            --URLRmt.ServerName AS DAGRmtSvr,
            SUBSTRING(LEFT(DARRmt.[endpoint_url], CHARINDEX(':', DARRmt.[endpoint_url], 6) - 1), 7, 128) AS DAGRmtSvr,
            DARRmt.replica_server_name AS DAGRmtAG,
            --CASE WHEN DARPS.role_desc IS NULL THEN 'PRIMARY' ELSE DARPS.role_desc END AS DAGReplRole,
            IIF(DAG.[name] IS NULL, NULL, ISNULL(DARPS.role_desc, 'PRIMARY')) AS DAGReplRole,
            DARRmt.availability_mode_desc AS DAGAvlMode,
            DARRmt.failover_mode_desc AS DARFailMode,
            DARRmt.seeding_mode_desc AS DARSeeding,
            DAGState.synchronization_health_desc AS DAGSyncHealth,
            CONVERT(VARCHAR(256), UPPER(@RegValue)) AS ClusterName

            --,DARRmt.*
        FROM sys.availability_groups AG
            INNER JOIN sys.availability_replicas AR
                ON (AG.[group_id] = AR.[group_id])
            LEFT OUTER JOIN sys.dm_hadr_availability_replica_states HARS
                ON ( AG.group_id = HARS.group_id )
                    AND ( AR.replica_id = HARS.replica_id )
            LEFT OUTER JOIN (
                    sys.availability_groups DAG
                INNER JOIN sys.availability_replicas DAR
                    ON (DAG.[group_id] = DAR.[group_id])
                INNER JOIN sys.availability_replicas DARRmt
                    ON (DAG.[group_id] = DARRmt.[group_id]) AND (DAR.[replica_id] <> DARRmt.[replica_id])
                INNER JOIN sys.dm_hadr_availability_group_states DAGState
                    ON (DAG.group_id = DAGState.group_id)
                INNER JOIN sys.dm_hadr_availability_replica_states DARPS
                    ON (DAR.replica_id = DARPS.replica_id)
                    )
                ON (AG.[name] = DAR.replica_server_name)
            LEFT OUTER JOIN (
                sys.dm_hadr_database_replica_states HDRS
                INNER JOIN sys.dm_hadr_database_replica_cluster_states HDRCS
                    ON (HDRS.group_database_id = HDRCS.group_database_id)
                        AND (HDRCS.replica_id = HDRS.replica_id)
                    )
                ON (HDRS.group_id = AR.group_id) AND (HDRS.replica_id = AR.replica_id)

        WHERE (AG.is_distributed = 0)
        ORDER BY AG.[name], DatabaseName, AR.replica_server_name
"@
    $agInfo = Invoke-Sqlcmd -Query $sqlAgInfo -ServerInstance $instance # -credential $credential
   # $agInfo = Invoke-DbaQuery  -query $sqlAgInfo -SqlInstance $instance -SqlCredential $credential
    Write-Output $agInfo
}


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#Import-Module -Name dbatools

$Username = 'MFG\MG111257'
$Password = 'CWBCle6V<3hO~n#*T'
$secpasswd = ConvertTo-SecureString $Password -AsPlainText -Force
$credential = New-Object System.Management.Automation.PSCredential ($Username, $secpasswd) 
#
Get-FSAGTopology -S6OdsInstance "PBG1SQL01L205.fs.local" -RepoInstance "EDR1SQL01S004\DBA" -credential $credential