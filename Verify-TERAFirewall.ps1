function Verify-TERAFirewall  {
    <#
.SYNOPSIS

Verify and optionally update the configured TCP port for installed SQL instances

.DESCRIPTION

.PARAMETER InstanceName
Specifies the SQL instance to test and modify

.PARAMETER TCPPort
TCP port # to be used by the tested SQL instance  (Default: 1433)

.PARAMETER UpdatePort
If present, the script will update the SQL instance registry info with the TCP port # to use  (Default: no update)

.OUTPUTS

PS Object

.EXAMPLE

PS> Verify-TERAFirewall -InstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $InstanceName ,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] $AltUsername = "NONE",

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [SecureString] $AltPassword
    )

    #   Check for availability groups and their associated distributed availability groups
    #
    $SqlDag = "
    SELECT	AG.[name] AS AGName, 
            UPPER(AR.replica_server_name) AS ARName,
            UPPER(AR.[endpoint_url]) AS AREndpoint,
            IIF(DAG.[name] IS NULL, 0, 1) AS has_DAG,
            DAG.[name] AS DAGNam,
            UPPER(DAR.[endpoint_url]) AS DAGEndpoint,
            UPPER(DAR2.[endpoint_url]) AS DAGEndpoint2
        FROM sys.availability_groups AG
            INNER JOIN sys.availability_replicas AR
                ON (AG.group_id = AR.group_id) 
            LEFT OUTER JOIN (
                sys.availability_groups DAG
                INNER JOIN sys.availability_replicas DAR
                    ON (DAG.group_id = DAR.group_id) AND (DAG.is_distributed = 1)
                INNER JOIN sys.availability_replicas DAR2
                    ON (DAR.group_id = DAR2.group_id) AND (DAR.replica_id != DAR2.replica_id)
                ) ON (DAR.[replica_server_name] = AG.[name])
        WHERE (AG.is_distributed = 0)
            AND (DAG.[name] IS NOT NULL)
        ORDER BY AGName";

    $AgInfo1 = Invoke-Sqlcmd -ServerInstance $InstanceName -query $SqlDag

    $SvrList = $AgInfo1 | Select-Object ARName -Unique                      # Get the list of servers on this side of the firewall

    $DagList = $AgInfo1 | Select-Object DAGEndpoint, DAGEndpoint2 -Unique   # Get the endpoint pair connecting both sides of the firewall


    foreach ($dag in $DagList) {
        $UrlIP = ($dag.DAGEndpoint2.Split('/')[2]).Split(':')[0]
        foreach ($svr in $SvrList) {
            $AllowDist = Invoke-Sqlcmd -ServerInstance $svr.ARName-Query "SELECT CONVERT(INT,[value]) AS AllowDist FROM sys.configurations WHERE [name]='Ad Hoc Distributed Queries'"
            # If ($AllowDist.AllowDist -eq 0) {
            #     $OptShow = Invoke-Sqlcmd -ServerInstance $svr.ARName -Query "
            #         EXEC sp_configure 'show advanced options', 1;  
            #         RECONFIGURE  WITH OVERRIDE;"                
            #     $OptShow = Invoke-Sqlcmd -ServerInstance $svr.ARName -Query "
            #         EXEC sp_configure 'Ad Hoc Distributed Queries', 1;  
            #         RECONFIGURE  WITH OVERRIDE;"
            # }
            try {                
                $AltCredential = Get-Credential $AltUsername
                $GetOTF = Start-Job -ScriptBlock {
                    Invoke-Sqlcmd -ServerInstance $UrlIP -Query "SELECT @@VERSION"
                } -Credential $AltCredential
                Wait-Job $GetOTF
                $GetOTFResults = Receive-Job -Job $GetOTF
                $GetOTFResults
            }
            catch {
                $a = $_
            }

            # If ($AllowDist.AllowDist -eq 0) {
            #     $OptShow = Invoke-Sqlcmd -ServerInstance $svr.ARName -Query "
            #         EXEC sp_configure 'Ad Hoc Distributed Queries', 0;  
            #         RECONFIGURE  WITH OVERRIDE;
            #         EXEC sp_configure 'show advanced options', 0;  
            #         RECONFIGURE; "
            # }
        }
    }

}

Verify-TERAFirewall -InstanceName "PBG1SQL02T106.qa.fs" -AltUsername 'NPQ\NP111257'
