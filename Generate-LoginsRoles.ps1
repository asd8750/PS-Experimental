function Generate-LoginsRoles  {
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
        [string] $InstanceName ,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $OutputDirectory 
    )

    #   Check for availability groups and their associated distributed availability groups
    #
    $SqlDag = "";

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

        }
    }

}

Generate-LoginsRoles -InstanceName "PBG1SQL02T106.qa.fs" -OutputDirectory C:\Temp
