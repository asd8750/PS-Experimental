function Verify-FSSqlTcpPort  {
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

PS> Get-FSSqlPartitionInfo -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] $InstanceName = "",

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [int] $TCPPort = 1433,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [switch] $UpdatePort
    )
    Begin {}

    #
    Process {
        #  Get the list of installed instances
        #
        $RKSqlBase = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server"
        $RKSqlInstalls = "$($RKSqlBase)\Instance Names"

        $Installs = @{}

        $INames = (Get-ChildItem -Path $RKSqlInstalls | Where-Object PSChildName -eq "SQL" ).Property

        $INames | ForEach-Object {
            $InstName = $_
            if (($InstanceName -eq "") -or ($InstanceName -ieq $InstName)) {
                $Inst = New-Object -TypeName PSCustomObject -Property @{
                    Name = $InstName 
                    RKeyName = (Get-ItemProperty -Path "$($RKSqlInstalls)\SQL" -Name $_ )."$($_)"
                    RKey = ""
                    Version = ""
                    TCPPort = ""
                    TCPPortDyn = ""
                }
                $Inst.RKey = "$($RKSqlBase)\$($Inst.RKeyName)"  

                $Inst.Version = (Get-ItemProperty -Path "$($Inst.RKey)\MSSQLServer\CurrentVersion" -Name "CurrentVersion")."CurrentVersion"

                $Inst.TCPPort = (Get-ItemProperty -Path "$($Inst.RKey)\MSSQLServer\SuperSocketNetLib\Tcp\IPAll" -Name "TcpPort")."TcpPort"
                $Inst.TCPPortDyn = (Get-ItemProperty -Path "$($Inst.RKey)\MSSQLServer\SuperSocketNetLib\Tcp\IPAll" -Name "TcpDynamicPorts")."TcpDynamicPorts"

                $Installs[$InstName] = $Inst
            }
        }
    }


    End {
        #$rsGetPtInfo | FT
        if ($UpdatePort) {
            if ($Installs[$InstanceName]) {
                $Inst = $Installs[$InstanceName]
                if ($UpdatePort) {
                    try {
                        if ($Inst.TCPPortDyn.Length -gt 0) {
                            Set-ItemProperty  -Path "$($Inst.RKey)\MSSQLServer\SuperSocketNetLib\Tcp\IPAll" -Name "TcpDynamicPorts" -Value "" -ErrorAction Stop
                            Write-Verbose "Updated Instance '$($Inst.Name)': TCP Dynamic Port - '$($Inst.TCPPortDyn)' --> ''"
                        }
                        Set-ItemProperty -Path "$($Inst.RKey)\MSSQLServer\SuperSocketNetLib\Tcp\IPAll" -Name "TcpPort" -Value $TCPPort.ToString() -ErrorAction Stop
                        Write-Verbose "Updated Instance '$($Inst.Name)': TCP Fixed Port - '$($Inst.TCPPort)' --> '$($TCPPort)'"
                    }
                    catch {
                        Write-Error  "Update failed - May not be running in Administrator Mode"
                    }
                }  
                else {
                    Write-Verbose "Located Instance '$($Inst.Name)': TCP Fixed Port - '$($Inst.TCPPort)' --- TCP Dynamic Port - '$($Inst.TCPPortDyn)'"               
                }          
            } 
            else {
                Write-Error "Cannot find InstanceName: '$($InstanceName)'"
            }
        }
        Write-Output $Installs;
    }
}

# ============================================================
#   Test and correct a SQL Server instance TCP port #
# ============================================================
$sqlInfo = Verify-FSSqlTcpPort 
$sqlInfo  | ft

$SqlIName = "SQL2016"   # <<<<======  Place your SQL instance name in quotes

$sqlInfo = Verify-FSSqlTcpPort $SqlIName

if ($sqlInfo -eq $null) {
    Write-Error "No SQL Instances found"
}
elseif ($sqlInfo.Count -gt 1) {


    Write-Error "More than one SQL instance found! -- Please specify which one is to be corrected."
}
else {
    Verify-FSSqlTcpPort $SqlIName -Verbose  -UpdatePort
}
