function Get-ServerConfig  {
    <#
.SYNOPSIS

Returns a list of messages with the server config needed for a replacement server

.DESCRIPTION

Returns a collection of server config description items.

.PARAMETER ServerName
Fully qualified server name to be tested

.INPUTS

None. 

.OUTPUTS

PS Object

.EXAMPLE

PS> Get-ServerConfig -ServerName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $OldServerName,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $NewServerName
        )
    
    function New-MsgObj {
        Param (
            [Parameter(Position=0)]
            [string] $Type,
            [Parameter(Position=1)]
            [string] $Label,
            [Parameter(Position=2)]
            [string] $Value,
            [Parameter(Position=3)]
            [string] $Msg = ""
        )
        $newObj = New-Object -TypeName PSCustomObject -Property @{
            Type  = $Type
            Label = $Label
            Value = $Value
            Msg   = $Msg
        }
        Write-Output $newObj
    }

    #
    #   List the configuration of a server
    #
    #$OldServerName = "PBG1SQL01T104.fs.local"

    $OutMsgs = @()
    $OutMsgs += New-MsgObj  "Server" "Name" $NewServerName  "New ServerName:  $($NewServerName)"


    #   Get the processor configuration
    #
    $WmiProc = Get-CimInstance -ComputerName $OldServerName -Query 'Select * from Win32_Processor'

    $OutMsgs += New-MsgObj  "System" "Name" $WmiProc[0].SystemName
    $OutMsgs += New-MsgObj  "CPU" "Description" $WmiProc[0].Description
    $CoreCnt  = 0
    $LCoreCnt = 0
    foreach ($iProc in $WmiProc) {
        $CoreCnt  += $iProc.NumberOfCores
        $LCoreCnt += $iProc.NumberOfLogicalProcessors
    }
    $OutMsgs += New-MsgObj  "CPU" "Cores"  $CoreCnt  "Cores:  $($CoreCnt)"
    $OutMsgs += New-MsgObj  "CPU" "Logicals"  $LCoreCnt

    #   Get the physical memory configuration
    #
    $WmiMem = Get-CimInstance -ComputerName $OldServerName -Query 'Select * from Win32_PhysicalMemory'
    $Memsize = 0
    foreach ($iMem in $WmiMem) {
        $Memsize += $iMem.Capacity
    }
    $MemGB = ($Memsize / (1024 * 1024 * 1024))
    $OutMsgs += New-MsgObj  "Memory" "SizeGB" $MemGB  "Memory RAM:  $($MemGB) GB"

    #   Get the Windows OS configuration info
    #
    $WmiOS = Get-CimInstance -ComputerName $OldServerName -Query 'Select * from Win32_OperatingSystem'

    $OutMsgs += New-MsgObj  "Windows" "Caption" $WmiOS[0].Caption
    $OutMsgs += New-MsgObj  "Windows" "SKU"     $WmiOS[0].OperatingSystemSKU


    #   Get the Cluster configuration info
    #
    $WmiCls   = Get-CimInstance -ComputerName $OldServerName  -ClassName "MSCluster_Cluster" -Namespace "root\mscluster" | SELECT -ExpandProperty Name
    $OutMsgs += New-MsgObj  "Cluster" "Name"  $WmiCls  "ClusterName:  $($WmiCls)"

    #   Get the SQL Server configuration info
    #
    $sqlInf   = Invoke-DbaQuery -SqlInstance $OldServerName -Database 'master' -Query 'SELECT @@VERSION AS [SqlVersion]'
    $SqlVersion = $sqlInf.SqlVersion -replace "`r*`n\s*", "|"
    $SqlVLines  = $SqlVersion.Split('|')
    $OutMsgs += New-MsgObj "SQL" "Version0" $SqlVLines[0]
    $OutMsgs += New-MsgObj "SQL" "Version1" $SqlVLines[1]
    $OutMsgs += New-MsgObj "SQL" "Version2" $SqlVLines[2]
    $OutMsgs += New-MsgObj "SQL" "Version3" $SqlVLines[3]

    #   Get the disk storage configuration
    #
    $WmiVols = Get-CimInstance -ComputerName $OldServerName -Query 'Select * from Win32_Volume'

    $WmiMP   = Get-CimInstance -ComputerName $OldServerName -Query 'Select * from Win32_MountPoint'

    $Vols = @()
    foreach ($MP in $WmiMP) {
        $MyVol = ($WmiVols | Where-Object DeviceID -eq $MP.Volume.DeviceID | Select-Object -first 1)
        if (($MyVol.DriveType -eq 3)) {
            $newVol = New-Object PSObject -Property @{
                Path = $MP.Directory.Name
                DeviceID  = $MP.Volume.DeviceID
                Capacity  = $MyVol.Capacity
                FreeSpace = $MyVol.FreeSpace
            }
            $Vols += $newVol
        }
    }

    $VolCnt = 0
    $VolSizeGB = 0

    foreach ($LVol in ($Vols | Sort-Object Path)) {
        $DrvLetter = $LVol.Path.SubString(0,1)
        if ($DrvLetter -ine "X" -and $DrvLetter -ine "C") {
            [int] $SizeGB   = [Math]::Ceiling($LVol.Capacity  / 1073741824)
            [int] $FreeGB   = [Math]::Ceiling($LVol.FreeSpace / 1073741824)
            [int] $AllocGB  = $SizeGB - $FreeGB

            $NeededGB = $SizeGB
            $OrigMsg = ""

            if (($SizeGB -gt 300) -and ($FreeGB -gt 299)) {
                $NeededGB = ($AllocGB + 100)
                $OrigMsg = "  (Orig: $($SizeGB) GB)"
            }
            # $MaxFreeGB = [Math]::Ceiling($AllocGB * 0.2)
            # if ($MaxFreeGB -gt 100) {
            #     $MaxFreeGB = 199
            # } 

            # if (($FreeGB -gt $MaxFreeGB) -and ($SizeGB -gt 500)) {
            #     $NeededGB = $AllocGB + $MaxFreeGB               
            # }
            # else {
            #     $NeededGB = $SizeGB
            #     $OrigMsg = ""
            # }

            $VolCnt += 1
            $VolSizeGB += $NeededGB
            $DrvMsg = "Disk:  '$($LVol.Path)'  -- $($NeededGB) GB" + $OrigMsg
            $OutMsgs += New-MsgObj  "Disk" $LVol.Path  "" $DrvMsg
        }
    }

    $OutMsgs += New-MsgObj  "Disk" "Count"    $VolCnt     "Vol Count:  $($VolCnt)"
    $OutMsgs += New-MsgObj  "Disk" "TotalGB"  $VolSizeGB  "Vol Size:  $($VolSizeGB) GB"

    Write-Output ($OutMsgs | Select-Object Type,Label,Value,Msg )
}


$MyServer  = 'EDR1SQL01T001.fs.local'   # Name of the old server to be replaced
$RepServer = 'EDR1SQL01T001.fs.local'   # Name for the new replacement server
$OutputDir = "C:\Temp"                  # Directory where the output is to be stored

$Info = Get-ServerConfig -OldServerName $MyServer -NewServerName $RepServer 

$Info | Export-Csv -Path "C:\Temp\$($MyServer).csv" -NoTypeInformation 

$Info | Where-Object Msg -ine "" | Select-Object  -Property Msg

$Info | Where-Object Msg -ine "" | Select-Object  -Property Msg > "$($OutputDir)\$($MyServer)_Config.txt"
