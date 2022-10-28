cls

Import-Module dbatools
#Import-Module FS_Deployment

#Import-Module -Name  C:\Projects\DBA-Deployments\FS_Deployment\1.0\FS_Deployment.psd1

$oldverbose = $verbosepreference
$verbosepreference = "continue"

$InstanceName = 'azr1sql01L906.fs.local'

$dbList = Get-FSSqlDatabases -FullInstanceName $InstanceName
#$dbList | FT

$FGList = @();   # Create an array to hold filegroup names and date boundaries

$FGList += [PSCustomObject]@{
    FGName = 'FG_2021_Q1Q2'
    StartDate = [Datetime]::Parse('2021-01-01')
}

$FGList += [PSCustomObject]@{
    FGName = 'FG_2021_Q3Q4'
    StartDate = [Datetime]::Parse('2021-07-01')
}

foreach ($db in ($dbList | Where-Object {$_.DatabaseName -like 'Performance*'} | Sort-Object DatabaseName)) {  #
    if ($db.AGRole -ilike 'READ_WRITE') {
        $db.DatabaseName
        $ptInfo = Get-FSSqlPartitionInfo -InstanceName $InstanceName -Database $db.DatabaseName

        foreach ($pt in ($ptInfo.Functions.Values | Where-Object { ($_.FuncName -ilike '*_Monthly*') -or 
                                                         ($_.FuncName -ilike '*_Weekly*') -or 
                                                         ($_.FuncName -ilike '*_Daily*') -or
                                                         ($_.FuncName -ieq 'PtFunc_S60PerformanceTrackerControllers') } )) {
            [System.Datetime] $DateLimit = (Get-Date -Date (Get-Date).Date) # get the curent date                                                 
            if ($pt.FuncName -ilike '*_Daily*') {
                $PtStep = @{ MaxDays=31;       IncDays=1;  Name="DAY"}
                }
            elseif (($pt.FuncName -ilike '*_Weekly*') -or ($pt.FuncName -ieq 'PtFunc_S60PerformanceTrackerControllers')) {
                $PtStep = @{ MaxDays=(4 * 7);  IncDays=7;  Name="WEEK"}
                }
            elseif ($pt.FuncName -ilike '*_Monthly*') {
                $PtStep = @{ MaxDays=(2 * 30); IncDays=30; Name="MONTH"}
                }
            else {
                $PtStep = @{ MaxDays=0; IncDays=10000; Name="Error"}
            }
            $DateLimit = $DateLimit.AddDays($PtStep["MaxDays"])

            [System.Datetime] $RightMostBoundary = $pt.BndList[$pt.Fanout - 2]  # Get the current right most boundary date
            while ($RightMostBoundary -lt $DateLimit) {
                $sqlCmd = ""
                if ($PtStep["Name"] -eq "DAY") {
                    $RightMostBoundary = $RightMostBoundary.AddDays($PtStep["IncDays"])                    
                }
                elseif ($PtStep["Name"] -eq "WEEK") {
                    if ($RightMostBoundary.Day -le 23) {
                        $RightMostBoundary = $RightMostBoundary.AddDays(8);
                    } 
                    else {
                        $RightMostBoundary = $RightMostBoundary.AddDays(-($RightMostBoundary.Day - 1)).AddMonths(1)
                    }
                } 
                elseif ($PtStep["Name"] -eq "MONTH") {
                    $RightMostBoundary = $RightMostBoundary.AddMonths(1)
                }
                $NewDateStr = $RightMostBoundary.ToString("yyyy-MM-ddTHH:mm:ss")

                $sqlCmd = "SET DEADLOCK_PRIORITY HIGH; `r`n"
                foreach ($sch in $pt.Schemes.Values) {
                    $NextFG = 'FG_2021_Q3Q4'   # $sch.FGList[$pt.Fanout-1]
                    $FGEntry = ($FGList | Sort-Object StartDate -Descending | Where-Object StartDate -le $RightMostBoundary | Select-Object -First 1)
                    $NextFG = $FGEntry.FGName
                    $sqlCmdAPS = "ALTER PARTITION SCHEME [$($sch.SchemeName)] NEXT USED [$($NextFG)]; `r`n"
                    Write-Verbose "SQL-> $($sqlCmdAPS)"
                    $sqlCmd = $sqlCmd + $sqlCmdAPS
                }

                $sqlCmdAPF = "ALTER PARTITION FUNCTION [$($Pt.FuncName)]() SPLIT RANGE ('$($NewDateStr)'); `r`n"
                Write-Verbose "SQL-> $($sqlCmdAPF)"
                $sqlCmd = $sqlCmd + $sqlCmdAPF
                Invoke-DbaQuery -SqlInstance $InstanceName -Database $db.DatabaseName -Query $sqlCmd -QueryTimeout 1800          
                }
            }
        }
    }

$VerbosePreference = $oldverbose
