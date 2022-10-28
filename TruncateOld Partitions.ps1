cls

Import-Module dbatools
Import-Module FS_Deployment

#Import-Module -Name  C:\Projects\DBA-Deployments\FS_Deployment\1.0\FS_Deployment.psd1

$oldverbose = $VerbosePreference
#$VerbosePreference = "continue"

$InstanceName = 'azr1sql01L906.fs.local'

$RetainBeforeDate = Get-Date -Date '3/1/2020'
$RetainAfterDate = Get-Date -Date '4/1/2020'

$dbList = Get-FSSqlDatabases -FullInstanceName $InstanceName
#$dbList | FT

$neededIndexTypes = 0, 1, 5;

foreach ($db in ($dbList | Where-Object {$_.DatabaseName -like 'Performance*'} | Sort-Object DatabaseName)) {  #
    if ($db.AGRole -ilike 'READ_WRITE') {
        "`r`n"
        "`r`n -- ==========================================================================="
        "`r`nUSE [$($db.DatabaseName)] "
        $ptDBInfo = Get-FSSSqlPartitionInfo -FullInstanceName "azr1sql01t904" -Database $db.DatabaseName
        $ptInfo = $ptDBInfo.Functions
        foreach ($ptf in ($ptInfo.Values | Where-Object { ($_.FuncName -ilike '*_Monthly*') -or 
                                                         ($_.FuncName -ilike '*_Weekly*') -or 
                                                         ($_.FuncName -ilike '*_Daily*') -or
                                                         ($_.FuncName -ieq 'PtFunc_S60PerformanceTrackerControllers')   } )) {
            
            $eligiblePt = @()
            $bndStart = Get-Date -Date '1900-01-01'  # Partition 1 has unspecified start date
            for (($pIdx = 0); ($pIdx -lt $ptf.Fanout); ($pIdx++)) {
                $ptNum = $pIdx + 1
                if ($pidx -lt ($ptf.Fanout-1)) {
                    $bndEnd = Get-Date -Date $ptf.BndList[$pIdx]
                }
                else {
                    $bndEnd = Get-Date -Date '2099-01-01' # Last partition has no end date, I use year 2099
                }
                if ( (($bndStart -lt $RetainAfterDate) -and ($bndEnd -gt $RetainBeforeDate )) ) { # -and (($bndStart -lt $RetainAfterDate) -or ($bndEnd -ge $RetainBeforeDate )) 
                    $crossBnd = $false
                    $action = "OK"
                    if (($bndStart -lt $RetainBeforeDate) -or ($bndEnd -gt $RetainAfterDate )) {
                        $crossBnd = $true
                        $action = "Cross Boundary"
                    }
                    #"----- Function: $($ptf.FuncName) -- [$($ptNum)]  BndStart: '$($bndStart)'  BndEnd: '$($bndEnd)'  Action: $($action)"
                    $eligiblePt += New-Object PSObject -Property @{
                        PtNum    = $ptNum
                        BndStart = $BndStart
                        BndEnd   = $BndEnd
                        CrossBnd = $crossBnd
                        Action   = $action
                    }
                }
                $bndStart = $bndEnd
            }

            if ($eligiblePt.Count -gt 0) {
                $tablesProcessed = @{}
                $ptf.Schemes.Values | ForEach-Object {
                    $pts = $_
                    "`r`n   --   Scheme: $($pts.SchemeName) "
                    $pts.Indexes.Values | Where-Object { $neededIndexTypes -contains $_.IndexID } | Sort-Object TableObjID,IndexID | ForEach-Object {
                        $pti = $_
                        "`r`n   --    Table: [$($pti.TableSchema)].[$($pti.TableName)] "
                        if ($tablesProcessed[$pti.TableObjID] -eq $null) {
                            $tablesProcessed[$pti.TableObjID] = $pti
                            $eligiblePt | Sort-Object PtNum | ForEach-Object {
                                if ($_.Action -eq "OK") {
                                    if ($pti.Rows[$_.PtNum-1] -gt 0) {
                                        "TRUNCATE TABLE [$($db.DatabaseName)].[$($pti.TableSchema)].[$($pti.TableName)]  WITH ( PARTITIONS ( $($_.PtNum) )) --  BndStart: '$($_.BndStart)'  BndEnd: '$($_.BndEnd)' -  Rows: $($pti.Rows[$_.PtNum-1])"
                                    }
                                    else {
                                        #"   --       PT: [$($_.PtNum)] -- $($_.Action) --  BndStart: '$($_.BndStart)'  BndEnd: '$($_.BndEnd)' -  Rows: $($pti.Rows[$_.PtNum-1])"               
                                    }
                                }
                                ELSE {
                                    "   --       PT: [$($_.PtNum)] -- $($_.Action) --  BndStart: '$($_.BndStart)'  BndEnd: '$($_.BndEnd)' -  Rows: $($pti.Rows[$_.PtNum-1])"               
                                }
                            }
                        }
                    }
                }           
            }

        }
    }

}

#$VerbosePreference = $oldverbose
