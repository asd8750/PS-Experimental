$OldInstance = "PBG1SQL02T105.qa.fs"
$NewInstance = "PBG1SQL02T115.qa.fs"

$SqlGetConfig = "SELECT *
                    FROM sys.configurations
                    ORDER BY [name]"

$CfgOld = Invoke-Sqlcmd -ServerInstance $OldInstance -Query $SqlGetConfig

$CfgNew = Invoke-Sqlcmd -ServerInstance $NewInstance -Query $SqlGetConfig



$Ctab = $CfgOld | % {
            foreach ($cNew in $CfgNew) {
                if ($_.name -eq $cNew.name) {
                    if ($_.value -ne $cNew.value) {
                        [PSCustomObject]@{
                            Name = $_.name
                            OC = $_
                            NC = $cNew
                        }                        
                    }
                }
            }
        }

$Ctab | Select-Object {
    "Config: [$($_.Name)]:  Old: $($_.OC.value)  New: $($_.NC.value)"
}



$a = 2
