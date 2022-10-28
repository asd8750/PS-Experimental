
function Invoke-SQL {
    param(
        [string] $dataSource = "PBGMESProcessData\ProcessData",
        [string] $database = "ProcessData",
        [string] $sqlCommand = $(throw "Please specify a query."),
        [string] $userid = "",
        [string] $passwd = ""
      )

    $connectionString = "Server=tcp:$dataSource; " +
            "Database=$database; " +
            "Connection Timeout=15; "

    if ($userid -eq "") {
        $connectionString += "Integrated Security=SSPI; "
    }
    else {
        $connectionString += "UID=$($userid); PWD=$($passwd); "
    }
    

    $connection = new-object system.data.SqlClient.SQLConnection($connectionString)
    $command = new-object system.data.sqlclient.sqlcommand($sqlCommand,$connection)
    $connection.Open()

    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataSet) | Out-Null

    $connection.Close()
    return $dataSet
}

$sqlCommand = @"
SELECT DISTINCT
       FullInstanceName,
       ServerName,
       serverTable.isSrc,
	   serverTable.username,
	   serverTable.[password]
   FROM
       (
           SELECT sourceTable.FullInstanceName,
                  CASE
                      WHEN backSlashIndex > 0 THEN
                          SUBSTRING(sourceTable.[FullInstanceName], 0, backSlashIndex)
                      ELSE
                          sourceTable.[FullInstanceName]
                  END AS ServerName,
                  1 AS isSrc,
                  sourceTable.username,
                  [sourceTable].[password]
              FROM
                  (
                      SELECT DISTINCT
                             [server] AS FullInstanceName,
                             CHARINDEX(CHAR(92), [server]) AS backSlashIndex,
                             TS.username,
                             TS.[password]
                         FROM dbo.tc_task TT
                             INNER JOIN tc_taskdetail TD
                                ON TD.taskid = TT.id
                             INNER JOIN dbo.tc_server TS
                                ON TD.source_serverid = TS.id
                         WHERE
                          ( TT.active = 1 )
                  ) AS sourceTable
           UNION
           SELECT destTable.FullInstanceName,
                  CASE
                      WHEN destTable.backSlashIndex > 0 THEN
                          SUBSTRING(destTable.[FullInstanceName], 0, destTable.backSlashIndex)
                      ELSE
                          destTable.[FullInstanceName]
                  END AS ServerName,
                  0 AS isSrc,
                  destTable.username,
                  destTable.[password]
              FROM
                  (
                      SELECT DISTINCT
                             TS.[server] AS FullInstanceName,
                             CHARINDEX(CHAR(92), TS.[server]) AS backSlashIndex,
                             TS.username,
                             TS.[password]
                         FROM dbo.tc_task TT
                             INNER JOIN dbo.tc_server TS
                                ON TT.dest_serverid = TS.id
                         WHERE
                          ( TT.active = 1 )
                  ) AS destTable
      ) AS serverTable
   ORDER BY isSrc, FullInstanceName;
"@

# Fetch the DataCustodian configuration information
$dataTable = (Invoke-SQL -dataSource "PBG1SQL01V104\ProcessData" -Database "ProcessData" -sqlCommand $sqlCommand).Tables[0]
$hCnt = $dataTable.Rows.Count
$hIdx = 0

$ipList = @()
$failedLookups = @()
$LookupList = @()

#loop though results 
$dataTable.rows | ForEach-Object {
    $row = $_;
    $HostToTest = $row["ServerName"]
    $TestData = New-Object -TypeName PSObject -Property @{
        Name = $HostToTest
        Host = ""
        FullInstanceName = $row["FullInstanceName"]
        IP = "Unknown"
        Other = ""
        Pingable = $false
        User = $row["username"]
        PWD = $row["password"]
        Version = ""
        Error = ""
    }
    try {
        $hIdx += 1
        Write-Host "Resolving: $($TestData.Name) [$($hIdx) of $($hCnt)] ..."
        Resolve-DnsName $row["ServerName"] -DnsOnly -ErrorAction Stop | Where-object {($_.Type -eq 'A') -or ($_.Type -eq 'CNAME')} | ForEach-Object { 
            #$_ | FT
            if($_.Type -eq "A") {
               $TestData.IP = $_.IPAddress 
            }
            elseif ($_.Type -eq "CNAME") {
                $TestData.Host = $_.NameHost
            } 
            else {
               $TestData.Other = $_.Type
            }
        }
    }
    catch {
        $TestData.Error = $_.Exception.Message  #  Capture the Dns-Resolve error
    }
    $LookupList += $TestData

    # Try to ping the host
    if ($TestData.IP -ne "Unknown") {
        Write-Host "    Pinging: $($TestData.Name) ..."
        $TestData.Pingable = (Test-Connection -ComputerName $HostToTest -Quiet -Count 3)
    }


    # Now try to retrieve data
    if ($TestData.Pingable -eq $true) {
        Write-Host "    Contacting: $($TestData.Name) ..."
        try {
            $SqlResults = Invoke-SQL  -dataSource $TestData.FullInstanceName -database "master" -sqlCommand "SELECT @@VERSION AS [Version];" `
                                                -userid $TestData.User -passwd $TestData.PWD
            $dtSql = $SqlResults.Tables[0]
            if ($dtSql.Rows.Count -gt 0) {
                $Version = $dtSql.Rows[0]["Version"]
                $TestData.Version = $Version.Replace("`r","  ").Replace("`n","  ")
            }
            else {
                $TestData.Error = "No Data"
            }
            Write-Host "    SQL: $($TestData.Error) - $($testData.Version)"
        }
        catch {
            $ex = $_.Exception
            do {
                $TestData.Error = $ex.Message  #  Capture the SQL Error
                $ex = $ex.InnerException
            } until (
                $null -eq $ex
            )
            Write-Host "    SQL Error: $($TestData.Error)"
        }
    }

}


$LookupList | Sort-Object Name | FT

$LookupList | Sort-Object Name | Export-Csv "C:\Temp\serverIPAndTest.csv" -Force
