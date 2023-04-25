import-Module Az.Accounts
import-Module Az.storage
import-Module SqlServer
import-Module FS_Deployment

$StorageAccntName = "corpitdatabase0100"
$SASToken = "sp=racwl&st=2023-02-19T14:41:11Z&se=2023-03-10T22:41:11Z&spr=https&sv=2021-06-08&sr=c&sig=B5F%2FhECNdzglYh2ULYqIAAf0lKS2qzyxq0eI1WqDjx0%3D"

$ctx = New-AzStorageContext -StorageAccountName $StorageAccntName -SASToken $SASToken

$BakLst = Get-AzStorageBlob -Container "longterm-globalfed-2028-1231" -Context $ctx -Blob 'AZR1SQL01T904*'

$AzRunTime = Get-Date

#   Loop through all blob objects and look for the ones with the file name ending in "-1".

$tCount = '1'
foreach ($bak in ($BakLst | Sort Name -Descending)) {
    $pat = "^(AZR.+?-)(?<tnum>\d)(\.bak)$"
    $result = $bak.Name -match $pat
    if ($Matches['tnum'] -gt '1'){
        if ($Matches['tnum'] -gt $tCount) {
            $tCount = $Matches['tnum']
        }
        continue;
    }

    "[$($tCount)] - $($bak.Name)"
    $sqlFileList = "
    RESTORE FILELISTONLY 
        FROM URL = N'https://corpitdatabase0100.blob.core.windows.net/longterm-globalfed-2028-1231/$($bak.Name)' WITH  FILE = 1
    "

    #$dRows = Invoke-Sqlcmd -ServerInstance 'AZR1SQL01T301.fs.local' -Database 'master' -Query $sqlFileList
    $RFL = Invoke-FSSqlCmd -Instance 'AZR1SQL01T301.fs.local' -Database 'master' -GetSchema -Query $sqlFileList
 
    $ColBlobName = [System.data.DataColumn]::new("BlobName",[String])
    $ColBlobName.DefaultValue = $bak.Name
    $RFL.Data.Columns.Add($ColBlobName)   

    $ColTCount = [System.data.DataColumn]::new("tCount",[int])
    $ColTCount.DefaultValue = $tCount
    $RFL.Data.Columns.Add($ColTCount)   

    #$ColList = $RFL.Schema | Select -ExpandProperty ColumnName
    $ColList = $RFL.Data.Columns | Select -ExpandProperty ColumnName

    Write-FSSqlDataTable -SqlInstanceName 'EDR1SQL01S004\DBA' -Database 'RepoInstanceInfo' -TableSchema 'dbo' -TableName 'Restore-FileListOnly' `
                         -Columns $ColList -DataTable $RFL.Data[0]
    $tCount = '1'
}

