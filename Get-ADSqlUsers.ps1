Import-Module ActiveDirectory
#Import-Module SqlServer
Import-module FS_Deployment

Add-Type -AssemblyName System.DirectoryServices
Add-Type -AssemblyName System.DirectoryServices.AccountManagement

$RepoInstanceName = "EDR1SQL01S004.fs.local,50003"
$RepoDatabase = "RepoInstanceInfo"
$RepoSchema = "MonitorData"

$MyDomain = (Get-WMIObject Win32_ComputerSystem).Domain -replace '.LOCAL|.FS|.MFG',''
$DateCollected = Get-Date

#   Get the list of AD objects referenced in our SQL instances
#
$sqlFetchSqlADLogins = "
SELECT  DISTINCT
        SL.ServerName,
        SL.ServerDomain,
        SL.LoginName,
        LEFT(SL.ServerDomain,CHARINDEX('.',SL.ServerDomain)-1) AS Domain,
        RIGHT(SL.LoginName, LEN(SL.LoginName) - CHARINDEX('\',SL.LoginName)) AS AcctName,
        SL.LoginSType,
        SL.sid
    FROM MonitorData.ServerLogins SL
    INNER JOIN Monitor.tvfFetchInfo_MostRecentImportByTName('ServerLogins', DEFAULT) MRI
        ON (SL.__ImportCycle__ = MRI.ImportCycle)
    WHERE (LoginSType IN ('G', 'U'))

";

$sqlQuery = $sqlFetchSqlADLogins.Replace('<<Domain>>', $MyDomain)

$SqlLogins = Invoke-SqlCmd -ServerInstance $RepoInstanceName -Database $RepoDatabase -Query $sqlQuery

#   Build a datatable to hold collected AD user info
#
$ADUsers = New-Object System.Data.Datatable 'ADUsers'
$newCol = New-Object System.Data.DataColumn DateCollected,([datetime]); $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn DomainName,([string]);      $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn SAM,([string]);             $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn SID,([string]);             $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn ObjType,([string]);         $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn DisplayName,([string]);     $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn Description,([string]);     $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn GivenName,([string]);       $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn SurName,([string]);         $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn Name,([string]);            $ADUsers.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn DistinguishedName,([string]); $ADUsers.Columns.Add($NewCol)

$ADUsersColumnNames = $ADUsers.Columns.ColumnName                 

#   Build a datatable to hold collected AD Group info
#
$ADGroups = New-Object System.Data.Datatable 'ADGroups'
$newCol = New-Object System.Data.DataColumn DateCollected,([datetime]); $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn DomainName,([string]);      $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn NestLevel,([int]);          $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn SAM,([string]);             $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn SID,([string]);             $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn ObjType,([string]);         $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn DisplayName,([string]);     $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn Description,([string]);     $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn GivenName,([string]);       $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn SurName,([string]);         $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn Name,([string]);            $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn DistinguishedName,([string]); $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn MemberSID,([string]);       $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn MemberName,([string]);      $ADGroups.Columns.Add($NewCol)
$newCol = New-Object System.Data.DataColumn MemberType,([string]);      $ADGroups.Columns.Add($NewCol)

$ADGroupsColumnNames = $ADGroups.Columns.ColumnName                 

$LoginCnt = 0
$dc = Get-ADDomainController -DomainName $DomainDNS -Discover -NextClosestSite

#   Now process all non-group AD objects referenced in our SQL instances
#
foreach ($login in ($SqlLogins | Where-Object LoginSType -ne "G" )) {

    $adinfo = $null

    $SType = $login.LoginSType
    $sam = $login.AcctName
    $BinSid = $login.sid
    $Sid = (New-Object System.Security.Principal.SecurityIdentifier($BinSid,0)).Value

    try {
        $adu = Get-ADObject -Filter "objectSid -eq '$($Sid)'" -Properties *
    }
    catch {
        $adu = Get-ADServiceAccount -Server $dc.HostName[0] -filter 'samAccountName -eq $sam' -Properties *
    }    

    #if ($ADUsers.Rows.Count -gt 50) {break}   #######  Debug ########

    $adInfo = $ADUsers.NewRow()
    $ADUsers.Rows.Add($adInfo)

    $adInfo.SAM = $sam
    $adInfo.SID = $Sid
    $adInfo.DateCollected = $DateCollected
    $adInfo.DomainName = $MyDomain
    $adinfo.ObjType = $adu.ObjectClass
    switch ($adu.ObjectClass) {
        "user" {
            $adInfo.DisplayName     = $adu.DisplayName
            $adInfo.Description     = $adu.Description
            $adInfo.GivenName       = $adu.givenName
            $adInfo.SurName         = $adu.sn
            $adInfo.Name            = $adu.name
            $adInfo.DistinguishedName = $adu.distinguishedName
        }
    
        "msDS-GroupManagedServiceAccount" {
            $adInfo.DisplayName     = $adu.DisplayName
            $adInfo.Description     = $adu.Description
            $adInfo.GivenName       = ''
            $adInfo.SurName         = ''
            $adInfo.Name            = $adu.name
            $adInfo.DistinguishedName = $adu.distinguishedName
        }
    
        default {
            if ($adu.ObjectClass) {
                $adInfo.ObjType = $adu.ObjectClass
            } else {
                $adInfo.ObjType = "unknown"
            }       
            $adInfo.DisplayName     = ''
            $adInfo.Description     = ''
            $adInfo.GivenName       = ''
            $adInfo.SurName         = ''
            $adInfo.Name            = ''
            $adInfo.DistinguishedName = ''
        }
    }
    
}

# Now store the ADUsers in the repo
#
Write-FSSqlDataTable -SqlInstanceName $RepoInstanceName -Database $RepoDatabase -TableSchema $RepoSchema -TableName 'ADUsers' -Columns $ADUsersColumnNames -DataTable $ADUsers 

#   Build a list of AD groups to process on the first pass
#

$GroupList = @{}
$MaxGroupNest = 1
foreach ($glogin in ($SqlLogins | Where-Object LoginSType -eq "G" )) {
    if (-not $GroupList[$glogin.LoginName]) {
        $BinSid = $glogin.SID
        $Sid = (New-Object System.Security.Principal.SecurityIdentifier($BinSid,0)).Value
        $GroupList.Add($glogin.LoginName, @{NestLevel=$MaxGroupNest; SID=$Sid})
    }
}

# 
#   Now process all the groups we have encountered this far.  The "while" loop will let us process any new groups we discover that are members of already processed groups
#

while (($GroupList.GetEnumerator() | Where-Object { $_.Value.NestLevel -eq $MaxGroupNest}).Count -gt 0) {

    foreach ($grpName in ($GroupList.GetEnumerator() | Where-Object { $_.Value.NestLevel -eq $MaxGroupNest}).Name.Clone()) {

        $grp = $GroupList[$grpName]
        $Sid = $grp.SID

        $adu = Get-ADObject -Filter "objectSid -eq '$($Sid)'" -Properties * 
        if (-not $adu) {continue}   # Unknown group

        foreach ($membDN in $adu.member) {
            $adInfo = $ADGroups.NewRow()
            $ADGroups.Rows.Add($adInfo) 

            $adInfo.SAM = $grpName
            $adInfo.SID = $Sid
            $adInfo.DateCollected   = $DateCollected
            $adInfo.DomainName      = $MyDomain
            $adInfo.NestLevel       = $MaxGroupNest
            $adinfo.ObjType         = $adu.ObjectClass

            $adInfo.DisplayName     = $adu.DisplayName
            $adInfo.Description     = $adu.Description
            $adInfo.GivenName       = ''
            $adInfo.SurName         = ''
            $adInfo.Name            = $adu.name
            $adInfo.DistinguishedName = $adu.distinguishedName

            $admb = Get-ADObject -Filter "distinguishedName -eq `"$($membDN)`"" -Properties *
            if (-not $admb) {continue}

            if ($membDN -match 'CN=(?<CN>.+?),.*?DC=(?<DC>FS|NPQ|MFG|QA)') {
                $adInfo.MemberName      = "$($Matches['DC'])\$($Matches['CN'])"   # $admb.samAccountName
            }
            # else {
            #     $a2 = 4     # DEBUG ### --  Set breakpoint on this statement
            # }

            # if (-not $adInfo.MemberName) {
            #     $a3 = 4     # DEBUG ### --  Set breakpoint on this statement
            # }

            $adInfo.MemberSID       = $admb.objectSid.Value
            $adInfo.MemberType      = $admb.ObjectClass
            if ($admb.ObjectClass -eq "group") {
                if (-not $GroupList[$adInfo.MemberName]) {
                    $GroupList.Add($adInfo.MemberName, @{NestLevel=$MaxGroupNest+1; SID=$admb.objectSid.Value})
                }
            }

        }

    }

    $MaxGroupNest = $MaxGroupNest + 1
}

# Now store the ADUsers in the repo
#
Write-FSSqlDataTable -SqlInstanceName $RepoInstanceName -Database $RepoDatabase -TableSchema $RepoSchema -TableName 'ADGroups' -Columns $ADGroupsColumnNames -DataTable $ADGroups 


$a = 1
