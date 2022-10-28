Import-Module ActiveDirectory
#Import-Module SqlServer

Add-Type -AssemblyName System.DirectoryServices
Add-Type -AssemblyName System.DirectoryServices.AccountManagement


$sqlGetLogins = @"
    DECLARE		@Domain varchar(100), @key varchar(100)
    SET @key = 'SYSTEM\ControlSet001\Services\Tcpip\Parameters\'
    EXEC master..xp_regread @rootkey='HKEY_LOCAL_MACHINE', @key=@key,@value_name='Domain',@value=@Domain OUTPUT 
    --SELECT 'Server Name: '+@@servername + ' Domain Name:'+convert(varchar(100),@Domain)

    ;WITH SID1
        AS (SELECT sid, 
                    [name] AS LoginName, 
                    DATALENGTH(sid) AS SidByteLen, 
                    CONVERT(TINYINT, SUBSTRING(sid, 1, 1)) AS SidVersion, 
                    CONVERT(TINYINT, SUBSTRING(sid, 2, 1)) AS SidAuthorityCount, 
                    CONVERT(INT, SUBSTRING(sid, 3, 6))	   AS SidAuthorityIdent, 
                    CONVERT(BIGINT, CONVERT(BINARY(4), REVERSE(CONVERT(BINARY(4), SUBSTRING(sid, 9, 4))))) AS SidFirstAuthGroup 
                FROM sys.server_principals
                WHERE([type] IN('U', 'G'))),   -- Only consider Windows User or Widnows Group
        AG AS (
        SELECT s1.sid, 
                    s1.LoginName, 
                    POS.GrpNum, 
                    CONVERT(BIGINT, CONVERT(BINARY(4), REVERSE(CONVERT(BINARY(4), SUBSTRING(s1.sid, (POS.GrpNum * 4) + 5, 4))))) AS SidAuthGroup -- Reverse little endian to big endian
                FROM SID1 s1
                    INNER JOIN ( SELECT ROW_NUMBER() OVER(ORDER BY ( SELECT NULL )) AS GrpNum FROM sys.objects ) POS
                        ON (POS.GrpNum <= s1.SidAuthorityCount) ),
        FullSID
        AS (
            SELECT SID1.sid, 
                    SID1.LoginName, 
                    SID1.SidFirstAuthGroup,
                    BigSid = CONCAT('S-', 
                                    CONVERT(VARCHAR(3), SID1.SidVersion), 
                                    '-', 
                                    CONVERT(VARCHAR(15), SID1.SidAuthorityIdent), 
                                    STUFF( (SELECT '-' + 
                                                CONVERT(VARCHAR(10), AG.SidAuthGroup)
                                                FROM AG
                                                WHERE (AG.sid = SID1.sid)
                                                ORDER BY AG.GrpNum FOR XML PATH('')
                                            ), 1, 1, '-'))
                FROM SID1)

        SELECT	@@SERVERNAME AS ServerName,
                @Domain AS ServerDomain,
                FullSID.LoginName,
                FullSID.BigSid AS FullSid,
                FullSID.sid,
                CASE WHEN FullSID.SidFirstAuthGroup = 18 THEN	'SYSTEM'
                    WHEN FullSID.SidFirstAuthGroup = 21 THEN	'DOMAIN'
                    WHEN FullSID.SidFirstAuthGroup = 80 THEN	'SERVICE'
                    ELSE	'OTHER' END AS SidType ,
                CASE WHEN FullSID.SidFirstAuthGroup = 21 THEN LEFT(FullSID.BigSid, LEN(FullSID.BigSid) - CHARINDEX('-',REVERSE(FullSID.BigSid)))
                    ELSE '' END AS DomainSid
            FROM FullSID
            ORDER BY BigSid;

"@;

$logins = Invoke-Sqlcmd -ServerInstance 'PBG1SQL01V105' -Database 'master' -Query $sqlGetLogins


$SID = "S-1-9-3-1475729262-1267362322-375976881-1430926914"

$objSID = New-Object System.Security.Principal.SecurityIdentifier($SID)
$objUser = $objSID.Translate([System.Security.Principal.NTAccount])
Write-Host "Resolved user name: " $objUser.Value
$domain = $objUser.Value.Split('\')[0]
switch ($domain) {
    'FS'  { $DomainDNS = 'fs.local'}
    'QA'  { $DomainDNS = 'qa.fs'}
    'DEV' { $DomainDNS = 'dev.fs'}
    'MFG' { $DomainDNS = 'mfg.fs'}
    'NPQ' { $DomainDNS = 'npq.mfg'}
}
$sam = $objUser.Value.Split('\')[1]

$dc = Get-ADDomainController -DomainName $DomainDNS -Discover -NextClosestSite

try {
    $adu = Get-ADObject -Server $dc.HostName[0] -Filter "samaccountname -eq '$($sam)'" -Properties *
}
catch {
    $adu = Get-ADServiceAccount -Server $dc.HostName[0] -filter 'samAccountName -eq $sam' -Properties *
}

switch ($adu.ObjectClass) {
    "user" {
        $adInfo = [PSCustomObject]@{
            ObjectClass     = $adu.objectClass
            ObjectGUID      = $adu.objectGUID
            SID             = $adu.objectSid
            DisplayName     = $adu.DisplayName
            Description     = $adu.Description
            GivenName       = ''
            SurName         = ''
            Name            = $adu.name
            SamAccountName  = $adu.SamAccountName
            DistinguishedName = $adu.distinguishedName
            Enabled         = $adu.Enabled
            UserPrincipalName = ''
        }
    }

    "group" {
        $adInfo = [PSCustomObject]@{
            ObjectClass     = $adu.objectClass
            ObjectGUID      = $adu.objectGUID
            SID             = $adu.objectSid
            DisplayName     = $adu.DisplayName
            Description     = $adu.Description
            GivenName       = ''
            SurName         = ''
            Name            = $adu.name
            SamAccountName  = $adu.SamAccountName
            DistinguishedName = $adu.distinguishedName
            Enabled         = $adu.Enabled
            UserPrincipalName = ''
        }
    }

    "msDS-GroupManagedServiceAccount" {
        $adInfo = [PSCustomObject]@{
            ObjectClass     = $adu.objectClass
            ObjectGUID      = $adu.objectGUID
            SID             = $adu.objectSid
            DisplayName     = $adu.DisplayName
            Description     = $adu.Description
            GivenName       = ''
            SurName         = ''
            Name            = $adu.name
            SamAccountName  = $adu.SamAccountName
            DistinguishedName = $adu.distinguishedName
            Enabled         = $adu.Enabled
            UserPrincipalName = ''
        }
    }

    default {
        $adu2 = Get-ADServiceAccount -Filter "SamAccountName -eq '$($sam)'" -Server $dc.HostName[0]
    }
}

$testSid = (Invoke-SQLCmd -ServerInstance "PBG1SQL01V105.fs.local" -Query "SELECT sid AS testSid FROM sys.server_principals WHERE [name] = 'NT SERVICE\MSSQLSERVER'").testSid

# Get-ADDomain -Current LoggedOnUser

$AdItem = @{}
$AdLink = @()

#   Fetch the AD major attributes of the AD SQL login registered with the current SQL instance
#
$adg = ( $adu | Get-ADPrincipalGroupMembership )

$adg | Select-Object {
    $group = $_
    "Group: $($group.Name)"

    $recGroup = 
    if ( -not $AdItem.ContainsKey($group.SID)) {
        $script:AdItem[$group.SID] = [PSCustomObject]@{
            DistinguishedName = $group.distinguishedName
            GroupCategory = $group.GroupCategory
            Name   = $group.name
            ObjectClass = $group.objectClass
            ObjectGUID  = $group.objectGUID
            SamAccountName = $group.SamAccountName
            SID         = $group.SID
        }
    }

    $script:AdLink += [PSCustomObject]@{
        Group = $group.SID
        Member = $adu.SID
        ItemType = "MO"
    }
    
    ($group | Get-ADPrincipalGroupMembership) | Select-Object {
        #$_
        $recGM = 
        if ( -not $AdItem.ContainsKey($_.SID)) {
            $script:AdItem[$_.SID] = [PSCustomObject]@{
                DistinguishedName = $_.distinguishedName
                GroupCategory = $_.GroupCategory
                Name   = $_.name
                ObjectClass = $_.objectClass
                ObjectGUID  = $_.objectGUID
                SamAccountName = $_.SamAccountName
                SID         = $_.SID
            }
            $script:AdLink += [PSCustomObject]@{
                Group = $group.SID
                Member = $_.SID
                ItemType = "GM"
            }
        }
    }

} 

$a = 1

(((Get-ADTrust -Filter *).Name | where-object {$_ -notlike '*dmz*'}) | % {Get-ADDomainController -DomainName $_ -Discover -NextClosestSite} ) | %{ ( Get-ADDomain -Identity $_.Domain -Server $_.HostName[0]  ).DomainSID }