clear;

Import-Module E:\BU\DAG\MySqlCmds;

#  SETUP_TERA_DAG
#
#  This script will setup the Availability Groups environments for the TERA SQL clusters.
#
#  Author: F. LaForest
#  Revision History:
#  -  2017-06-28 - F. LaForest - Initial version
#  -  2017-08-11 - F. LaForest - Correct CREATE ENDPOINT, and CREATE CERTIFICATE
#  -  2017-08-15 - F. LaForest - Added $TERA_Loc to denote geogrphic location prefix
#  -  2019-02-13 - F. LaForest - Changed nameing scheme from TERA to S6
#
#  Arguments:
#
# Phase - 1) Prepare certificates and inbound logins, 
#         2) Load certificates, 
#         3) Prepare endpoints, 
#         4) Create local AG
#         9) Create the ODS-side AG
#        10) Create the distributed AG
#        
#        99) Run all phases
$Phase = 4
#  
# S6 Plant Designation numeric designation
#
$Plant_Type = 'CSS' # Either S6 or CSS
$Plant_XXXN = 'PGT7' # Plant designator - Location followed by a number
#
# MFG-side AG information
#
# List of SQL instances (FQDN)
$AG_MFG_Instances = "KLM1SQL20T102.mfg.fs,KLM2SQL20T102.mfg.fs"  # TERA0-1
#
# AG name fragment
$AG_MFG_Name = 'MesSqlSpc'
#
# AG Listener Name
$AG_MFG_ListenerFQDN = 'KLM1SQL20V102.mfg.fs'
#
# AG Listener IP
$AG_MFG_ListenerIP = '10.8.34.192'
#
# FS-side AG information
#
# List of SQL instances (FQDN)
$AG_FS_Instances = "KLM1SQL01T104.fs.local,KLM2SQL01T104.fs.local"  
#
# AG name fragment
#$AG_FS_Name = 'MesOds'
#
# AG Listener Name
$AG_FS_ListenerFQDN = 'KLM1SQL01V107.fs.local'
#
# AG Listener IP
$AG_FS_ListenerIP = '10.8.8.232'
# 
# =====================
# Port number of mirroring port
$MirroringPort = 5022
#
# File share UNC
#$ShareUNC_MFG = '\\KLM1SQL20T102.mfg.fs\Backup\';
#$ShareUNC_FS = '\\KLM1SQL01T104.fs.local\Backup\';
#
# Master key password
$MasterKeyPassword  = 'seismic-M9ZjZQU7dK78-ominous-abTtn9UqGSJD';

# Security
#
$Config_Login = 'sa';
$Config_Pswd = 'F1rstS0l@rT3R!-M9ZjZQU7dK78';

$Inbound_Pswd = 'M9ZjZQU7dK78-ambiguous-abTtn9UqGSJD$';

# IP Subnets Involved
#
$AG_FS_IPMask = ''
$AG_MFG_IPMask = ''


# =========================================================================================
#
# Resolve all Listener DNS names
#
# =========================================================================================
if ($AG_MFG_ListenerIP -eq "") {
    $AG_MFG_ListenerIP = (Resolve-DnsName -Name $AG_MFG_ListenerFQDN).IPAddress;
    "--- MFG Listener: $($AG_MFG_ListenerFQDN) [$($AG_MFG_ListenerIP)]";
}
else {
    "--- MFG Listener: $($AG_MFG_ListenerFQDN) [$($AG_MFG_ListenerIP)] -- IP OVERRIDE";
}
$AG_MFG_ListenerName = $AG_MFG_ListenerFQDN.Split('.')[0];


if ($AG_FS_ListenerIP -eq "") {
    $AG_FS_ListenerIP = (Resolve-DnsName -Name $AG_FS_ListenerFQDN).IPAddress;
    "---  FS Listener: $($AG_FS_ListenerFQDN) [$($AG_FS_ListenerIP)]";
}
else {
    "---  FS Listener: $($AG_FS_ListenerFQDN) [$($AG_FS_ListenerIP)] -- IP OVERRIDE";
}
$AG_FS_ListenerName = $AG_FS_ListenerFQDN.Split('.')[0];

#
# Get some environment info
#
#$DnsRoot = (Get-ADDomain).DNSRoot
#$LocPrefix = $TERA_Loc #

$AG_MFG = "AG_$($Plant_Type)_$($Plant_XXXN)_$($AG_Mfg_Name)" # AG name - MFG side
$AG_FS = "AG_$($Plant_Type)_$($Plant_XXXN)_$($AG_Mfg_Name)_$($LocPrefix)_ODS" # AG name - MFG side
$AG_DAG = "DAG_$($Plant_Type)_$($Plant_XXXN)_$($AG_Mfg_Name)_$($LocPrefix)_ODS" # Distributed AG Name

#
# Resolve server/instance names
#
$AG_Instances = @();
$AG_MFG_Instances.Split(",") | foreach { 
    $dnsInfo = (Resolve-DnsName -Name $_ | Where {$_.Type -eq 'A' -And $_.IPAddress -like '10.8*' -And $_.IPAddress -notlike '10.8.249*'} );
    #$dnsInfo;
    $sObj = New-Object -TypeName PSObject
    $sObj | Add-Member -MemberType NoteProperty -Name ServerFQDN -Value $dnsInfo.Name;
    $sObj | Add-Member -MemberType NoteProperty -Name IPAddress -Value $dnsInfo.IPAddress;
    $AG_Instances += $sObj
    }

$AG_FS_Instances.Split(",") | foreach { 
    $dnsInfo = (Resolve-DnsName -Name $_ | Where {$_.Type -eq 'A' -And $_.IPAddress -like '10.8*'} );
    $sObj = New-Object -TypeName PSObject
    $sObj | Add-Member -MemberType NoteProperty -Name ServerFQDN -Value $dnsInfo.Name;
    $sObj | Add-Member -MemberType NoteProperty -Name IPAddress -Value $dnsInfo.IPAddress;
    $AG_Instances += $sObj
    }
#$AG_Instances

#
# Verify connectivity to all hosts
#
$AG_Instances | foreach {
    "--- Testing connection to $($_.ServerFQDN) ..."
    $dbConn = New-SqlConnection -InstanceName $_.ServerFQDN -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
    [System.Data.DataRow]$iTab = Get-SqlQueryData -Connection $dbConn -SqlQuery "
        SELECT  CAST(SERVERPROPERTY('ServerName') AS VARCHAR(256)) AS [instName] ,
                COALESCE(CAST(SERVERPROPERTY('IsHadrEnabled') AS INT), 0) AS [instHadr] ,
                CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(128)) AS [instVers] , 
                CAST(DEFAULT_DOMAIN() AS VARCHAR(128)) AS [instDomain] " 
    $_ | Add-Member -MemberType NoteProperty -Name instName -Value $iTab.instName;
    $_ | Add-Member -MemberType NoteProperty -Name instHadr -Value $iTab.instHadr;
    $_ | Add-Member -MemberType NoteProperty -Name instVers -Value $iTab.instVers;
    $_ | Add-Member -MemberType NoteProperty -Name instDomain -Value $iTab.instDomain;

    if ($_.instName.IndexOf("\") -ge 0) {
        $_ | Add-Member -MemberType NoteProperty -Name fullInstanceName -Value "$($_.ServerFQDN)\" + (if ($_.instName -Match '(_.*?)(\\)(.*?$)') {$matches[2]});
    }
    else {
        $_ | Add-Member -MemberType NoteProperty -Name fullInstanceName -Value "$($_.ServerFQDN)" ;
    }

    [System.int32]$instPort = Get-SqlQueryValue -Connection $dbConn -SqlQuery "
        SELECT DISTINCT local_tcp_port
            FROM sys.dm_exec_connections 
            WHERE (local_tcp_port IS NOT NULL) AND (protocol_type = 'TSQL') " 
    $_ | Add-Member -MemberType NoteProperty -Name instPort -Value $instPort;

    if ($_.instName.length -gt 0) {
        "    ... SQL connection established"
        }
    else {
        "    ... Connot connect SQL session"
    }
    $dbConn.close();
}
$AG_Instances | FT

# =========================================================================================
#
# Phase 1:  Check each instance: 1-Master Key, 2-Certificate, 3-Save certificate in share
#
# =========================================================================================
if ($Phase -eq 1 -Or $Phase -eq 99) {

    $AG_Instances | foreach {
        $currentInstance = $_;

        if ($_.instDomain -ieq "MFG") { $ShareUNC = $ShareUNC_MFG; }
        else {$ShareUNC = $ShareUNC_FS; }

        "- Phase 1: Prepare $($_.FullInstanceName)"
        $dbConn = New-SqlConnection -InstanceName $_.fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd


    #   Get master key information
    #
        $iMKey = Get-SqlQueryData -Connection $dbConn -SqlQuery "
            SELECT COUNT(*) AS [hasMasterKey]
            FROM   sys.symmetric_keys
            WHERE  ( [name] = '##MS_DatabaseMasterKey##' ) "
        $hasMasterKey = $iMKey.hasMasterKey;

        if ($hasMasterKey -ne 1) {
            "--- Create Master Key: None detected"
            Submit-SqlNonQuery -Connection $dbConn -SqlQuery "CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$($MasterKeyPassword)';"
        }
        else {
            "--- Master Key detected: Skip create"
        }

    #   Get certificate information
    #
        $certName = "TERA$($TERA)_ODS_$($_.instName)_Cert"
        "--- Testing for certificate: $($certName)"

        $iMKey = Get-SqlQueryData -Connection $dbConn -SqlQuery "
            SELECT COUNT(*) AS [cRows]
            FROM   sys.certificates
            WHERE  ( [name] = '$($certName)' ); "

        if ($iMKey.cRows -ne 1) {
            "--- Create Certificate [$($certName)]: None detected"
            Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
                CREATE CERTIFICATE [$($certName)]	
                    WITH SUBJECT = 'Certificate [$($certName)] used for cross firewall traffic' ,
               	    START_DATE = '05/01/2017', EXPIRY_DATE = '12/31/2030';"
        }
        else {
            "--- Certificate [$($certName)] detected: Skip create"
        }

        # Check for a backup copy of the certificate for use by pther AG members
        #
        #$bkKey = Get-SqlQueryData -Connection $dbConn -SqlQuery "
        #    DECLARE @FileName VARCHAR(255) = '$($ShareUNC)$($certName).cer';
        #    DECLARE @File_Exists INT;
        #    EXEC master.dbo.xp_fileexist @FileName ,@File_Exists OUT;
        #    SELECT @File_Exists AS [BkExists]; ";
        #$bkExists = $bkKey.BkExists;
#
        #if ($bkExists -eq 1) {
        #    "--- Cert Backup detected: '$($ShareUNC)$($certName).cer'"
        #}
        #else {
        #    "--- Cert Backup created: '$($ShareUNC)$($certName).cer'"
        #    $bkKey = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
        #        BACKUP CERTIFICATE [$($certName)] TO FILE = '$($ShareUNC)$($certName).cer';"        
        #}

        # (Re)Establish the inbound connection logins and users
        #
        $AG_Instances | foreach {
            if ($currentInstance.instName -ine $_.instName) {
                "--- [$($_.instName)]: Creating inbound login/user"
                $ibSql = "             
                USE [master]
                If NOT EXISTS (SELECT loginname from master.dbo.syslogins 
                    WHERE name =  N'AG_In_$($_.instName)_Login')
                    CREATE LOGIN [AG_In_$($_.instName)_Login] WITH PASSWORD = '$($Inbound_Pswd)';  

                IF NOT EXISTS (SELECT name 
                                FROM [sys].[database_principals]
                                WHERE [type] = 'S' AND name = N'AG_In_$($_.instName)_User')
                    CREATE USER [AG_In_$($_.instName)_User] FOR LOGIN [AG_In_$($_.instName)_Login]; "
            $ibSql; 
            Submit-SqlNonQuery -Connection $dbConn -SqlQuery $ibSql;
            }
        }

        $dbConn.Close();
    }

    "--- Please copy created certificate backup files from the share one domain to the share in the other domain"
    "--- Then proceed to Phase 2"
}



# =========================================================================================
#
# Phase 2:  Certificate backups have been saved and copied cross-somain.
#           Now load the certs and prepare the mirroring endpoints
#
# =========================================================================================
if ($Phase -eq 2 -Or $Phase -eq 99) {
    
    #
    # Obtain the asn-encoded public key for each of the remote servers
    #
    $pubKeys = @{};
    $AG_Instances | foreach {
        $currentInstance = $_;  # Remember the instance being prepared
        $certName = "TERA$($TERA)_ODS_$($_.instName)_Cert"

        $dbConn = New-SqlConnection -InstanceName $_.fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
        $pbKey = Get-SqlQueryValue -Connection $dbConn -SqlQuery "
            SELECT CONVERT(VARCHAR(MAX),CERTENCODED(CERT_ID('$($certName)')),1); "
        $pubKeys["$($certName)"] = [string]$pbKey    
        }
    $pubKeys | FT
 
    #
    # Create (public key only) certificates for the remote servers
    #
    $AG_Instances | foreach {
        $currentInstance = $_;  # Remember the instance being prepared
        "--- Phase 2: $($_.instName)"

        # Share UNC
        if ($_.instDomain -ieq "MFG") { $ShareUNC = $ShareUNC_MFG; }
        else {$ShareUNC = $ShareUNC_FS; }

        $dbConn = New-SqlConnection -InstanceName $_.fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd

        # Load certs for other instances
        #
        $AG_Instances | foreach {
            $certName = "TERA$($TERA)_ODS_$($_.instName)_Cert"

            if ($currentInstance.instName -ieq $_.instName) {
                "--- [$($currentInstance.instName)]: Skipping own certificate load"
                }
            else {
               #$dcCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
               #         DROP CERTIFICATE [$($certName)]; "
               $iMKey = Get-SqlQueryData -Connection $dbConn -SqlQuery "
                    SELECT COUNT(*) AS [cRows]
                    FROM   sys.certificates
                    WHERE  ( [name] = '$($certName)' ); "

                if ($iMKey.cRows -eq 1) {
                    "--- [$($currentInstance.instName)]: Detected certificate [$($certName)] -- Do not load"
                }
                else {
                    "--- [$($currentInstance.instName)]: Loading certificate $($ShareUNC)$($certName).cer"
                    $asnCert = $pubKeys["$certName"];
                    $bkKey = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
                        CREATE CERTIFICATE [$($certName)]
                            AUTHORIZATION [AG_In_$($_.instName)_User]   
                            FROM BINARY = $($asnCert); "
#                    "--- [$($currentInstance.instName)]: Loading certificate $($ShareUNC)$($certName).cer"
#                    $bkKey = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
#                        CREATE CERTIFICATE [$($certName)]
#                            AUTHORIZATION [AG_In_$($_.instName)_User]   
#                            FROM FILE = '$($ShareUNC)$($certName).cer'; "
                }
            }

        }

    }

}


# =========================================================================================
#
# Phase 3:  Prepare the endpoints
#
# =========================================================================================
if ($Phase -eq 3 -Or $Phase -eq 99) {
    
    $AG_Instances | foreach {
        $currentInstance = $_;  # Remember the instance being prepared
        "--- Phase 3: $($_.instName)"

        $dbConn = New-SqlConnection -InstanceName $_.fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd

        # Test for endpoint existance and proper settings
        #
        $certName = "TERA$($TERA)_ODS_$($_.instName)_Cert"
        $MirrorPortName = "Hadr_Endpoint";
        # $ep_auth = 7
        # $ep_authtxt = "WINDOWS NEGOTIATE CERTIFICATE [$($certName)] "
        $ep_auth = 10
        $ep_authtxt = "CERTIFICATE [$($certName)] WINDOWS NEGOTIATE "

        $iEP = Get-SqlQueryData -Connection $dbConn -SqlQuery "
            SELECT	DME.[name] AS [epName],
                    TE.[port] AS [epPort], 
  		            DME.[state_desc] AS [epState], 
  		            DME.connection_auth AS [epConnAuth], -- 7 = Negotiate, Certificate or 10 = Certificate, Negotiate
  		            DME.[role] AS [epRole], -- 3=ALL
  		            DME.is_encryption_enabled AS [epEncrypted], -- 1 = enabled
  		            DME.encryption_algorithm AS [epEncAlg]-- 2 = AES
                FROM   sys.database_mirroring_endpoints DME
                        INNER JOIN sys.tcp_endpoints TE ON ( DME.endpoint_id = TE.endpoint_id )
                WHERE  ( DME.protocol_desc = 'TCP' ); "

        if ($iEP -ne $null) {
            $MirrorPortName = $iEP.epName  # Capture existing name

            # Endpoint exists - check settings
            if ($iEP.epPort -ne $MirroringPort) {
                "--- Mirroring Endpoint [$($MirrorPortName)] already exists: but on wrong port [$($iEP.$epPort)] -- Drop and Re-Create"
                $epCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
                    DROP ENDPOINT [$($MirrorPortName)]; "
                $epCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
                    CREATE ENDPOINT [$($MirrorPortName)]   STATE = STARTED 
                    	AS TCP ( LISTENER_PORT='$($MirroringPort)', LISTENER_IP = ALL )
                    	    FOR DATABASE_MIRRORING 
                               ( AUTHENTICATION = $($ep_authtxt),
                                 ENCRYPTION = REQUIRED ALGORITHM AES, ROLE=ALL); "
            }
            else {
                if ($iEP.epConnAuth -eq $ep_auth -and
                    $iEP.epRole -eq 3 -and
                    $iEP.epEncrypted -eq 1 -and
                    $iEP.epEncAlg -eq 2) {
                    "--- Mirroring Endpoint [$($MirrorPortName)] already exists: Properly setup"
                }
                else {
                    "--- Mirroring Endpoint [$($MirrorPortName)] already exists: Adjusting properties"
                    $epCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
                        ALTER ENDPOINT [$($MirrorPortName)]
                    	        FOR DATABASE_MIRRORING 
                                   ( AUTHENTICATION = $($ep_authtxt),
                                     ENCRYPTION = REQUIRED ALGORITHM AES, ROLE=ALL); "
                }
            }
        }

        # No endpoint
        else {
            "--- No Mirroring Endpoint: Creating [$($MirrorPortName)]..."
            $epCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
                CREATE ENDPOINT [$($MirrorPortName)]   STATE = STARTED 
                    AS TCP ( LISTENER_PORT=$($MirroringPort), LISTENER_IP = ALL )
                    	FOR DATABASE_MIRRORING 
                            ( AUTHENTICATION = $($ep_authtxt),
                                ENCRYPTION = REQUIRED ALGORITHM AES, ROLE=ALL); "
        }
        
        # Grant inbound login permission to the AG users
        #
        $AG_Instances | foreach {
            if ($currentInstance.instName -ieq $_.instName) {
                "--- [$($currentInstance.instName)]: Skipping own inbound grant"
            }
            else {
                "--- [$($currentInstance.instName)]: Grant inbound to [AG_In_$($_.instName)_Login]"
                $bkKey = Submit-SqlNonQuery -Connection $dbConn -SqlQuery "
                    GRANT CONNECT ON ENDPOINT::[$($MirrorPortName)] TO [AG_In_$($_.instName)_Login];  "
            }
        }

        $dbConn.Close();
    }
}


# =========================================================================================
#
# Phase 4:  Create the local Availability Groups
#
# =========================================================================================
if ($Phase -eq 4 -Or $Phase -eq 99) {
    
    "--- Phase 4: "
    # Spin through instances looking for existing AG's in MFG and FS realms
    #
    $agCntMfg = 0
    $agCntFs = 0

    $agMfgInst = @();
    $agFSInst = @();

    $AG_Instances | foreach {
        $currentInstance = $_;  # Remember the instance being prepared

        $dbConn = New-SqlConnection -InstanceName $_.fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
         
        # Test for AG existence
        #
        [System.int32]$agCnt = Get-SqlQueryValue -Connection $dbConn -SqlQuery "
            SELECT COUNT(*)
	            FROM sys.availability_groups AG
	            WHERE (AG.[name] = '$(If ($_.instDomain -ieq "MFG") { $AG_MFG } else { $AG_FS } )') " 

        If ($_.instDomain -ieq "MFG") { 
            $agMfgInst += $_ 
            if ($agCnt -eq 0) { $agCntMfg += 1} 
        } 
        else { 
            $agFsInst += $_ 
            if ($agCnt -eq 0) { $agCntFs += 1 } 
        } 

        $dbConn.close();
    }

    # Create the MFG side AG
    #
    "--- Creating MFG-side AG: (Primary) [$($AG_MFG)]"

    $dbConn = New-SqlConnection -InstanceName $agMfgInst[0].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
    $agSql = "CREATE AVAILABILITY GROUP [$($AG_MFG)]   
            FOR   
            REPLICA ON N'$($agMfgInst[0].instName)' 
            WITH (ENDPOINT_URL = N'TCP://$($agMfgInst[0].ServerFQDN):$($MirroringPort)',
                FAILOVER_MODE = AUTOMATIC,  
                AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,   
                BACKUP_PRIORITY = 50,   
                SECONDARY_ROLE(ALLOW_CONNECTIONS = NO),   
                SEEDING_MODE = AUTOMATIC), 
            N'$($agMfgInst[1].instName)' 
            WITH (ENDPOINT_URL = N'TCP://$($agMfgInst[1].ServerFQDN):$($MirroringPort)',   
                FAILOVER_MODE = AUTOMATIC,   
                AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,   
                BACKUP_PRIORITY = 50,   
                SECONDARY_ROLE(ALLOW_CONNECTIONS = NO),   
                SEEDING_MODE = AUTOMATIC);   "
    $agSql;
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();


    "--- Waiting for create to complete..."
    Start-Sleep -s 5

    "--- Joining MFG-side AG: (Secondary) [$($AG_MFG)]"

    $dbConn = New-SqlConnection -InstanceName $agMfgInst[1].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
    $agSql = "ALTER AVAILABILITY GROUP [$($AG_MFG)] JOIN"
    $agSql;
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql

    Start-Sleep -s 3
    $agSql = "ALTER AVAILABILITY GROUP [$($AG_MFG)] GRANT CREATE ANY DATABASE"
    $agSql;
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();

    
    "--- Adding AG Listener [$($AG_MFG)] - $($AG_MFG_ListenerName) [$($AG_MFG_ListenerIP)]"

    $dbConn = New-SqlConnection -InstanceName $agMfgInst[0].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
    $agSql = "ALTER AVAILABILITY GROUP [$($AG_MFG)]
        ADD LISTENER N'$($AG_MFG_ListenerName)' (
            WITH IP
            ((N'$($AG_MFG_ListenerIP)', N'255.255.254.0')), PORT=$($agMfgInst[0].instPort));"
    $agSql;
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();


    # Create the FS side AG
    #
    "--- Creating FS-side AG: (Primary) [$($AG_FS)]"
    $dbConn = New-SqlConnection -InstanceName $agFsInst[0].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
    $agSql = "CREATE AVAILABILITY GROUP [$($AG_FS)]   
            FOR   
            REPLICA ON N'$($agFsInst[0].instName)' 
            WITH (ENDPOINT_URL = N'TCP://$($agFsInst[0].ServerFQDN):$($MirroringPort)', 
                FAILOVER_MODE = AUTOMATIC,  
                AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,   
                BACKUP_PRIORITY = 50,   
                SECONDARY_ROLE(ALLOW_CONNECTIONS = NO),   
                SEEDING_MODE = AUTOMATIC), 
            N'$($agFsInst[1].instName)' 
            WITH (ENDPOINT_URL = N'TCP://$($agFsInst[1].ServerFQDN):$($MirroringPort)',   
                FAILOVER_MODE = AUTOMATIC,   
                AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,   
                BACKUP_PRIORITY = 50,   
                SECONDARY_ROLE(ALLOW_CONNECTIONS = NO),   
                SEEDING_MODE = AUTOMATIC); "
    $agSql;
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();

    "--- Waiting for create to complete..."
    Start-Sleep -s 5

    "--- Joining FS-side AG: (Secondary) [$($AG_FS)]"

    $dbConn = New-SqlConnection -InstanceName $agFsInst[1].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
    $agSql = "ALTER AVAILABILITY GROUP [$($AG_FS)] JOIN"
    $agSql;
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql

    Start-Sleep -s 3
    $agSql = "ALTER AVAILABILITY GROUP [$($AG_FS)] GRANT CREATE ANY DATABASE"
    $agSql;
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();

    "--- Adding FS AG Listener [$($AG_FS)] - $($AG_FS_ListenerName) [$($AG_FS_ListenerIP)]"

    $dbConn = New-SqlConnection -InstanceName $agFsInst[0].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
    $agSql = "ALTER AVAILABILITY GROUP [$($AG_FS)]
        ADD LISTENER N'$($AG_FS_ListenerName)' (
            WITH IP
            ((N'$($AG_FS_ListenerIP)', N'255.255.254.0')), PORT=$($agFsInst[0].instPort));"
    $agSql;
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();

    #
    # Create the Distributed AG
    #

    "--- Waiting for FS AG create to complete..."
    Start-Sleep -s 5

    "--- Creating Distributed AG: (Primary) [$($AG_DAG)]"
    $dbConn = New-SqlConnection -InstanceName $agMfgInst[0].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd

    $agSql = "CREATE AVAILABILITY GROUP [$($AG_DAG)]   
            WITH (DISTRIBUTED) AVAILABILITY GROUP ON
            N'$($AG_MFG)' 
            WITH (LISTENER_URL = N'TCP://$($AG_MFG_ListenerIP):$($MirroringPort)', 
                FAILOVER_MODE = MANUAL,  
                AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                SEEDING_MODE = AUTOMATIC),  
            N'$($AG_FS)' 
            WITH (LISTENER_URL = N'TCP://$($AG_FS_ListenerIP):$($MirroringPort)',   
                FAILOVER_MODE = MANUAL,   
                AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                SEEDING_MODE = AUTOMATIC);   "
    $agSql
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();


    "--- Waiting for DAG primary create to complete..."
    Start-Sleep -s 10

    "--- Joining Distributed AG: (Secondary) [$($AG_DAG)]"
    $dbConn = New-SqlConnection -InstanceName $agFSInst[0].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd

    $agSql = "ALTER AVAILABILITY GROUP [$($AG_DAG)]   
            JOIN AVAILABILITY GROUP ON
            N'$($AG_MFG)' 
            WITH (LISTENER_URL = N'TCP://$($AG_MFG_ListenerIP):$($MirroringPort)', 
                FAILOVER_MODE = MANUAL,  
                AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                SEEDING_MODE = AUTOMATIC),  
            N'$($AG_FS)' 
            WITH (LISTENER_URL = N'TCP://$($AG_FS_ListenerIP):$($MirroringPort)',   
                FAILOVER_MODE = MANUAL,   
                AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                SEEDING_MODE = AUTOMATIC);   "
    $agSql
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();


    "--- Waiting for DAG secondary create to complete..."
    Start-Sleep -s 10

    $dbConn = New-SqlConnection -InstanceName $agFSInst[0].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd

    "--- DAG: [$($AG_DAG)] -- GRANT CREATE ANY DATABASE "
    $agSql = "ALTER AVAILABILITY GROUP [$($AG_FS)] GRANT CREATE ANY DATABASE;   "
    $agSql
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
    $dbConn.close();
 
    Start-Sleep -s 5
 
    $dbConn = New-SqlConnection -InstanceName $agFSInst[0].fullInstanceName -Database "master" -TrustedConnection $false -LoginName $Config_Login -Password $Config_Pswd
    "--- DAG: [$($AG_F)] -- Make FS-side of distributed AG readable "
    $agSql = "   ALTER AVAILABILITY GROUP [$($AG_FS)]  
                     MODIFY REPLICA ON  N'$($agFsInst[0].instName)' WITH   
                    (SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL));"
    $agSql
    $agCmd = Submit-SqlNonQuery -Connection $dbConn -SqlQuery $agSql
 
    $dbConn.close();
}