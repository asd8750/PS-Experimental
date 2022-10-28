

Import-Module SqlServer;
#Import-Module FailoverClusters


#  Author: F. LaForest
#  Revision History:
#  -  2022-03-08 - F. LaForest - Intial version with code ripped from the Setup_TERA_DAG.ps1 script (circa. 2019-02-13)
#

function local:Get-CertificateInfo {
    param (
        [parameter(Mandatory=$true)]
        [string] 
        $ServerInstance        
    )
        
    #   Get the list of installed certificates used for mirroing plus user/login/permission info
    #
    begin {    
        $result = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database master -Query "
        SELECT	CT.[name] AS CertName,
                CT.certificate_id AS CertID,
                CT.pvt_key_encryption_type AS CertType,
                DBP.[name] AS DBUser,
                SVP.[name] AS LoginName,
                SVP.[type_desc] AS LoginType,
                IIF(SPM.[type]='CO', 1, 0) AS hasEPConnect,
                CT.cert_serial_number AS CertSerialNo,
                CONVERT(VARCHAR(MAX),CERTENCODED(CERT_ID(CT.[name])),1) AS PubCertEnc
                --,DBP.*,CT.*,SVP.*,SPM.*,DME.*
            FROM master.sys.certificates CT
                LEFT OUTER JOIN master.sys.database_principals DBP
                    ON (CT.principal_id = DBP.principal_id)  
                LEFT OUTER JOIN master.sys.server_principals SVP
                    ON (DBP.[sid] = SVP.[sid])
                LEFT OUTER JOIN (
                    master.sys.server_permissions SPM
                        INNER JOIN master.sys.database_mirroring_endpoints DME
                            ON (SPM.major_id = DME.endpoint_id) AND (DME.[type_desc] = 'DATABASE_MIRRORING')
                            )
                    ON (SVP.principal_id = SPM.grantee_principal_id) AND (SPM.class_desc = 'ENDPOINT')
                        --AND (CT.certificate_id = DME.certificate_id)
            WHERE (((CT.principal_id = 1) AND (CT.pvt_key_encryption_type = 'MK'))
                    OR ((CT.principal_id <> 1) AND (CT.pvt_key_encryption_type = 'NA')))
            ORDER BY CT.[name]";

        Write-Output $result
    }
}


function local:Get-EndpointInfo {
    param (
        [parameter(Mandatory=$true)]
        [string] 
        $ServerInstance        
    )

    begin {
        $instEPs = Invoke-Sqlcmd -ServerInstance $ServerInstance -Database master -Query "
                WITH MP AS (
                        SELECT	ISNULL(DME.[name],'NONE') AS [MPortName],
                                DME.endpoint_id AS MEndpointID,
                                TE.[port] AS [MPortNo], 
                                DME.[state_desc] AS [MPortState], 
                                REPLACE(DME.connection_auth_desc, ' ', '') AS [MPortConnAuth], -- 7 = Negotiate, Certificate or 10 = Certificate, Negotiate
                                DME.[role_desc] AS [MPortRole], -- 3=ALL
                                DME.is_encryption_enabled AS [MPortEncState], -- 1 = enabled
                                REPLACE(DME.encryption_algorithm_desc, ' ', '') AS [MPortEnc],  
                                DME.certificate_id,
                                CT.[name] AS CertName
                            FROM   sys.database_mirroring_endpoints DME
                                    INNER JOIN sys.tcp_endpoints TE ON ( DME.endpoint_id = TE.endpoint_id ) AND ( DME.protocol_desc = 'TCP' )
                                    LEFT OUTER JOIN sys.certificates CT ON (DME.certificate_id = CT.certificate_id)
                            WHERE (DME.type_desc = 'DATABASE_MIRRORING')
                            )
                    SELECT	SERVERPROPERTY('productversion') AS SqlVersion,
                            ISNULL(MP.MEndpointID, 0) AS MEndpointID,
                            ISNULL(MP.MPortName,'HADR_EndPoint') AS MPortName,
                            ISNULL(MP.MPortNo, 5022) AS MPortNo,
                            ISNULL(MP.MPortState, 'STARTED') AS MPortState,
                            ISNULL(MP.MPortConnAuth, 'CERTIFICATE,NEGOTIATE') AS MPortConnAuth,
                            ISNULL(MP.MPortRole, 'ALL') AS MPortRole,
                            ISNULL(MP.MPortEncState, 1) AS MPortEncState,
                            ISNULL(MP.MPortEnc, 'AES') AS MPortEnc,
                            ISNULL(MP.certificate_id, 0) AS MPortCertNo,
                            ISNULL(MP.CertName, '') AS MPortCertName
                        FROM (SELECT 1 AS ONE) J1
                            LEFT OUTER JOIN MP
                                ON (1=1);" ;  

        $local:EPInfo = $null
        foreach ($iEP in $instEPs) {
            $EPInfo = [PSCustomObject]@{
                Name = $iEP.MPortName
                EndpointID = $iEP.MEndpointID
                State = $iEP.MPortState
                PortNo = $iEP.MPortNo
                Authentication = ($iEP.MPortConnAuth -replace ' ','')
                Role = $iEP.MPortRole
                EncryptState = $iEP.MPortEncState
                Encryption = $iEP.MPortEnc
                CertNo = $iEP.MPortCertNo
                CertName = $iEP.MPortCertName
            }
        }
        Write-Output $EpInfo
    }
}


function local:Get-RandomPassword {
    param (
        [Parameter(Mandatory)]
        [int] $length
    )
    #$charSet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789{]+-[*=@:)}$^%;(_!&amp;#?>/|.'.ToCharArray()
    $charSet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'.ToCharArray()
    $rng = New-Object System.Security.Cryptography.RNGCryptoServiceProvider
    $bytes = New-Object byte[]($length)
 
    $rng.GetBytes($bytes)
 
    $result = New-Object char[]($length)
 
    for ($i = 0 ; $i -lt $length ; $i++) {
        $result[$i] = $charSet[$bytes[$i]%$charSet.Length]
    }
 
    return (-join $result)
}



# =========================================================================
function Deploy-FSPlantHadr  {
    <#
.SYNOPSIS

Create/configure the SQL HADR infrastructure required by FS plants

.DESCRIPTION

Create and deploy the SQL objects and settings needed for mirroring traffic between endpoints
to use credential authentication.

Version History
- 2022-08-22 - 1.0 - F.LaForest - Initial version

.PARAMETER InstanceList
Specifies an array of FQDN instance names wich will be the endpoints of this mirror/availability group constellation

.PARAMETER Command


.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

Return a PSObject with deployment configuration information

.EXAMPLE

PS> Get-FSDeploymentConfig -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(Mandatory=$true)]
        [string[]] 
        $InstanceList,
    
        [parameter(Mandatory=$true)]
        [string] 
        $Command,    

        [Parameter(Mandatory=$false)]
        [int32]
        $EndpointPort = 5022,

        [parameter(Mandatory=$false)]
        [string] 
        $MasterCertPassword = "seismic-M9ZjZQU7dK78-ominous-abTtn9UqGSJD",    

        [Parameter(Mandatory=$false)]
        [switch]
        $CrossFW,

        [parameter(Mandatory=$false)]
        [string] 
        $GeneratedOutputDir,

        [Parameter(Mandatory=$false)]
        [int32]
        $DebugLevel = 2
    )

    #   Loop through each supplied SQL instance name.. Querying each one to find the current state
    #
    begin {

        #   Do not Change  ####################################### Below ############################
        #
        # $MasterKeyPassword  = 'seismic-M9ZjZQU7dK78-ominous-abTtn9UqGSJD';      # Master key password
        #
        #   Do not Change  ####################################### Above ############################

        #  ***************** Change below to reflect you local standards *******************************************
        $My_MPortName = 'HADR_EndPoint'         # Defauly mirroring port name
        $My_MPortEnc  = 'AES'                   # Default mirroring encryption scheme
        $My_MPortAuth = 'CERTIFICATE,NEGOTIATE' # Default mirror port authenticaetion scheme
        $My_MCertNameBase = 'TERA_HA_<Server>_Cert' #Name pattern for new certificate - '<Server>' is replaced by server name
        $My_MCertPatt = "TERA%SQL[02][012][ST][0-9][014]_[_]Cert"
        #  ***************** Change above to reflect you local standards *******************************************

        $InstanceInfo = @();

        $instErrorCnt = 0

        Write-Verbose "-------------------------------------- Phase 1: Gather instance information ---------------------------------"

        foreach ($instName in $InstanceList) {
            try {
                Write-Verbose "-------------------- Instance: $($instName)"

                #   Get basic instance identification info
                #
                $oInfo = (Invoke-Sqlcmd -ServerInstance $instName -Query "
                SELECT  SERVERPROPERTY('productversion') AS SqlVersion,
                        CAST(SERVERPROPERTY('ServerName') AS VARCHAR(127)) AS ServerName,
                        CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS VARCHAR(256)) AS DataPath;" );
                $InstSqlVersion = $OInfo.SqlVersion;
                $InstServerName = $oInfo.ServerName;
                Write-Verbose "--- [$($instName)]  ServerName  $($InstServerName)"
                Write-Verbose "--- [$($instName)]  SQL Version $($InstSqlVersion)"

                #   Get master key information
                #
                $iMKey = Invoke-Sqlcmd -ServerInstance $instName -Database master -Query "
                SELECT COUNT(*) AS [hasMasterKey]
                    FROM   master.sys.symmetric_keys
                    WHERE  ( [name] = '##MS_DatabaseMasterKey##' ) "
                $InstHasMasterKey = $iMKey.hasMasterKey;

                if ($InstHasMasterKey -gt 0) {
                    Write-Verbose "--- [$($instName)]  Master Encryption Key exists"                    
                }
                else  {
                    Write-Verbose "--- [$($instName)]  Master Encryption Key missing!"                    
                }

                #   Get the list of installed certificates used for mirroing plus user/login/permission info
                #
                $instKeys = Get-CertificateInfo -ServerInstance $instName
                $epCert = $instKeys | Where-Object CertType -eq 'MK'          # ENCRYPTED_BY_MASTER_KEY
                $instRmtCerts = $instKeys | Where-Object CertType -eq 'NA'       # No private key

                if ($epCert) {
                    Write-Verbose "--- [$($instName)]  Outbound Certificate exists - [$($epCert.CertName)]"                                      
                } else {
                    Write-Verbose "--- [$($instName)]  Outbound Certificate Missing!"                    
                }
                
                if ($instRmtCerts) {
                    Write-Verbose "--- [$($instName)]  Inbound Certificates exist - $($instRmtCerts.Count) key(s)"                                      
                } else {
                    Write-Verbose "--- [$($instName)]  Inbound Certificates Missing!"                    
                }


                #   Get information on the Mirroring endpoint
                $EPInfo = Get-EndpointInfo -ServerInstance $instName

                if ($EPInfo.EndPointID -gt 0) {
                    Write-Verbose "--- [$($instName)]  Mirror Port - Port# $($EPInfo.PortNo)"
                    Write-Verbose "--- [$($instName)]  Mirror Port - Name - $($EPInfo.Name)"
                    Write-Verbose "--- [$($instName)]  Mirror Port - State - $($EPInfo.State)"
                    Write-Verbose "--- [$($instName)]  Mirror Port - Authentication - $($EPInfo.Authentication)"
                    Write-Verbose "--- [$($instName)]  Mirror Port - Role - $($EPInfo.Role)"
                    Write-Verbose "--- [$($instName)]  Mirror Port - Encrypt State - $($EPInfo.EncryptState)"
                    Write-Verbose "--- [$($instName)]  Mirror Port - Encrypt Method - $($EPInfo.Encryption)"
                    Write-Verbose "--- [$($instName)]  Mirror Port - Certificate ID - $($EPInfo.CertNo)"
                    Write-Verbose "--- [$($instName)]  Mirror Port - Certificate Name - $($EPInfo.CertName)"
                }
                else {
                    Write-Verbose "--- [$($instName)]  Mirror Port - NO ENDPOINT Defined"  
                }

                #   Create a custom object to hold all the discovered certificate and endpoint information
                #
                $InstStatus = [PSCustomObject]@{
                    ServerName = $InstServerName
                    ListedName = $instName
                    Version    = $InstSqlVersion
                    MasterKey  = ($InstHasMasterKey -gt 0)
                    Endpoint   = $EPInfo
                    EPCert     = $epCert
                    RemoteCerts = $instRmtCerts
                }
                $InstanceInfo += $InstStatus

            }
            catch {
                Write-Error $_.Exception.Message
                Write-Error $_.StackTrace
                $instErrorCnt += 1      # Add to the error count
                #Return            
            }
        }

        if ($instErrorCnt -gt 0) {
            Write-Error  "Multiple errors when gathering server configuration information -- Processing stopped"
            return
        }

        
        # ###############################################################################
        #   Create and adjust where required to bring the instances into compliance for use of certificates.
        #   Phase 2 - Create master key, inbound certificate and the mirroring endpoint
        #   Loop through each instance.
        # ###############################################################################

        Write-Verbose "----------------------- Phase 2: Create certificate auth elements on each node instance --------------"

        foreach ($InstInfo in $InstanceInfo) {

            #
            #   Step 1: Create the MASTER KEY ENCRYPTION is not already present
            #
            if (-not $InstInfo.MasterKey) {
                Write-Verbose "--- [$($InstInfo.ListedName)]  Creating Master Encryption Key"
                if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $InstInfo.ListedName -Query "CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$($MasterCertPassword)';"}
            }
            else {
                Write-Verbose "--- [$($InstInfo.ListedName)]  Master Encryption Key exists"
            }


            #
            #   Step 2: Create a full public/private certificate for mirroring/AG replicas for this node
            #
            
            if (-not $InstInfo.EpCert) {
                $NewCertName = $My_MCertNameBase -replace '\<Server\>',$InstInfo.ServerName.ToUpper().Replace('\','_')
                Write-Verbose "--- [$($InstInfo.ListedName)]  Inbound: Creating full certificate [$($NewCertName)] "
                $SqlCert = "
                    IF NOT EXISTS (SELECT [name] 
                                        FROM sys.certificates
                                        WHERE [name] = '$($NewCertName)')
                        CREATE CERTIFICATE [$($NewCertName)]	
                            WITH SUBJECT = 'Certificate [$($NewCertName)] used for secure mirroring traffic' ,
                            START_DATE = '01/01/2022', EXPIRY_DATE = '12/31/2031';
                    ELSE
                        PRINT '** Skip ** -- Cert: [$($NewCertName)] already exists on new server [$($InstInfo.ServerName)]';"
                Write-Verbose "--- [$($InstInfo.ListedName)]  $($SqlCert)"
                if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $InstInfo.ListedName -Database master -Query $SqlCert}
                $instKeys = Get-CertificateInfo -ServerInstance $InstInfo.ListedName
                $InstInfo.EPCert = $instKeys | Where-Object CertName -eq $NewCertName          # ENCRYPTED_BY_MASTER_KEY
                $MyCertName = $NewCertName
            }
            else {
                $MyCertName = $InstInfo.EPCert.CertName
                Write-Verbose "--- [$($InstInfo.ListedName)]  Public/private key certificate [$($InstInfo.EPCert.CertName)] exists "
            }


            
            #
            #   Step 3: Create the mirroring endpoint is not present.  If already present, ensure t is correctly configured
            #
            $sqlEP = ''
            $My_MPortName = 'HADR_EndPoint'         # Defauly mirroring port name
            $My_MPortEnc  = 'AES'                   # Default mirroring encryption scheme
            $My_MPortAuth = 'CERTIFICATE,NEGOTIATE' # Default mirror port authenticaetion scheme
            $EPName = ""
            if ($InstInfo.Endpoint.EndpointID -eq 0) {
                $EPName = $My_MPortName
                $sqlEP = "
                CREATE ENDPOINT [$($My_MPortName)]   STATE = STARTED 
                    AS TCP ( LISTENER_PORT=$($EndpointPort), LISTENER_IP = ALL )
                        FOR DATABASE_MIRRORING 
                            ( AUTHENTICATION = CERTIFICATE [$($MyCertName)] WINDOWS NEGOTIATE ,
                                ENCRYPTION = REQUIRED ALGORITHM AES, ROLE=ALL); "
                Write-Verbose "--- [$($InstInfo.ListedName)] Created ENDPOINT - [$($My_MPortName)]";
            } 
            else {
                if ($InstInfo.Endpoint.Authentication -ine $My_MPortAuth) {
                    $EPName = $InstInfo.Endpoint.Name
                    $SqlEP = "
                        ALTER ENDPOINT [$($InstInfo.Endpoint.Name)]
                            FOR DATABASE_MIRRORING 
                            ( AUTHENTICATION = CERTIFICATE [$($MyCertName)] WINDOWS NEGOTIATE ,
                                ENCRYPTION = REQUIRED ALGORITHM AES, ROLE=ALL);"
                    Write-Verbose "--- [$($InstInfo.ListedName)] Altered ENDPOINT - [$($InstInfo.Endpoint.Name)]";
                } 
            }
            if ($sqlEP.Length -gt 2) {
                Write-Verbose "--- [$($InstInfo.ListedName)]  $($sqlEP)"
                if ($DebugLevel -le 1) {$cmdResults = (Invoke-Sqlcmd -ServerInstance $InstInfo.ListedName -Database master -Query $sqlEP)}
            }
            else {
                Write-Verbose "--- [$($InstInfo.ListedName)] No changes to ENDPOINT"
            }
            
        }
        
        # ###############################################################################
        #  
        #   Phase 3
        #   Iterate to each server and ensure each has the public key needed to connect to each of the other 
        #   servers in our HA/mirroring constellation.   Then create a login/user on each server 
        #   that will be associated with each public key.
        # ###############################################################################

        Write-Verbose "----------------------- Phase 3: Interate to each server and create public keys if not present --------------"

        foreach ($EPInst in $InstanceInfo) {

            foreach ($RmtInst in $InstanceInfo) {
                $RmtInstName2 = $RmtInst.ServerName.ToUpper().Replace('\','_') 
                if ($EPInst.ServerName -eq $RmtInst.ServerName) {continue}
                $rmtCert = ($EPInst.RemoteCerts | Where-Object { $_.CertName -eq $RmtInst.EPCert.CertName} )
                if ($rmtCert -and ($rmtCert.CertSerialNo -eq $RmtInst.EPCert.CertSerialNo)){
                    Write-Verbose "--- [$($EPInst.ListedName)] - Rmt: [$($RmtInst.ListedName)] contains certificate [$($RmtInst.EPCert.CertName)]"
                    if ($rmtCert.hasEPConnect) {
                        Write-Verbose "--- [$($EPInst.ListedName)] - Rmt: [$($RmtInst.ListedName)] has Connect permission via [$($rmtCert.LoginName)]"                   
                    }
                    Write-Verbose "--- [$($EPInst.ListedName)] - Cert Sigs: EP: [$($rmtCert.CertSerialNo)] -- Rmt: [$($RmtInst.EPCert.CertSerialNo)]"                                      
                }
                else {
                    Write-Verbose "--- [$($EPInst.ListedName)] - Rmt: [$($RmtInst.ListedName)] Needs certificate [$($RmtInst.EPCert.CertName)]"
                    $sqlPrincipals = "
                        USE [master]
                        IF EXISTS ( SELECT [name] FROM sys.certificates WHERE ([name] = '$($RmtInst.EPCert.CertName)'))
                            DROP CERTIFICATE [$($RmtInst.EPCert.CertName)];
        
                        IF NOT EXISTS (SELECT name 
                            FROM [sys].[database_principals]
                            WHERE [type] = 'S' AND name = N'AG_In_$($RmtInstName2)_User')
                            DROP USER [AG_In_$($RmtInstName2)_User];
                        If EXISTS (SELECT loginname from master.dbo.syslogins 
                                    WHERE name =  N'AG_In_$($RmtInstName2)_Login')
                            DROP LOGIN [AG_In_$($RmtInstName2)_Login];  

                        If NOT EXISTS (SELECT loginname from master.dbo.syslogins 
                            WHERE name =  N'AG_In_$($RmtInstName2)_Login')
                            CREATE LOGIN [AG_In_$($RmtInstName2)_Login] WITH PASSWORD = '$(Get-RandomPassword 30)';  
        
                        IF NOT EXISTS (SELECT name 
                                        FROM [sys].[database_principals]
                                        WHERE [type] = 'S' AND name = N'AG_In_$($RmtInstName2)_User')
                            CREATE USER [AG_In_$($RmtInstName2)_User] FOR LOGIN [AG_In_$($RmtInstName2)_Login];
                            
                        CREATE CERTIFICATE [$($RmtInst.EPCert.CertName)]
                            AUTHORIZATION [AG_In_$($RmtInstName2)_User]   
                            FROM BINARY = $($RmtInst.EPCert.PubCertEnc); 
                            
                        GRANT CONNECT ON ENDPOINT::[$($EPInst.Endpoint.Name)] TO [AG_In_$($RmtInstName2)_Login];  
                        ";
                    Write-Verbose "--- [$($EPInst.ListedName)] - Rmt: [$($RmtInst.ListedName)] SQL -->> $($sqlPrincipals)"
                    if ($DebugLevel -le 1) {$cmdResults = (Invoke-Sqlcmd -ServerInstance $EPInst.ListedName -Database master -Query $sqlPrincipals)}

                }
            }

        }

    }

}

#   Test instance lists
#
$InstanceList_Test1 = @("PBG1SQL01S221", "PBG1SQL01T011.fs.local", "PBG2SQL01T011.fs.local")
$InstanceList_Test2 = @("PBG1SQL01T011.fs.local", "PBG2SQL01T011.fs.local")
$InstanceList_PGT1_PDR11 = @("PBG1SQL20T140.mfg.fs", "PBG2SQL20T140.mfg.fs","PBG1SQL20T144.mfg.fs", "PBG2SQL20T144.mfg.fs")
$InstanceList_PGT1_PDR12 = @("PBG1SQL20T145.mfg.fs", "PBG2SQL20T145.mfg.fs")
$InstanceList_PGT1_ODS = @("PBG1SQL01T114.fs.local", "PBG2SQL01T114.fs.local")
$InstanceList_PGT1_ODS_SqlProd= @("PBG1SQL01T114.fs.local", "PBG2SQL01T114.fs.local","PBG1SQL20T111.mfg.fs","PBG2SQL20T111.mfg.fs")
#
$InstanceList_PGT3_ODS_SqlProd= @("PBG1SQL01T314.fs.local","PBG1SQL20T301.mfg.fs","PBG2SQL20T301.mfg.fs")
$InstanceList_PGT3_ODS_SqlSpc= @("PBG1SQL01T314.fs.local","PBG1SQL20T302.mfg.fs","PBG2SQL20T302.mfg.fs")
$InstanceList_PGT3_ODS_SqlMisc= @("PBG1SQL01T314.fs.local","PBG1SQL20T303.mfg.fs","PBG2SQL20T303.mfg.fs")
$InstanceList_PGT3_ODS_SqlPrcData= @("PBG1SQL01T314.fs.local","PBG1SQL20T304.mfg.fs","PBG2SQL20T304.mfg.fs")
#    
$InstanceList_SAP_S820 = @("EDR1SQL01S820.fs.local\EP", "EDR1SQL01S821.fs.local\EP")
# 
#    

if (-not $FSDeploymentIsLoading) {
    Deploy-MirrorEndPointCredAuth -InstanceList $InstanceList_SAP_S820 -Command VALIDATE -Verbose -DebugLevel 2
}
