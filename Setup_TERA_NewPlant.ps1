
Import-Module SqlServer;
Import-Module FailoverClusters

function Setup-TERA-NewPlant  {
    <#
.SYNOPSIS

Connect to the local domain master server and retrieve the deployment configuration information

.DESCRIPTION

Return a PSObject with deployment configuration information



.PARAMETER FullInstanceName
Specifies the Fully qualified SQL Server instance name of the domain master job server 

.PARAMETER Database
Specifies the database name of the deployment control database.  
Default is 'FSDeployDB'

.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

Return a PSObject with deployment configuration information

.EXAMPLE

PS> Get-FSDeploymentConfig -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $OldFullInstanceName,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $NewFullInstanceName,

        [Parameter(Mandatory=$false)]
        [switch]
        $CrossFW,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $GeneratedOutputDir,

        [Parameter(Mandatory=$false)]
        [int32]
        $Phase = 99,

        [Parameter(Mandatory=$false)]
        [int32]
        $DebugLevel = 2
    )


    #  SETUP_TERA_Replacement
    #
    #  This script will configure the new cluster SQL node that will replace an older existing cluster member node
    #
    #  Author: F. LaForest
    #  Revision History:
    #  -  2021-03-25 - F. LaForest - Intial version with code ripped from the Setup_TERA_DAG.ps1 script (circa. 2019-02-13)
    #
    #  Arguments:
    #
    # $Phase - 1) Prepare certificates, inbound logins, mirroring endpoint and endpoint permissions 
    #          2) Configure the remote AG partnet nodes with information on the new replacement node
    #          3) Copy server/instance level settings and objects from the old server to the new server
    #          4) Create local AG
    #          5) On new server, GRANT CREATE ANY DATABASE on each joined AG
    #          6) Create the distributed AGs
    #          9) Create the ODS-side AG
    #         10) Create the distributed AG
    #        
    #         99) Run all phases


    #   
    #   Input parameters
    #
    $SvrOld_Name = $OldFullInstanceName   # Server to be replaced
    $SvrNew_Name = $NewFullInstanceName    # New replacement server
    #
    #$Debug = 1                              # 0-commit (no extra debugging text)
                                            # 1-commit (Output debug text)
                                            # 2-No Commit (Output debug text)

    Write-verbose "--- Debug: Debug level set to '$($DebugLevel)' "

    $DateStamp = Get-Date -Format "yyyyMMdd_HHmmss"

                                        
    #   Derived variables
    #
    $OldInstName = ($SvrOld_Name.Split('.'))[0]    # Get the the old instance name
    $NewInstName = ($SvrNew_Name.Split('.'))[0]    # Get the the new instance name
    $PlantLoc = $NewInstName.Substring(0,3)         # Plant location
    $PlantNumber = $NewInstName.Substring(10,1)     # Plant #

    switch ($PlantLoc) {
        "PBG" {
            $PlantCode = "PGT" + $PlantNumber
            if ($PlantCode -ieq "PGT7") 
                {
                    $PlantCode = "PBG3"
                    $PlantNumber = '3';
                }
        }
        "KLM" {
            $PlantCode = "KMT" + $PlantNumber
        }
        "DMT" {
            $PlantCode = "DMT" + $PlantNumber
        }
    }
 
    $PlantCertMask = "TERA%SQL__T$($PlantNumber)[014]_[_]Cert"
    Write-verbose "--- Debug: Plant Code: '$($PlantCode)' "
    Write-verbose "--- Debug: Plant Location: '$($PlantLoc)' "

    #
    #   Gather needed information about the cluster config from the cluster associated with the old server
    #
    #   Get the cluster name
    #
    $ClsInfo = Get-Cluster -Name $SvrNew_Name   # Get information about the cluster containing the new server
    $ClsInfoOld = Get-Cluster -Name $SvrOld_Name   # Get information about the cluster containing the old server
    $ClsSame = ($ClsInfo.Name -eq $ClsInfoOld.Name)
    $ClsDomain = $ClsInfo.Domain                # Domain name
    $ClsName = $ClsInfo.Name                    # Cluster name

    if ($ClsDomain -ieq 'fs.local') { $FirewallSide = "Corp"}   
    elseif ($ClsDomain -ieq 'qa.fs') { $FirewallSide = "Corp"}
    elseif ($ClsDomain -ieq 'dev.fs') { $FirewallSide = "Corp"}
    elseif ($ClsDomain -ieq 'mfg.fs') { $FirewallSide = "MFG"}
    elseif ($ClsDomain -ieq 'npq.mfg') { $FirewallSide = "MFG"}
    else {
        Write-Error  "Domain: $($ClsDomain) -- Not sure which side of domain the server is located"
        Return
    }

    #   Script output file path names
    #
    $GeneratedSqlScriptFileBase = Join-Path $GeneratedOutputDir "GeneratedSQL_$($PlantCode)_"
    #$GeneratedSqlScriptFileBase = Join-Path $GeneratedOutputDir "GeneratedSQL_$($OldInstName)_$($NewInstName)_$($DAteStamp)_"
    $GeneratedSql_RemoteCorp = $GeneratedSqlScriptFileBase + "Corp_Remote_<ServerName>.sql"
    $GeneratedSql_RemoteMfg = $GeneratedSqlScriptFileBase + "Mfg_$($ClsName)_Remote_<ServerName>.sql"
    $GeneratedSql_AGCreate = $GeneratedSqlScriptFileBase + "AGCreate_$($ClsName).sql"
    $GeneratedSql_AGJoin = $GeneratedSqlScriptFileBase + "AGJoin_$($ClsName).sql"    
    $GeneratedSql_AGDAG = $GeneratedSqlScriptFileBase + "AGDAG_$($ClsName).sql"

    #   Do not Change  ####################################### Below ############################
    #
    $MasterKeyPassword  = 'seismic-M9ZjZQU7dK78-ominous-abTtn9UqGSJD';      # Master key password
    $Inbound_Pswd = 'M9ZjZQU7dK78-ambiguous-abTtn9UqGSJD$';                 # Special login
    #
    #   Do not Change  ####################################### Above ############################

    $OldInstSQL = [System.Collections.ArrayList]::new();  # SQL to Execute on the new server
    $NewInstSQL = [System.Collections.ArrayList]::new();  # SQL to Execute on the new server

    "-- Generated SQL commands for remote AG nodes" > $GeneratedSql_RemoteNodes


    # Phase 0: Validate the given instances do exist!
    #
    #   Test connect to the old (source) server
    #
    try {
            $oInfo = (Invoke-Sqlcmd -ServerInstance $SvrOld_Name -Query "
                SELECT  SERVERPROPERTY('productversion') AS SqlVersion,
                        CAST(SERVERPROPERTY('ServerName') AS VARCHAR(127)) AS ServerName,
                        CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS VARCHAR(256)) AS DataPath;" );
            $OldSqlVersion = $OInfo.SqlVersion;
            $OldInstanceName = $oInfo.ServerName;
            $OldDataPath = $oInfo.DataPath;
            Write-Verbose "--- Old:  SQL - Data Path '$($OldDataPath)'"
            Write-Verbose "--- Old:  SQL - Version $($OldSqlVersion)"

    }
    catch {
        Write-Error $_.Exception.Message
        Write-Error $_.StackTrace
        Return
    }

    #   Test connect the the new (replacement)server
    #
    try {
        $nInfo = (Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query "
            SELECT  SERVERPROPERTY('productversion') AS SqlVersion,
                    CAST(SERVERPROPERTY('ServerName') AS VARCHAR(127)) AS ServerName,
                    CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS VARCHAR(256)) AS DataPath;" );
        $NewSqlVersion = $nInfo.SqlVersion;
        $NewInstanceName = $nInfo.ServerName;
        $NewDataPath = $nInfo.DataPath;
        Write-Verbose "--- New:  SQL - Data Path '$($NewDataPath)'"
        if ($NewSqlVersion.Split('.')[0] -eq '15') {
            Write-Verbose "--- New:  SQL - Version $($NewSqlVersion) - Correct!! SQL 2019"
        }
        else {
            Write-Error "--- New:  SQL - Incorrect Version - $(NewSqlVersion)"
        }
    }
    catch {
        Write-Error "Cannot connect to '$($SvrNew_Name)'"
        Return 
    }

    #
    #   Gather needed information about the cluster config from the cluster associated with the old server
    #
    #   Get the cluster name
    #
    $ClsInfo = Get-Cluster -Name $SvrOld_Name   # Get information about the cluster containing the old server
    $ClsDomain = $ClsInfo.Domain                # Domain name
    $ClsName = $ClsInfo.Name                    # Cluster name

    if ($ClsDomain -ieq 'fs.local') { $FirewallSide = "Corp"}   
    elseif ($ClsDomain -ieq 'qa.fs') { $FirewallSide = "Corp"}
    elseif ($ClsDomain -ieq 'dev.fs') { $FirewallSide = "Corp"}
    elseif ($ClsDomain -ieq 'mfg.fs') { $FirewallSide = "MFG"}
    elseif ($ClsDomain -ieq 'npq.mfg') { $FirewallSide = "MFG"}
    else {
        Write-Error  "Domain: $($ClsDomain) -- Not sure which side of domain the server is located"
        Return
    }



    #   Get the list of SQL Server Availability Groups regeistered with the cluster
    #
    Write-Verbose "------ Old:  Get the list of Availability Groups registered with the cluster: $($ClsName)"
    $ClsAGs = @()
    $cName = @{label="AGName";expression={$_.OwnerGroup.Name}}
    $cPrimaryNode = @{label="AGPrimaryNode";expression={$_.OwnerNode.Name}}
    $cState = @{label="AGState";expression={$_.State}}
    $ClsAGs = ($ClsInfo | Get-ClusterResource) | Where-Object ResourceType -eq 'SQL Server Availability Group' | Select-Object $cName, $cPrimaryNode, $cState

    #   Get the list of cluster member nodes (servers) connected to the cluster
    #
    Write-Verbose "------ Old:  Get the list of member nodes registered with the cluster: $($ClsName)"
    $ClsNodes = @()
    $cNode = @{label="NodeName";expression={$_.Nodename.ToUpper()}}
    $ClsNodes = $ClsInfo | Get-ClusterNode | Select-Object $cNode, State
    
    #   Test if bith old and new server are in the same cluster
    #
    if ($ClsNodes.Where({$_.NodeName -eq $NewInstName}).Count -eq 1) {
        Write-Verbose "------ New:  New server '$($NewInstName)' is in the same cluster as the old server: $($ClsName)" 
        #Return;
    }
    else {
        Write-Verbose "------ New:  New server '$($NewInstName)' is NOT in the same cluster as the old server: $($ClsName)"
        #Return
    }

    # ###############################################################################################
    #
    #   M A S T E R   E N C R Y P T I O N   K E Y
    #
    #   A master certificate must exist on each instance.  This is required to generate and use 
    #   digital certificates used to authenticate servers to one another for data replication (mirroring)
    #   communication.  Since we are communicating over a firewall, we cannot always depend on AD
    #   authentication.  Each server generates a unique public/private key certificate that will be tied to the
    #   login used by other servers to replicate data to this server.  The public key is copied to each
    #   server participating in AG replication traffic to this server.  A login is created in the remote servers
    #   with the same name as the one on this server.  The connecting server will pass the public key to this server 
    #   and if it is authenticated by the private key on this server.  The mirroring connection is authorized.
    # 
    # ###############################################################################################

    #   Get master key information
    #
    $iMKey = Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query "
        SELECT COUNT(*) AS [hasMasterKey]
            FROM   sys.symmetric_keys
            WHERE  ( [name] = '##MS_DatabaseMasterKey##' ) "
    $NewSrv_hasMasterKey = $iMKey.hasMasterKey;

    if ($NewSrv_hasMasterKey -ne 1) {
        "--- Create Master Key: None detected"
        Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query "CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$($MasterKeyPassword)';"
        $NewSrv_hasMasterKey = 1
    }
    else {
        "--- Master Key detected: Skip create"
    }


    #   Old>  Is there a master key and mirror port?
    #
    Write-verbose "------ Old:  Obtain current information about old system mirroring port,  '$($OldInstName)" ;
    $oInfo = Invoke-Sqlcmd -ServerInstance $SvrOld_Name -Query "
        WITH MP AS (
                SELECT	ISNULL(DME.[name],'NONE') AS [MPortName],
                        TE.[port] AS [MPortNo], 
                        DME.[state_desc] AS [MPortState], 
                        DME.connection_auth_desc AS [MPortConnAuth], -- 7 = Negotiate, Certificate or 10 = Certificate, Negotiate
                        DME.[role_desc] AS [MPortRole], -- 3=ALL
                        DME.is_encryption_enabled AS [MPortEncState], -- 1 = enabled
                        DME.encryption_algorithm_desc AS [MPortEnc],  -- 2 = AES
                        DME.certificate_id,
                        CT.[name] AS CertName
                    FROM   sys.database_mirroring_endpoints DME
                            INNER JOIN sys.tcp_endpoints TE ON ( DME.endpoint_id = TE.endpoint_id ) AND ( DME.protocol_desc = 'TCP' )
                            LEFT OUTER JOIN sys.certificates CT ON (DME.certificate_id = CT.certificate_id)
                    )
            SELECT	SERVERPROPERTY('productversion') AS SqlVersion,
                    ISNULL(MP.MPortName,'NONE') AS MPortName,
                    ISNULL(MP.MPortNo, 0) AS MPortNo,
                    MP.MPortState,
                    ISNULL(MP.MPortConnAuth, '') AS MPortConnAuth,
                    MP.MPortRole,
                    MP.MPortEncState,
                    MP.MPortEnc,
                    ISNULL(MP.certificate_id, 0) AS MPortCertNo,
                    ISNULL(MP.CertName, 'NONE') AS MPortCertName
                FROM MP; " ;   

   # Write-verbose "--- Old:  SQL - Master Cert exists"
    Write-verbose "--- Old:  SQL - Version - $($oInfo.SqlVersion)"
    Write-verbose "--- Old:  Mirror Port - TCP $($oInfo.MPortNo)"
    Write-verbose "--- Old:  Mirror Port - Name - $($oInfo.MPortName)"
    Write-verbose "--- Old:  Mirror Port - State - $($oInfo.MPortState)"
    Write-verbose "--- Old:  Mirror Port - Authentication - $($oInfo.MPortConnAuth)"
    Write-verbose "--- Old:  Mirror Port - Role - $($oInfo.MPortRole)"
    Write-verbose "--- Old:  Mirror Port - Encrypt State - $($oInfo.MPortEncState)"
    Write-verbose "--- Old:  Mirror Port - Encrypt Method - $($oInfo.MPortEnc)"
    Write-verbose "--- Old:  Mirror Port - Certificate ID - $($oInfo.MPortCertNo)"
    Write-verbose "--- Old:  Mirror Port - Certificate Name - $($oInfo.MPortCertName)"

    #   Setup default mirror parameters

    $MPortName     = $oInfo.MPortName
    $MPortNo       = $oInfo.MPortNo
    $MPortState    = $oInfo.MPortState
    $MPortConnAuth = $oInfo.MPortConnAuth
    $MPortRole     = $oInfo.MPortRole
    $MPortEncState = $oInfo.MPortEncState
    $MPortEnc      = $oInfo.MPortEnc



    #   New>  Is there a master key?
    #
    Write-verbose "------ New:  Obtain current information about new system, '$($NewInstName)'"
    $nInfo = Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query "
        WITH MP AS (
                SELECT	ISNULL(DME.[name],'NONE') AS [MPortName],
                        TE.[port] AS [MPortNo], 
                        DME.[state_desc] AS [MPortState], 
                        DME.connection_auth_desc AS [MPortConnAuth], -- 7 = Negotiate, Certificate or 10 = Certificate, Negotiate
                        DME.[role_desc] AS [MPortRole], -- 3=ALL
                        DME.is_encryption_enabled AS [MPortEncState], -- 1 = enabled
                        DME.encryption_algorithm_desc AS [MPortEnc],  -- 2 = AES
                        DME.certificate_id,
                        CT.[name] AS CertName
                    FROM   sys.database_mirroring_endpoints DME
                            INNER JOIN sys.tcp_endpoints TE ON ( DME.endpoint_id = TE.endpoint_id ) AND ( DME.protocol_desc = 'TCP' )
                            LEFT OUTER JOIN sys.certificates CT ON (DME.certificate_id = CT.certificate_id)
                    )
            SELECT	SERVERPROPERTY('productversion') AS SqlVersion,
                    ISNULL(MP.MPortName,'NONE') AS MPortName,
                    ISNULL(MP.MPortNo, 0) AS MPortNo,
                    MP.MPortState,
                    ISNULL(MP.MPortConnAuth, '') AS MPortConnAuth,
                    MP.MPortRole,
                    MP.MPortEncState,
                    MP.MPortEnc,
                    ISNULL(MP.certificate_id, 0) AS MPortCertNo,
                    ISNULL(MP.CertName, 'NONE') AS MPortCertName
                FROM (SELECT 1 AS One) one OUTER APPLY MP ; " ;
      
    Write-verbose "--- New:  SQL - Version - $($nInfo.SqlVersion)"
    if ($nInfo.MPortName -ine 'NONE') {
        Write-verbose "--- New:  Mirror Port - $($nInfo.MPortNo)"
        Write-verbose "--- New:  Mirror Port - Name - $($nInfo.MPortName)"
        $MPortName = $nInfo.MPortName
        Write-verbose "--- New:  Mirror Port - State - $($nInfo.MPortState)"
        Write-verbose "--- New:  Mirror Port - Authentication - $($nInfo.MPortConnAuth)"
        Write-verbose "--- New:  Mirror Port - Role - $($nInfo.MPortRole)"
        Write-verbose "--- New:  Mirror Port - Encrypt State - $($nInfo.MPortEncState)"
        Write-verbose "--- New:  Mirror Port - Encrypt Method - $($nInfo.MPortEnc)"
        Write-verbose "--- New:  Mirror Port - Certificate ID - $($nInfo.MPortCertNo)"
        Write-verbose "--- New:  Mirror Port - Certificate Name - $($nInfo.MPortCertName)"
    }
    else {
        Write-verbose "--- New:  Mirror Port - None!! "
    }


    # ###############################################################################################
    #
    #   P H A S E ---  1
    # 
    # ###############################################################################################

    if ($Phase -eq 1 -or $Phase -eq 99) {
            
        # ###############################################################################################
        #
        #   C R E A T E   O U R   F U L L   C E R T I F I C A T E
        # 
        # ###############################################################################################

        #   New> If the new instance does not have a master encryption key, create it
        # 
        if ($NewSrv_hasMasterKey -ne 1) {
            Write-verbose "--- New:  SQL - Master Cert does NOT exist - Create it"

            $qry = "CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$($MasterKeyPassword)';"
            Write-Verbose " TSQL>> $($qry)"
            [void]$NewInstSQL.Add($qry);
            if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query $qry}
            Write-verbose "--- New:  SQL - Master Cert CREATED"
        }
        else {
            Write-verbose "--- New:  SQL - Master Cert exists"
        }
        #   Old> Get the list of "TERA" certificates
        #
        Write-verbose "--- Old:  Obtaining the list of 'TERA' certificates"
        $oCerts = Invoke-Sqlcmd -ServerInstance $SvrOld_Name -Query "
            SELECT  CT.[name] AS CertName,
                    CT.certificate_id AS CertID,
                    CT.[pvt_key_encryption_type] AS CertType,
                    CONVERT(VARCHAR(MAX),CERTENCODED(CERT_ID([name])), 1) AS CertBin
                FROM sys.certificates CT
                WHERE ([name] LIKE '$($PlantCertMask)') AND ([name] NOT LIKE '%$($NewInstName)%')
                ORDER BY CertName; " 

        if ($oCert) {
            $TERA = ($oCerts[0].CertName.Substring(4,1)) -replace '_','' ;     # Get the TERA plant name (the one coded into our config)
        }
        else {
            $TERA = ''
        }
        $OldCertName = "TERA$($TERA)_ODS_$($OldInstName)_Cert"
        $NewCertName = "TERA$($TERA)_ODS_$($NewInstName)_Cert"

        #   New> Get the list of "TERA" certificates
        #
        Write-verbose "--- New:  Obtaining the list of 'TERA' certificates"
        $nCerts = Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query "
            SELECT  CT.[name] AS CertName,
                    CT.certificate_id AS CertID,
                    CONVERT(VARCHAR(MAX),CERTENCODED(CERT_ID([name])), 1) AS CertBin
                    FROM sys.certificates CT
                    WHERE ([name] LIKE '$($PlantCertMask)')
                    ORDER BY CertName "

        #   New> Generate a new full CERTIFICATE with private key for the new server if not already present
        #
        # $iCert = $nCerts | Where-Object CertName -EQ $NewCertName | Select-Object -First 1
        # if ($null -eq $iCert) {
            Write-verbose "--- New:  Creating full certificate for [$($NewCertName)]"
            $SqlCert = "
            IF NOT EXISTS (SELECT [name] 
                                FROM sys.certificates
                                WHERE [name] = '$($NewCertName)')
                CREATE CERTIFICATE [$($NewCertName)]	
                    WITH SUBJECT = 'Certificate [$($NewCertName)] used for cross firewall traffic' ,
                    START_DATE = '03/01/2021', EXPIRY_DATE = '12/31/2030';
            ELSE
                PRINT '** Skip ** -- Cert: [$($NewCertName)] already exists on new server [$($SvrNew_Name)]';"
            [void]$NewInstSQL.Add($SqlCert)
            Write-verbose "--- New: $($SqlCert)"
            if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query $SqlCert}
        # }
        # else {
        #     Write-Verbose "--- New:  ** Skip ** Full certificate for [$($NewCertName)] Exists"
        # }

        # ###############################################################################################
        #
        #   C R E A T E   A G   L O G I N S  a n d   U S E R S  
        # 
        # ###############################################################################################

        #   New> Generate CREATE CERTIFICATE statements (no private key) to duplicate the old server TERA certificates (public key only)
        #
        foreach ($oCert in $oCerts) {
            # if ($oCert.CertName -ine $NewCertName) {
                $RmtInstName = $oCert.CertName.Split('_')[2]
                Write-Verbose "--- New:  [$($oCert.CertName)]: Creating inbound login/user - [AG_In_$($RmtInstName)_Login] "
                $SqlLogin = "

                    -- $($RmtInstName) : Login/User/Cert      
                    If NOT EXISTS (SELECT loginname from master.dbo.syslogins 
                                    WHERE name =  N'AG_In_$($RmtInstName)_Login')
                        BEGIN
                            PRINT 'Creating LOGON [AG_In_$($RmtInstName)_Login]'
                            CREATE LOGIN [AG_In_$($RmtInstName)_Login] WITH PASSWORD = '$($Inbound_Pswd)';  
                        END
                        ELSE BEGIN
                            PRINT 'Skipping LOGIN - [AG_In_$($RmtInstName)_Login] exists'
                        END
                    IF NOT EXISTS (SELECT name 
                                    FROM [sys].[database_principals]
                                    WHERE [type] = 'S' AND name = N'AG_In_$($RmtInstName)_User')
                        CREATE USER [AG_In_$($RmtInstName)_User] FOR LOGIN [AG_In_$($RmtInstName)_Login];
                    IF NOT EXISTS (SELECT [name] 
                                    FROM sys.certificates
                                    WHERE [name] = '$($oCert.CertName)')
                                AND (CAST(SERVERPROPERTY('ServerName') AS VARCHAR(127)) != '$($RmtInstName)')
                        CREATE CERTIFICATE [$($oCert.CertName)] AUTHORIZATION [AG_In_$($RmtInstName)_User] FROM BINARY = $($oCert.CertBin);
                    "
                [void]$NewInstSQL.Add($SqlLogin)
                if ($RmtInstName -imatch "SQL0") {
                    $GeneratedSql_RemoteCorpFile = $GeneratedSql_RemoteCorp -replace "<ServerName>", $RmtInstName 
                    $SqlLogin > $GeneratedSql_RemoteCorpFile
                }
                else {
                    $GeneratedSql_RemoteMfgFile = $GeneratedSql_RemoteMfg -replace "<ServerName>", $RmtInstName 
                    $SqlLogin > $GeneratedSql_RemoteMfgFile
                }
                Write-verbose "--- New:  $($SqlLogin)"
                if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Database 'master' -Query $SqlLogin}
            #}
        }

        #   New> Refresh the list of "TERA" certificates
        #
        Write-verbose "--- New:  Refresh the list of 'TERA' certificates"
        $nCerts = Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query "
            SELECT  CT.[name] AS CertName,
                    CT.certificate_id AS CertID,
                    CONVERT(VARCHAR(MAX),CERTENCODED(CERT_ID([name])), 1) AS CertBin
                    FROM sys.certificates CT
                    WHERE ([name] LIKE '$($PlantCertMask)')
                    ORDER BY CertName "

        #   Old> Now add a public certificate of the new node to the other older nodes
        #
        $oCert = ($nCerts | Where-Object CertName -eq $NewCertName | Select-Object -first 1)
        Write-Verbose "--- Rmt:  [$($NewCertName)]: Creating inbound login/user"
        $SqlLogin = " 

            -- $($NewInstName) : Login/User/Cert               
            If NOT EXISTS (SELECT loginname from master.dbo.syslogins 
                            WHERE name =  N'AG_In_$($NewInstName)_Login')
                        BEGIN
                            PRINT 'Creating LOGON [AG_In_$($NewInstName)_Login]'
                            CREATE LOGIN [AG_In_$($NewInstName)_Login] WITH PASSWORD = '$($Inbound_Pswd)';
                         END
                        ELSE BEGIN
                            PRINT 'Skipping LOGIN - [AG_In_$($NewInstName)_Login] exists'
                        END 
            IF NOT EXISTS (SELECT name 
                            FROM [sys].[database_principals]
                            WHERE [type] = 'S' AND name = N'AG_In_$($NewInstName)_User')
                CREATE USER [AG_In_$($NewInstName)_User] FOR LOGIN [AG_In_$($NewInstName)_Login];
            IF NOT EXISTS (SELECT [name] 
                            FROM sys.certificates
                            WHERE [name] = '$($oCert.CertName)')
                    AND (CAST(SERVERPROPERTY('ServerName') AS VARCHAR(127)) != '$($NewInstName)')
                CREATE CERTIFICATE [$($oCert.CertName)] AUTHORIZATION [AG_In_$($NewInstName)_User] FROM BINARY = $($oCert.CertBin);
            "
        #[void]$RmtInstSQL.Add($SqlLogin)
        if ($NewInstName -imatch "SQL0") {
            $GeneratedSql_RemoteCorpFile = $GeneratedSql_RemoteCorp -replace "<ServerName>", $NewInstName 
            $SqlLogin > $GeneratedSql_RemoteCorpFile
        }
        else {
            $GeneratedSql_RemoteMfgFile  = $GeneratedSql_RemoteMfg  -replace "<ServerName>", $NewInstName 
            $SqlLogin > $GeneratedSql_RemoteMfgFile
        }
        Write-verbose "--- Rmt: $($SqlLogin)"
        Write-verbose "--- New: $($SqlLogin)"
        if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query $SqlLogin}


        # ###############################################################################################
        #
        #   M I R R O R   P O R T 
        #
        #   Get the mirror port properly configured
        # 
        # ###############################################################################################

        #   If the mirror port is configured -- make sure the setting are proper
        #
        $OldAuthScheme = ""
        foreach ($iAuth in ($MPortConnAuth.Split(','))) {
            if ($iAuth.Trim() -ieq 'NEGOTIATE') {$OldAuthScheme = $OldAuthScheme + 'WINDOWS NEGOTIATE '}
            if ($iAuth.Trim() -ieq 'CERTIFICATE') {$OldAuthScheme = $OldAuthScheme + "CERTIFICATE [$($NewCertName)] "}   
        } 
        $OldAuthScheme = $OldAuthScheme.Trim()

        $NewMPortAction = ""
        if ($nInfo.MPortName -ieq 'NONE') {
            $NewMPortAction = "CREATE"
            $NewMPortName = $MPortName
        }
        elseif ($nInfo.MPortNo -ne $MPortNo) {
            $NewMPortAction = "CREATE"
            $NewMPortName = $nInfo.MPortName

            #Write-verbose "--- New:  Mirror Port is ($($nInfo.MPortNo)) - Should be ($($MPortNo)) - Drop and recreate "

            $SqlConf =  "DROP IF EXISTS ENDPOINT [$($nInfo.MPortName)]; "
            [void]$NewInstSQL.Add($SqlConf)
            Write-verbose "--- New:  $($SqlConf)"
            if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query $SqlConf }
        }
        elseif ($MPortConnAuth -eq $nInfo.MPortConnAuth -and
                $MPortRole -eq $nInfo.MPortRole -and
                $MPortEncState -eq $nInfo.MPortEncState -and
                $MPortEnc -eq $nInfo.MPortEnc) {
            $NewMPortAction = "OK"  
            $NewMPortName = $nInfo.MPortName
            Write-Verbose "--- New:  Mirroring Endpoint [$($nInfo.MPortName)] already exists: Properly setup"      
        }
        else {
            Write-Verbose "--- New:  Mirroring Endpoint [$($nInfo.MPortName)] already exists: Adjusting properties"
            $SqlConf = "
                ALTER ENDPOINT [$($nInfo.MPortName)]
                        FOR DATABASE_MIRRORING 
                        ( AUTHENTICATION = $($OldAuthSchema),
                            ENCRYPTION = REQUIRED ALGORITHM $($MPortEnc), ROLE=$($MportRole)); "
            [void]$NewInstSQL.Add($SqlConf)
            Write-verbose "--- New:  $($SqlConf)"
            if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query $SqlConf}        
        }
        
        if ($NewMPortAction -ieq "CREATE") {
            Write-verbose "--- New:  Mirror Port Not Found -- Create new mirror Port"
            $SqlConf =  "
                CREATE ENDPOINT [$($NewMPortName)]   STATE = STARTED 
                    AS TCP ( LISTENER_PORT=$($MPortNo), LISTENER_IP = ALL )
                        FOR DATABASE_MIRRORING 
                            ( AUTHENTICATION = $($OldAuthScheme),
                                ENCRYPTION = REQUIRED ALGORITHM $($MPortEnc), ROLE=$($MportRole)); "
            [void]$NewInstSQL.Add($SqlConf)
            Write-verbose "--- New:  $($SqlConf)"
            if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query $SqlConf}
        }


        # ###############################################################################################
        #
        #   G R A N T   I N B O U N D   A C C E S S 
        #
        #   Grant the special AG_In_xxxxxxxxxxxxx_Login accounts connect permission to the mirror port
        # 
        # ###############################################################################################

        #   New> Get the list of "TERA" certificates
        #
        Write-verbose "--- New:  Obtaining the list of 'TERA' certificates"
        $nCerts = Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query "
            SELECT  CT.[name] AS CertName,
                    CT.certificate_id AS CertID,
                    CONVERT(VARCHAR(MAX),CERTENCODED(CERT_ID([name])), 1) AS CertBin
                    FROM sys.certificates CT
                    WHERE ([name] LIKE '$($PlantCertMask)')
                    ORDER BY CertName ";

        foreach ($nCert in $nCerts) {
            $RmtInstName = $nCert.CertName.Split('_')[2]
            $AGLoginName = "AG_In_$($RmtInstName)_Login"
            Write-Verbose "--- New:  [$($AGLoginName)]: Grant login permission to [$($NewMPortName)]"
            $SqlLogin = "

                        -- $($RmtInstName) : Login/User/Cert                 
                        GRANT CONNECT ON ENDPOINT::[$($NewMportName)] TO [AG_In_$($RmtInstName)_Login]; 
                        PRINT 'Granting CONNECT ON ENDPOINT::[$($NewMportName)] TO [AG_In_$($RmtInstName)_Login]' "
            
            if ($nCert.CertName -ine $NewCertName) {
                [void]$NewInstSQL.Add($SqlLogin) 
                Write-verbose "--- New:  $($SqlLogin)"
                if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Database 'master' -Query $SqlLogin}
            }
            if ($RmtInstName -imatch "SQL0") {
                $GeneratedSql_RemoteCorpFile = $GeneratedSql_RemoteCorp -replace "<ServerName>", $RmtInstName 
                $SqlLogin >> $GeneratedSql_RemoteCorpFile
            }
            else {
                $GeneratedSql_RemoteMfgFile = $GeneratedSql_RemoteMfg -replace "<ServerName>", $RmtInstName 
                $SqlLogin >> $GeneratedSql_RemoteMfgFile
            }
    
                #[void]$RmtInstSQL.Add($SqlLogin)
            #$SqlLogin >> $GeneratedSql_RemoteNodes
            if ($DebugLevel -ge 3) { Write-verbose "--- Rmt:  $($SqlLogin)" }
        }


        # ###############################################################################################
        #
        #   Assemble the combined script files 
        #
        #   One for defining the logins/users/certificates for the corp side servers.  
        #   One for defining the logins/users/certificates for the MFG cluster servers.  
        # 
        # ##############################################################################################

        $GeneratedSql_CorpCluster = $GeneratedSql_RemoteCorp -replace "<ServerName>", "Combined"
        if (Test-Path $GeneratedSql_CorpCluster ) {
            Remove-Item -Path  $GeneratedSql_CorpCluster
        }
        "" | Out-File -Append $GeneratedSql_CorpCluster

        Get-ChildItem  -Path ($GeneratedSql_RemoteCorp -replace "<ServerName>", "*") -include "*$($PlantCode)_Corp_*.sql" -exclude "*Combined*.sql" | Select-Object {
            "`r`n -----------------------------------------------------------------------------------`r`n" >> $GeneratedSql_CorpCluster
            Get-Content $_ | Out-File -Append $GeneratedSql_CorpCluster
        }

        $GeneratedSql_MfgCluster = $GeneratedSql_RemoteMfg -replace "<ServerName>", "Combined"
        if (Test-Path $GeneratedSql_MfgCluster ) {
            Remove-Item -Path  $GeneratedSql_MfgCluster
        }
        "" | Out-File -Append $GeneratedSql_MfgCluster

        Get-ChildItem  -Path ($GeneratedSql_RemoteMfg -replace "<ServerName>", "*") -include "*$($PlantCode)_Mfg_*.sql" -exclude "*Combined*.sql" | Select-Object {
            "`r`n -----------------------------------------------------------------------------------`r`n" >> $GeneratedSql_MfgCluster
            Get-Content $_ | Out-File -Append $GeneratedSql_MfgCluster
        }

        $GeneratedSql_Complete = $GeneratedSqlScriptFileBase + "Both_$($ClsName).sql"
        
        if (Test-Path $GeneratedSql_Complete ) {
            Remove-Item -Path  $GeneratedSql_Complete
        }
        Get-Content $GeneratedSql_CorpCluster | Out-File -Append $GeneratedSql_Complete
        Get-Content $GeneratedSql_MfgCluster  | Out-File -Append $GeneratedSql_Complete
            
    }


    # ###############################################################################################
    #
    #   P H A S E ---  2
    # 
    # ###############################################################################################

    if ($Phase -eq 2 -or $Phase -eq 99) {
        Write-Verbose "***********  Phase 2  **********************"

        # ###############################################################################################
        #
        #   P U S H   T O   O T H E R   R E P L I C A S 
        # 
        # ###############################################################################################

        $GeneratedSql_CorpCluster = $GeneratedSql_RemoteCorp -replace "<ServerName>", "Combined"
        $GeneratedSql_MfgCluster = $GeneratedSql_RemoteMfg -replace "<ServerName>", "Combined"
        $GeneratedSql_Complete = $GeneratedSqlScriptFileBase + "Both_$($ClsName).sql"

        Write-Verbose "--- Rmt:  Get the list of reachable AG partner nodes for pushing scripts"
        $nPlantSvrs = Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Query "
            WITH CRT AS (
                SELECT  CT.[name] AS CertName,
                        SUBSTRING(CT.[name], (CHARINDEX('SQL', CT.[name] )-4), 13) AS InstanceName,
                        CASE SUBSTRING(CT.[name], (CHARINDEX('SQL', CT.[name] )+3), 2)
                            WHEN '01' THEN '.fs.local'
                            WHEN '02' THEN '.qa.fs'
                            WHEN '03' THEN '.dev.fs'
                            WHEN '20' THEN '.mfg.fs'
                            WHEN '21' THEN '.npq.mfg'
                            ELSE 'XX' END AS [Domain],
                        CASE SUBSTRING(CT.[name], (CHARINDEX('SQL', CT.[name] )+3), 2)
                            WHEN '01' THEN 'Corp'
                            WHEN '02' THEN 'Corp'
                            WHEN '03' THEN 'Corp'
                            WHEN '20' THEN 'MFG'
                            WHEN '21' THEN 'MFG'
                            ELSE 'XX' END AS [FWSide]
                        FROM sys.certificates CT
                        WHERE ([name] LIKE '$($PlantCertMask)')
                )
            SELECT	CRT.CertName,
                    CRT.InstanceName,
                    CONCAT(CRT.InstanceName, CRT.[Domain]) AS FullInstanceName,
                    CRT.FWSide
                FROM CRT";

        #
        #   Now select the proper combined login script needed for servers on the corporate or MFG side of the firewall
        #
        
        $TSql_CorpCluster = ( Get-Content -Path $GeneratedSql_CorpCluster ) -join "`r`n"   # Import the file text into a string with CRLF after each line
        $TSql_MfgCluster = ( Get-Content -Path $GeneratedSql_MfgCluster ) -join "`r`n" 
        $TSql_Complete = ( Get-Content -Path $GeneratedSql_Complete ) -join "`r`n" 

        foreach ($plantSvr in $nPlantSvrs) {
            if ($FirewallSide -ieq 'Corp') {
                if ($FirewallSide -ieq $plantSvr.FWSide) {
                    Write-Verbose "--- Rmt:  (Corp) Submit certs, logins, users and connect permissions --> '$($plantSvr.FullInstanceName)'"
                    Invoke-Sqlcmd -ServerInstance $plantSvr.FullInstanceName -Database 'master' -Query $TSql_CorpCluster
                }
                elseif ($CrossFW) {
                    Write-Verbose "--- Rmt:  (Corp) Submit certs, logins, users and connect permissions --> '$($plantSvr.FullInstanceName)' (Cross firewall)"
                    Invoke-Sqlcmd -ServerInstance $plantSvr.FullInstanceName -Database 'master' -Query $TSql_CorpCluster
                }
                else {
                    Write-Verbose "--- Rmt:  (Corp) SKIP -- scripts --> '$($plantSvr.FullInstanceName)' (Cross firewall)"
                }
            }

            elseif ($FirewallSide -ieq 'MFG') {
                if ($FirewallSide -ieq $plantSvr.FWSide) {
                    Write-Verbose "--- Rmt:  (MFG) Submit certs, logins, users and connect permissions --> '$($plantSvr.FullInstanceName)'"
                    Invoke-Sqlcmd -ServerInstance $plantSvr.FullInstanceName -Database 'master' -Query $TSql_Complete
                }
                elseif ($CrossFW) {
                    Write-Verbose "--- Rmt:  (MFG) Submit certs, logins, users and connect permissions --> '$($plantSvr.FullInstanceName)' (Cross firewall)"
                    Invoke-Sqlcmd -ServerInstance $plantSvr.FullInstanceName -Database 'master' -Query $TSql_MfgCluster
                }              
                else {
                    Write-Verbose "--- Rmt:  (MFG) SKIP -- scripts --> '$($plantSvr.FullInstanceName)' (Cross firewall)"
                }
            }
        }

    }


    # ###############################################################################################
    #
    #   P H A S E ---  3    -- Copy objects from the old server to this server
    # 
    # ###############################################################################################

    if ($Phase -eq 3 -or $Phase -eq 99) {
        Write-Verbose "***********  Phase 3  **********************"

        #
        #   Create all needed database data and log subdirectories on the new server
        #
        Write-Verbose "--- Old:  Get the list of databases file paths that are members of AG's"

        #   Get the list of database data and log directories from the old server
        #
        $SqlFiles = "
            SELECT	DISTINCT -- DB.[name] AS DatabaseName,
                LEFT(SMF.physical_name,LEN(SMF.physical_name) - CHARINDEX('\',REVERSE(SMF.physical_name))) AS FileDir
                --,SMF.*
            FROM sys.databases DB
                INNER JOIN sys.availability_databases_cluster ADC
                    ON (DB.[name] = ADC.[database_name])
                INNER JOIN sys.master_files SMF
                    ON (DB.[database_id] = SMF.[database_id])
            ORDER BY FileDir";

        Write-Verbose "--- Old:  Fetching the list of database data/log directories associated with AGs"
        $oFileDirs = Invoke-Sqlcmd -ServerInstance $SvrOld_Name -Database 'master' -Query $SqlFiles
 
        $oFileDirs | Where-Object  FileDir -match '^[^\\]+[\\]+[^\\]+$'  | Select-Object  {  # -ExpandProperty FileDir)
            $path = $_.FileDir
            $sql = "EXEC master.dbo.xp_create_subdir '$($path)'"
            try {
                $oFileDirs = Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Database 'master' -Query $sql
                Write-Verbose "--- New:  Created new subdirectory - $($path)"
            }
            catch {
                Write-Verbose "--- New:  Cannot create new subdirectory - $($path)"
                Write-Verbose "--- New:  " + $_.Message
            }

        }  

        #  Copy the logins, server roles and server level permissions to the new server
        #

        $sqlSecurity = "                 
            DECLARE @crlf  CHAR(2) = CHAR(13)+CHAR(10);

            IF OBJECT_ID('tempdb..#cmdlist') IS NOT NULL
                DROP TABLE #cmdlist;
            CREATE TABLE #cmdlist (
                    ID INT IDENTITY(1,1) NOT NULL,
                    LoginName VARCHAR(128) NOT NULL,
                    CMD VARCHAR(2000) NOT NULL
                        );
                        
            SET NOCOUNT ON
            -- Scripting Out the Logins To Be Created
            INSERT INTO #cmdlist (LoginName, CMD)
            SELECT  SP.name AS LoginName,
                    'IF (SUSER_ID('+QUOTENAME(SP.name,'''')+') IS NULL) BEGIN '
                    + 'CREATE LOGIN ' +QUOTENAME(SP.name)
                        +  CASE 
                                WHEN SP.type_desc = 'SQL_LOGIN' THEN ' WITH PASSWORD = ' +CONVERT(NVARCHAR(MAX),SL.password_hash,1)+ @crlf +' HASHED, ' + 
                                        ' SID = ' + CONVERT(NVARCHAR(MAX),SL.[sid],1) + @crlf 
                                    + ', CHECK_EXPIRATION = ' 
                                    + IIF(SL.is_expiration_checked = 1, 'ON', 'OFF') 
                                    + ', CHECK_POLICY = ' + IIF(SL.is_policy_checked = 1, 'ON', 'OFF')
                                    + IIF(NOT SP.default_language_name IS NULL, ', DEFAULT_LANGUAGE=[' +SP.default_language_name+ ']','')
                                ELSE ' FROM WINDOWS ' +  IIF(NOT SP.default_language_name IS NULL, 'WITH DEFAULT_LANGUAGE=[' +SP.default_language_name+ ']','')
                            END 
                + IIF(SP.default_database_name <> 'master', @crlf + 'ALTER LOGIN '+QUOTENAME(SP.name,'[')+' WITH DEFAULT_DATABASE = '+QUOTENAME(SP.default_database_name,'['), '') +' END;' COLLATE SQL_Latin1_General_CP1_CI_AS AS [-- Logins To Be Created --]
            FROM sys.server_principals AS SP 
                    LEFT JOIN sys.sql_logins AS SL
                        ON SP.principal_id = SL.principal_id
            WHERE SP.type IN ('S','G','U')
                    AND SP.name NOT LIKE '##%##'
                    AND SP.name NOT LIKE 'NT AUTHORITY%'
                    AND SP.name NOT LIKE 'NT SERVICE%'
                    AND SP.name NOT LIKE 'AG_In_%'
                    AND SP.name <> ('sa')
                    AND ((SP.name NOT LIKE '%$($PlantCode.SubString(0,3))[0-9]%') OR (SP.name LIKE '%$($PlantCode)%'))
            ORDER BY SP.[type_desc], SP.[name];

            --
            -- Create Server Roles
            --
            INSERT INTO #cmdlist (LoginName, CMD)

            SELECT	'__' + SP.[name] AS LoginName,
                    'IF NOT EXISTS (SELECT [name] from master.sys.server_principals WHERE [name] = ' +
                            QUOTENAME(SP.[name], '''') + ' and [type] = ''R'')' + CHAR(13) + CHAR(10) +
                            '	CREATE SERVER ROLE ' + QUOTENAME(SP.[name], '[') +' AUTHORIZATION [sa];' + CHAR(13) + CHAR(10) AS Command
                FROM sys.server_principals AS SP 
                WHERE (SP.[type] IN ('R'))
                    --AND (SP.[is_fixed_role] <> 1)
                    AND (SP.[principal_id] > 10 )
                    AND (SP.[name] NOT IN ('public'))
                ORDER BY SP.[name];

            -- Scripting Out the Role Membership to Be Added
            INSERT INTO #cmdlist (LoginName, CMD)
            SELECT SL.name AS LoginName,
            'EXEC master..sp_addsrvrolemember @loginame = N''' + SL.name + ''', @rolename = N''' + SR.name + '''
            ' AS [-- Server Roles the Logins Need to be Added --]
            FROM master.sys.server_principals SR
                INNER JOIN master.sys.server_role_members SRM ON SR.principal_id = SRM.role_principal_id
                INNER JOIN master.sys.server_principals SL ON SL.principal_id = SRM.member_principal_id
            WHERE (SL.type IN ('S','G','U'))
                    AND (SL.name NOT LIKE '##%##' AND
                    SL.name NOT LIKE 'NT AUTHORITY%'
                    AND SL.name NOT LIKE 'AG_In_%'
                    AND SL.name NOT LIKE 'NT SERVICE%'
                    AND SL.name <> 'sa')
                    AND (SR.[type] IN ('R'))
                    AND ((SR.name NOT LIKE '%$($PlantCode.SubString(0,3))[0-9]%') OR (SR.name LIKE '%$($PlantCode)%'))
            ORDER BY SL.[name];

            -- Scripting out the Permissions to Be Granted
            INSERT INTO #cmdlist (LoginName, CMD)
                SELECT SP.name AS LoginName,
                    CASE
                        WHEN SrvPerm.state_desc <> 'GRANT_WITH_GRANT_OPTION' THEN
                            SrvPerm.state_desc
                        ELSE
                            'GRANT'
                    END + ' ' + 
                    SrvPerm.permission_name + 
                    CASE WHEN SrvPerm.class_desc = 'ENDPOINT'
                                THEN 
                                    ' ON ENDPOINT::[' + ISNULL(EP.[name],'') + '] '
                                ELSE ' ' END +
                    ' TO [' + SP.name + '] ' +
                    CASE
                            WHEN SrvPerm.state_desc <> 'GRANT_WITH_GRANT_OPTION ' THEN
                                ''
                            ELSE
                                ' WITH GRANT OPTION '
                    END COLLATE DATABASE_DEFAULT AS [-- Server Level Permissions to Be Granted --]

                FROM sys.server_permissions AS SrvPerm
                    INNER JOIN sys.server_principals AS SP
                        ON (SrvPerm.grantee_principal_id = SP.principal_id)
                    LEFT OUTER JOIN sys.endpoints EP
                            ON (SrvPerm.class = 105) AND (SrvPerm.major_id = EP.endpoint_id)
                WHERE
                    SP.type IN ( 'S', 'G', 'U' )
                    AND SP.name NOT LIKE '##%##'
                    AND SP.name NOT LIKE 'NT AUTHORITY%'
                    AND SP.name NOT LIKE 'NT SERVICE%'
                    AND SP.name NOT LIKE 'm_*$'
                    AND SP.name NOT LIKE 'g_*$'
                    AND SP.name NOT LIKE 'AG_In_%'
                    AND SP.name <> ( 'sa' )
                    AND ((SP.name NOT LIKE '%$($PlantCode.SubString(0,3))[0-9]%') OR (SP.name LIKE '%$($PlantCode)%'))
                ORDER BY
                    LoginName,
                    SrvPerm.permission_name

            SELECT LoginName, CMD	
                FROM #cmdlist
                ORDER BY LoginName, ID;

            DROP TABLE #cmdlist;
        ";

        #  Script out the security commands from the old server
        #
        $GeneratedSql_Security = $GeneratedSqlScriptFileBase + "Security_$($OldInstName).sql"
        Write-Verbose "--- Script out instance level security:  $($GeneratedSql_Security)"

        $secOldServer = Invoke-Sqlcmd -ServerInstance $SvrOld_Name -Database 'master' -Query $sqlSecurity
        $SecCommands = ($secOldServer | Select-Object -ExpandProperty CMD ) -join "`r`n"
        $SecCommands  | Out-File $GeneratedSql_Security

        try {
            Write-Verbose "--- Apply instance level security to :  $($SvrNew_Name)"
            Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Database 'master' -Query $SecCommands -OutputSqlErrors $true -ErrorAction SilentlyContinue 2>&1 > "$($GeneratedOutputDir)\SqlText.txt"
            $a = $Error[0]
        }
        catch {
            Write-Verbose "$($_.Exception.Message)"    
            $Error.Clear()
            Get-Content "$($GeneratedOutputDir)\SqlText.txt" 
        }
        
    }



    # =========================================================================================
    #
    # Phase 4:  Create the scripts for the Availability Groups (AG)
    #
    # =========================================================================================
    if ($Phase -eq 4 -Or $Phase -eq 99) {
        Write-Verbose "***********  Phase 4  **********************"

        #   Get the list of SQL Availability Groups from the Windows cluster.  If it is exists in the cluster
        #   then we do not need to CREATE the AG.  We only need to check if each 
        Write-Verbose "--- Cluster:  List of availability groups know to the cluster"

        #   Generate the CREATE AG script for emergency so the initial node can be created
        #
        if (Test-Path $GeneratedSql_AGCreate ) {
            Remove-Item -Path $GeneratedSql_AGCreate    # Remove the existing AG/DAG Create/Join scripts
        }
        if (Test-Path $GeneratedSql_AGJoin) {
            Remove-Item -Path  $GeneratedSql_AGJoin
        }        
        if (Test-Path $GeneratedSql_AGDAG ) {
            Remove-Item -Path  $GeneratedSql_AGDAG
        }

        foreach ($ag in $ClsAGs) {
                 
            Write-Verbose "--- Old:  Fetching detailed AG information from the Primary - '$($ag.AGPrimaryNode)'"
            $SqlAGInfo = "
                SELECT  AG.[Name] AS AGName,
                        AG.[group_id] AS AGID,
                        AG.[automated_backup_preference_desc] AS BackupPref,
                        AG.[failure_condition_level] AS FailureLevel,
                        AG.[health_check_timeout] AS HealthTimeout,
                        CASE WHEN AG.[db_failover] = 0 THEN 'OFF' ELSE 'ON' END AS DbFailover,
                        CASE WHEN AG.[dtc_support] = 0 THEN 'NONE' ELSE 'PER_DB' END AS DtcSupport,
                        AG.is_distributed,
                        ISNULL(HARS.is_local, 0) AS is_local,
                        IIF(AGL.dns_name IS NULL, 0, 1) AS has_Listener,
                        AGL.dns_name AS LDnsName,
                        ISNULL(AGL.[port], 0) AS LPort,
                        AGL.ip_configuration_string_from_cluster AS LInfo,
                        AGLIP.ip_address AS LIAddrs,
                        AGLIP.ip_subnet_mask AS LIMask,
                        CONCAT(ISNULL(AGLIP.network_subnet_ip, 0), '/',ISNULL(AGLIP.network_subnet_prefix_length,0)) AS LSubnet,
                        AR.replica_server_name AS ReplicaName,
                        AR.replica_id,
                        ISNULL(HARS.role_desc, IIF(AG.is_distributed=1 AND HARS.is_local IS NULL, 'PRIMARY', '--')) AS ReplRole,
                        AR.[endpoint_url],
                        RIGHT(AR.[endpoint_url], CHARINDEX(':',REVERSE(AR.[endpoint_url])) - 1) AS MirPort,
                        AR.availability_mode_desc AS AvailabilityMode,
                        AR.failover_mode_desc AS FailoverMode,
                        AR.primary_role_allow_connections_desc AS AllowPrimConn,
                        AR.secondary_role_allow_connections_desc AS AllowSecConn,
                        AR.backup_priority AS BackupPrio,
                        AR.seeding_mode_desc AS SeedingMode,
                        AR.[session_timeout] AS SessionTimeout
                    FROM sys.availability_groups AG
                        LEFT OUTER JOIN (sys.availability_group_listeners AGL 
                                            INNER JOIN sys.availability_group_listener_ip_addresses AGLIP 
                                                ON (AGLIP.listener_id = AGL.listener_id))
                            ON (AG.group_id = AGL.group_id)
                        INNER JOIN sys.availability_replicas AR
                            ON (AG.group_id = AR.group_id)
                        LEFT OUTER JOIN sys.dm_hadr_availability_replica_states HARS
                            ON (AR.replica_id = HARS.replica_id)
                    WHERE (AG.[Name] = '$($ag.AGName)') AND (AG.is_distributed = 0)
                    ORDER BY AG.[Name], AR.replica_server_name;"
            
            $AGList = Invoke-Sqlcmd -ServerInstance $ag.AGPrimaryNode -Database 'master' -Query $SqlAGInfo

            $AGRepl = $AGList | Where-Object ReplRole -ieq 'PRIMARY'

            Write-Verbose "--- Create:  Attempt to create the first instance of the AG: [$($ag.AGName)]"

            # ADD LISTENER N'AGTestListener' ( WITH IP ((N'10.1.12.34', N'255.255.254.0')), PORT=5022);
            $SqlListener = ""
            if ($AGRepl.has_Listener -eq 1) {
                $SqlListener = "LISTENER N'$($AGRepl.LDnsName)' ( WITH IP ((N'$($AGRepl.LIAddrs)', N'$($AgRepl.LIMask)')), PORT=$($AgRepl.LPort))"
            }

            $FirstAGNode = $ag.AGPrimaryNode
            if (-not $ClsSame) 
            {
                $FirstAGNode = $NewInstanceName
                $SqlListener = ""
            }

            $AGName = $ag.AGName -replace "$($PlantCode.SubString(0,3))[0-9]",$PlantCode

            $SqlAG = "
                IF (SERVERPROPERTY('servername') = '$($FirstAGNode)')
                BEGIN
                    BEGIN TRY
                        CREATE AVAILABILITY GROUP [$AGName)]
                            WITH (AUTOMATED_BACKUP_PREFERENCE = $($AGRepl.BackupPref),
                                DB_FAILOVER = $($AGRepl.DbFailover),
                                DTC_SUPPORT = $($AGRepl.DtcSupport),
                                HEALTH_CHECK_TIMEOUT = $($AGRepl.HealthTimeout))
                            FOR 
                                REPLICA ON N'$($FirstAGNode)' 
                                    WITH (ENDPOINT_URL = N'TCP://$($FirstAGNode).$($ClsDomain):$($AGRepl.MirPort)', 
                                        FAILOVER_MODE = $($AGRepl.FailoverMode), 
                                        AVAILABILITY_MODE = $($AGRepl.AvailabilityMode), 
                                        SESSION_TIMEOUT = $($AGRepl.SessionTimeout), 
                                        BACKUP_PRIORITY = $($AGRepl.BackupPrio), 
                                        PRIMARY_ROLE(ALLOW_CONNECTIONS = $($AGRepl.AllowPrimConn)), 
                                        SECONDARY_ROLE(ALLOW_CONNECTIONS = $($AGRepl.AllowSecConn)),
                                        SEEDING_MODE = $($AGRepl.SeedingMode))
                            -- $($SqlListener);
                        PRINT 'AG [$AGName)] created'
                    END TRY
                    BEGIN CATCH
                        PRINT '*** Error *** Could not CREATE the AG [$($AGName)]'
                        SELECT ERROR_MESSAGE()
                    END CATCH
                END;"
            $SqlAG >> $GeneratedSql_AGCreate
            [void]$NewInstSQL.Add($SqlAG) 
            Write-verbose "--- Create:  $($SqlAG)"
            #if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Database 'master' -Query $SqlAGCreate} 
        

            #   Initiate join the new server into the AG from the old server
            #   
            if ($ClsSame)
            {
                Write-Verbose "--- Primary:  Attempt to initiate the join into the AG: [$($ag.Name)]"
                $SqlAG = "
                    IF (SERVERPROPERTY('servername') = '$($ag.AGPrimaryNode)')
                    BEGIN
                        BEGIN TRY
                            ALTER AVAILABILITY GROUP [$($AGRepl.AGName)]
                                ADD REPLICA ON N'$($NewInstName)' 
                                        WITH (ENDPOINT_URL = N'TCP://$($NewInstName).$($ClsDomain):$($AGRepl.MirPort)', 
                                            FAILOVER_MODE = MANUAL, 
                                            AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT, 
                                            SESSION_TIMEOUT = $($AGRepl.SessionTimeout), 
                                            BACKUP_PRIORITY = $($AGRepl.BackupPrio), 
                                            PRIMARY_ROLE(ALLOW_CONNECTIONS = $($AGRepl.AllowPrimConn)), 
                                            SECONDARY_ROLE(ALLOW_CONNECTIONS = $($AGRepl.AllowSecConn)),
                                            SEEDING_MODE = $($AGRepl.SeedingMode));
                            PRINT 'AG [$($AGRepl.AGName)] Join Setup on Primary'
                        END TRY
                        BEGIN CATCH
                            PRINT '*** Error *** Could not ALTER ADD Replica the AG [$($AGRepl.AGName)]'
                            SELECT ERROR_MESSAGE()
                        END CATCH
                    END;"
                $SqlAG >> $GeneratedSql_AGJoin
                [void]$OldInstSQL.Add($SqlAG) 
                Write-verbose "--- Old:  $($SqlAG)"
                if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $ag.AGPrimaryNode -Database 'master' -Query $SqlAG} 

                Start-Sleep -s 3        


                #   Complete join the new server into the AG from the new server
                #   
                Write-Verbose "--- New:  Attempt to complete the join into the AG: [$($AGRepl.AGName)]"
                $SqlAG = "
                    DECLARE @EMSG VARCHAR(2000);
                    IF (SERVERPROPERTY('servername') = '$($NewInstName)')
                    BEGIN
                        BEGIN TRY
                            ALTER AVAILABILITY GROUP [$($AGRepl.AGName)] JOIN;
                            PRINT 'AG [$($AGRepl.AGName)] Join Completed on Primary ''$($NewInstName)'''
                        END TRY
                        BEGIN CATCH
                            SET @EMSG = ERROR_MESSAGE()
                            PRINT '*** Error *** Could not ALTER JOIN the AG [$($AGRepl.AGName)]'
                            PRINT @EMSG
                        END CATCH
                    END;"
                $SqlAG >> $GeneratedSql_AGJoin
                [void]$NewInstSQL.Add($SqlAG) 
                Write-verbose "--- New:  $($SqlAG)"
                if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Database 'master' -Query $SqlAG} 

                Start-Sleep -s 5

                Write-Verbose "--- New:  Submit GRANT CREATE ANY DATABASE on remote AG: [$($AGRepl.AGName)]"
                $SqlAG = "
                    IF (SERVERPROPERTY('servername') = '$($NewInstName)')
                    BEGIN
                        BEGIN TRY
                            ALTER AVAILABILITY GROUP [$($AGRepl.AGName)] GRANT CREATE ANY DATABASE;
                            PRINT 'AG [$($AGRepl.AGName)] GRANT completed on ''$($NewInstName)'''
                        END TRY
                        BEGIN CATCH
                            PRINT '*** Error *** Could not ALTER GRANT the AG [$($AGRepl.AGName)]'
                            SELECT ERROR_MESSAGE()
                        END CATCH
                    END;"
                $SqlAG >> $GeneratedSql_AGJoin
                [void]$NewInstSQL.Add($SqlAG) 
                Write-verbose "--- New:  $($SqlAG)"
                if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Database 'master' -Query $SqlAG} 
            }
        } 
    }



    # =========================================================================================
    #
    # Phase 5:  SUBMIT "GRANT CREATE ANY DATABASE" for each AG on the new remote server
    #
    # =========================================================================================
    if ($Phase -eq 5 -Or $Phase -eq 99) {
        Write-Verbose "***********  Phase 5  **********************"
              
        Write-Verbose "--- New:  Fetching detailed AG information from the New Server - '$($SvrNew_Name)'"
        $SqlAGInfo = "
            SELECT  AG.[Name] AS AGName,
                    AG.[group_id] AS AGID
                FROM sys.availability_groups AG
                WHERE (AG.is_distributed = 0)
                ORDER BY AG.[Name];"
        
        $AGList = Invoke-Sqlcmd -ServerInstance  $SvrNew_Name -Database 'master' -Query $SqlAGInfo
    
        foreach ($AGRepl in $AGList) {
            $SqlAG = "
                IF (SERVERPROPERTY('servername') = '$($NewInstName)')
                BEGIN
                    BEGIN TRY
                        ALTER AVAILABILITY GROUP [$($AGRepl.AGName)] GRANT CREATE ANY DATABASE;
                        PRINT 'AG [$($AGRepl.AGName)] GRANT completed on ''$($NewInstName)'''
                    END TRY
                    BEGIN CATCH
                        PRINT '*** Error *** Could not ALTER GRANT the AG [$($AGRepl.AGName)]'
                        SELECT ERROR_MESSAGE()
                    END CATCH
                END;"
            #$SqlAG >> $GeneratedSql_AGJoin
            #[void]$NewInstSQL.Add($SqlAG) 
            Write-verbose "--- New:  $($SqlAG) -- GRANT CREATE ANY DATABASE "
            if ($DebugLevel -le 1) {Invoke-Sqlcmd -ServerInstance $SvrNew_Name -Database 'master' -Query $SqlAG} 
        }
    }


    # =========================================================================================
    #
    # Phase 6:  Create the scripts for the Distributed Availability Groups (DAG)
    #
    # =========================================================================================
    if ($Phase -eq 6 -Or $Phase -eq 99) {
        Write-Verbose "***********  Phase 6  **********************"

        #   Get the list of connected distributed availability groups (DAG)
        #
        $SqlDag = "               
            ;WITH PRIM AS (
                SELECT	AR.group_id,
                        AR.replica_server_name,
                        HARS.role_desc
                    FROM sys.dm_hadr_availability_replica_states HARS
                        INNER JOIN sys.availability_replicas AR
                            ON (HARS.replica_id = AR.replica_id)
                    WHERE (HARS.role_desc = 'PRIMARY')
            )
            SELECT	Dag.[name] AS DagName,
                    ReplFS.[replica_server_name] AS AGNameFs,
                    ReplFS.endpoint_url AS DagFsUrl,
                    ISNULL(PrimFS.replica_server_name, '') AS PrimaryFS,
                    ReplMFG.[replica_server_name] AS AGNameMfg,
                    ReplMFG.endpoint_url AS DagMfgUrl,
                    ISNULL(PrimMFG.replica_server_name, '') AS PrimaryMfg

                FROM sys.availability_groups Dag
                    INNER JOIN sys.availability_replicas ReplFS
                        ON (Dag.group_id = ReplFS.group_id) 
                    LEFT OUTER JOIN sys.availability_groups AGFS
                        ON (ReplFS.replica_server_name = AGFS.[name])
                    LEFT OUTER JOIN PRIM  PRimFS
                        ON (AGFS.group_id = PrimFS.group_id)
                    INNER JOIN sys.availability_replicas ReplMfg
                        ON (Dag.group_id = ReplMfg.group_id) 
                    LEFT OUTER JOIN sys.availability_groups AGMfg
                        ON (ReplMfg.replica_server_name = AGMfg.[name])
                    LEFT OUTER JOIN PRIM  PrimMFG
                        ON (AGMfg.group_id = PrimMFG.group_id)
                WHERE	(Dag.is_distributed = 1)
                    AND (ReplFS.replica_id != ReplMfg.replica_id)
                    AND (RIGHT(ReplFS.[replica_server_name], 4) = '_ODS')
        ";
        $DagInfo = Invoke-Sqlcmd -ServerInstance $SvrOld_Name -Database 'master' -Query $SqlDag


        foreach ($dag in $DagInfo) {
            $AgFsIP = (($dag.DagFsUrl.Split('/')[2]).Split(':')[0]);
            $AgMfgIP = (($dag.DagMfgUrl.Split('/')[2]).Split(':')[0]);

            
        #
        # Create the Distributed AG on the MFG side
        #
        $SqlDAG = "
            -- Run on MFG side primary server
            --
            IF (SELECT DEFAULT_DOMAIN()[DomainName]) IN ('NPQ', 'MFG')
            BEGIN
                CREATE AVAILABILITY GROUP [$($dag.DagName)]   
                    WITH (DISTRIBUTED) AVAILABILITY GROUP ON
                    N'$($dag.AGNameMfg)'  -- 
                    WITH (LISTENER_URL = N'$($dag.DagMfgUrl)', 
                        FAILOVER_MODE = MANUAL,  
                        AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                        SEEDING_MODE = AUTOMATIC),  
                    N'$($dag.AGNameFs)' --
                    WITH (LISTENER_URL = N'$($dag.DagFsUrl)',   
                        FAILOVER_MODE = MANUAL,   
                        AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                        SEEDING_MODE = AUTOMATIC)
            END
            ELSE
            BEGIN
                PRINT '--- Skipping CREATE of DAG [$($dag.DagName)] '  
            END;   "
        $SqlDAG >> $GeneratedSql_AGDAG
        Write-Verbose "--- DAG:  Generate script to create the distributed availability group (DAG) [$($AGRepl.AGName)]"
        Write-verbose "--- DAG:  $($SqlDAG)"

        "--- Joining Distributed AG: (Secondary) [$($dag.DagName)]"
        $SqlDag = "
            IF (SERVERPROPERTY('servername') = '$($dag.PrimaryFs)')
            BEGIN
                BEGIN TRY
                    ALTER AVAILABILITY GROUP [$($dag.DagName)]   
                        JOIN AVAILABILITY GROUP ON
                        N'$($dag.AGNameMfg)' 
                        WITH (LISTENER_URL = N'$($dag.DagMfgUrl)', 
                            FAILOVER_MODE = MANUAL,  
                            AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                            SEEDING_MODE = AUTOMATIC),  
                        N'$($dag.AGNameFs)' 
                        WITH (LISTENER_URL = N'$($dag.DagFsUrl)',   
                            FAILOVER_MODE = MANUAL,   
                            AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                            SEEDING_MODE = AUTOMATIC); 
                    PRINT 'AG [$($dag.DagName)] Join Setup on Primary'
                    PRINT '... Sleeping 10 sec ...'
                    WAITFOR DELAY '00:00:10'
                    PRINT 'New:  Grant Create All Databases permission on $($dag.PrimaryFS)'
                    ALTER AVAILABILITY GROUP [$($dag.AGNameFs)] GRANT CREATE ANY DATABASE;
                    PRINT '... Sleeping 10 sec ...'
                    WAITFOR DELAY '00:00:10'
                    PRINT 'New:  Setting all replicas readable on $($dag.PrimaryFS)'
                    ALTER AVAILABILITY GROUP [$($dag.AGNameFs))]  
                        MODIFY REPLICA ON  N'$($dag.PrimaryFS)' WITH   
                            (SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL));
                END TRY
                BEGIN CATCH
                    PRINT '*** Error *** Could not ALTER the AG [$($dag.DagName)]'
                    SELECT ERROR_MESSAGE()
                END CATCH
            END;" 
            $SqlDAG >> $GeneratedSql_AGDAG
            Write-Verbose "--- DAG:  Generate script to join/complete the distributed availability group (DAG) [$($dag.DagName)]"
            Write-verbose "--- DAG:  $($SqlDAG)"

        }
    }




    # ###############################################################################################
    #
    #   P H A S E ---  8  -- Copy any Resource Governor configuration
    # 
    # ###############################################################################################

    #   Get current resource governor status
    #
    if ($Phase -eq 8 -or $Phase -eq 99) {
        $RGInfo = Invoke-Sqlcmd -ServerInstance $SvrOld_Name -Database 'master' -Query "
        WITH RG1 AS (
            SELECT	CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(RSC.classifier_function_id, DB_ID('master')), '['),
                           '.',
                           QUOTENAME(OBJECT_NAME(RSC.classifier_function_id, DB_ID('master')), ']')) AS ClassifierName,
                    RSC.classifier_function_id,
                    RSC.is_enabled,
                    DRSC.is_reconfiguration_pending,
                    (SELECT COUNT(*) FROM sys.dm_resource_governor_resource_pools) AS PoolCnt,
                    --(SELECT COUNT(*) FROM sys.dm_resource_governor_resource_pools) AS PoolCnt,
                    LTRIM(RTRIM(OBJECT_DEFINITION(RSC.classifier_function_id))) AS FunctionBody            
                FROM sys.resource_governor_configuration RSC
                    CROSS apply sys.dm_resource_governor_configuration DRSC
                )
    
        SELECT	CHARINDEX('CREATE ', RG1.FunctionBody),
                RG1.ClassifierName,
                RG1.classifier_function_id AS FunctionObjectID,
                RG1.is_enabled,
                RG1.is_reconfiguration_pending,
                CASE WHEN CHARINDEX('CREATE ', RG1.FunctionBody) > 1 THEN 
                            RIGHT(RG1.FunctionBody, LEN(RG1.FunctionBody) - CHARINDEX('CREATE ', RG1.FunctionBody) +1)
            ELSE RG1.FunctionBody END AS ClassifierText
            FROM RG1
            ";

    }

}

Setup-TERA-NewPlant  -OldFullInstanceName "PBG1SQL20T341.mfg.fs" -NewFullInstanceName  "PBG1SQL20T3.mfg.fs"  -Phase 1 -Verbose -DebugLevel 1 -GeneratedOutputDir "E:\Backup\GeneratedScript\"

#Deploy-NewAGNode  -OldFullInstanceName "PBG2SQL20T104.mfg.fs" -NewFullInstanceName  "PBG2SQL20T114.mfg.fs"  -Phase 5 -Verbose -DebugLevel 2 -GeneratedOutputDir "E:\Backup\"

