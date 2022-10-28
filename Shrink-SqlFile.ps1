Import-Module SqlServer

function Compress-DbaFileSize {
    #
    #   Revision History:
    #   --  2021-09-10 -- Initial version
    #   --  2021-09-20 -- Handle "cannot shrink file" case at this time  skip file
    #
    [CmdletBinding()]
    param (     
        [Parameter(Position=0, Mandatory=$True)]   # Full Instance Name
        [string]
        $SqlInstance,

        [Parameter(Position=1, Mandatory=$True)]  # Database name to shrink
        [string]
        $DatabaseName,

        # [Parameter(Mandatory=$false)]             # Optional if specific file shrink needed
        # [string]
        # $SqlFileName,

        [Parameter(Mandatory=$false)]             # Optional - Size of file reduction per incremental pass
        [int]
        $ChunkSizeMB = 2000,

        [Parameter(Mandatory=$false)]             # Optional - Min allowed free space in file
        [string]
        $MinFreeMB = 50000,

        [Parameter(Mandatory=$false)]             # Optional - Min allowed free space in growth locked file
        [string]
        $MinFreeLockedMB = 50000,

        [Parameter(Mandatory=$false)]             # Optional - Maximum size for AG REDO/LOG queue before pausing further shrinkfile operations
        [int]
        $QueueSizeMaxSize = 5000000,

        [Parameter(Mandatory=$false)]             # Optional - Redo queue size to restart the shrink loop if loop was paused due to the queue size
        [int]
        $QueueSizeRecovered  = 1000000              #
    )

    # #########################################################################################################################
    #   Internal function to run SQL command and capture OOB InfoMessages from the SQL Client
    #
    function RunSqlQuery {
        param (
            # [Parameter(Position=0, Mandatory=$True)]   # Full Instance Name
            # [string]
            # $SqlInstance,
    
            [Parameter(Position=0, Mandatory=$True)]  # Database name to shrink
            [string]
            $DatabaseName,
    
            [Parameter(Position=1, Mandatory=$True)]  # Query to submit to SQL
            [String]
            $Query,

            [Parameter(Mandatory=$false)]
            [int]
            $CmdTimeout = 1800   # command timeout (1800 seconds default)
        )

        # The result object is used to return 
        $result = [PSCustomObject]@{
            Command = $Query
            Rows = $null
            Messages = @()
            Error = $null
        }
        try {
            # Build the SQLConnection object and connect to the database
            #
            $conn = New-Object System.Data.SqlClient.SqlConnection "Server=$($SqlInstance);Database=$($DatabaseName);Integrated Security=SSPI;";           
            $conn.Open()
            ## Attach the InfoMessage Event Handler to the connection to write out the messages
            $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {param($sender, $event) 
            #   Write-Host " -->> RunSqlCommand (Capture) -- $($event.Message)"
                $result.Messages += $event.Message
            };
            $conn.add_InfoMessage($handler);
            $conn.FireInfoMessageEventOnUserErrors = $true; 

            # Build the SQLCommand object so the InfoMessage event can be captured
            [System.Data.SqlClient.SqlCommand]$SqlCmdObj = $conn.CreateCommand();
            $SqlCmdObj.CommandText = $Query
            $SqlCmdObj.CommandTimeout = $Timeout
            $SqlCmdObj.ExecuteNonQuery();
        }
        catch {
            $result.Error = $_      # Capture the offeding exception
            Write-Host " -->> RunSqlCommand -- $($_.Exception.Message)"
        }
        finally {
            if ($SqlCmdObj) {   # Cleanup the SqlCommand object
                $SqlCmdObj.Dispose()
                $SqlCmdObj = $null
            }
            if ($conn) {        # Close and cleanup the SQLConnection
                $conn.Close()
                $conn.Dispose()
            }
        }
        Write-Output $result
    }



    # #########################################################################################################################
    #   Internal function to Set and Check wait timers
    #
    function Set-WaitTimer {
        param (
            [Parameter(Mandatory)]
            [PSCustomObject]
            $StatusObj,

            [Parameter(Mandatory)]
            [int]
            $DelayMinutes,

            [Parameter(Mandatory)]
            [string]
            $Reason
        )

        if ($Delay -gt 0) {
            $StatusObj.Status = 'Wait'                  # Set the 'Wait' Status & reason along with a wait timer expiration time
            $StatusObj.StatusReason = $Reason
            $StatusObj.WaitUntil = (Get-Date).AddMinutes($DelayMinutes);
        }
        else {
            $StatusObj.Status = 'Process'                  # Set the 'Wait' Status & reason along with a wait timer expiration time
            $StatusObj.StatusReason = $null
            $StatusObj.WaitUntil = (Get-Date);
        }
        return $StatusObj.Status
    }

    # Check the wait status in the object for an active timer.  If active, check for expiration and clear it.  
    # Return the final status 
    function Get-WaitTimer {
        param (
            [Parameter(Mandatory)]
            [PSCustomObject]
            $StatusObj
        )

        if ($StatusObj.Status -ieq 'Wait') {
            if ($StatusObj.WaitUntil -le (Get-Date)) { # If the wait timer has expired, mark object as ready to process
                $StatusObj.Status = 'Process'                 
                $StatusObj.StatusReason = $null
            }
        }
        return $StatusObj.Status
    }


    # #########################################################################################################################
    #   Internal function to check for any conditions that we should wait until the condition clears
    #
    function Get-WaitConditions {
        param (
            [Parameter(Position=0, Mandatory=$True)]  # Database name to shrink
            [string]
            $DBName
        )

        #   Test if the backup is currently being backed up and sleep if one is found
        #
        $sqlTestBckup = "
                SELECT 'Backup' AS [Event], 
                    DB_NAME(CAST(r.database_id AS INT)) AS [Reason], 
                    CAST(r.percent_complete AS BIGINT) AS Measure1, 
                    CAST(r.total_elapsed_time / 1000.0 / 60.0 AS BIGINT) AS [Measure2],
                    '' AS [Description]
                    FROM sys.dm_exec_requests r
                    WHERE r.command IN('BACKUP DATABASE') 
                    AND (DB_NAME(r.database_id) = '$($DBName)')
                UNION ALL
                SELECT 'Queue' AS [Event], 
                    ag.[name] AS [Reason], 
                    ISNULL(SUM(drs.log_send_queue_size), 0) AS LogSendQueueSize, 
                    ISNULL(SUM(drs.redo_queue_size), 0) AS RedoQueueSize, 
                    '' AS [Description]
                    FROM sys.availability_groups ag
                        INNER JOIN sys.availability_databases_cluster adc
                            ON (ag.group_id = adc.group_id) 
                        INNER JOIN sys.dm_hadr_database_replica_states drs
                            ON (ag.group_id = drs.group_id)
                    WHERE (adc.database_name = '$($DBName)')
                    GROUP BY ag.[name], 
                            adc.[database_name];
                ";

        $returnStatus = 'Process'       # By Default, return a PROCESS status unless an instance or database wait is detected

        $WaitEvents = Invoke-Sqlcmd -ServerInstance $SqlInstance -Database $DatabaseName -Query $sqlTestBckup
        foreach ($evnt in $WaitEvents) {
            switch ($evnt.Event.ToUpper()) {

                "BACKUP" {
                    Write-Host " -- Begin Sleeping -- Backup in progress"
                    Set-WaitTimer -StatusObj $dbCmplStatus -DelayMinutes $DelayBackup -Reason 'Backup in progress'
                    $returnStatus = 'Wait'
                }

                "QUEUE" {
                    $TotalQueueSize = $evnt.Measure1 + $evnt.Measure2
                    if ($TotalQueueSize -gt $QueueSizeMaxSize) {
                        Write-Host " -- Begin Sleeping -- Redo/Log Queue size exceeds max threshold"
                        Set-WaitTimer -StatusObj $dbCmplStatus -DelayMinutes $DelayQueue -Reason 'REDO/Log Queue'
                        $returnStatus = 'Wait'
                    }
                    elseif ($TotalQueueSize -lt $QueueSizeRecovered) {   
                        if (($dbCmplStatus.Status -eq 'Wait') -and ($dbCmplStatus.StatusReason -ieq 'REDO/Log Queue')) {
                            Set-WaitTimer -StatusObj $dbCmplStatus -DelayMinutes $DelayQueue 0 -Reason ''  # Clear the timer
                        }
                    } 
                }                    
                
            }
        }
        return $returnStatus   # Let the caller know what the status is.
    }

    
    # #########################################################################################################################
    #   Internal function to actually shrink the DB file
    #
    function DoShrinkFile {
        param (
            [Parameter(Position=0, Mandatory=$True)]  # Database name to shrink
            [string]
            $DBName,

            [Parameter(Position=1, Mandatory=$True)]  # Local database file name
            [PSCustomObject]
            $LocalFileInfo,

            [Parameter(Mandatory=$false)]             # Minutes of delay between shrinkfile operations
            [int]
            $Delay = 1
        )

        [int]$ShrinkOpCount = 0
        $continueLoop = $true
        do {
            #   Get the current file size information to determine if a shrinkfile is needed
            #
            $sqlFileSizes = "
                WITH DBI
                    AS (SELECT RTRIM(name) AS [SegmentName],
                            groupid AS [GroupId],
                            filename AS [FileName],
                            CAST(size / 128.0 AS DECIMAL(12, 2)) AS [MBAllocated],
                            CAST(FILEPROPERTY(name, 'SpaceUsed') / 128.0 AS DECIMAL(12, 2)) AS [MBUsed],
                            CAST((CAST(FILEPROPERTY(name, 'SpaceUsed') AS DECIMAL(12, 2)) / CAST(CASE
                                                                        WHEN [size] > 0 THEN
                                                                            [size]
                                                                        ELSE
                                                                            1.0
                                                                    END AS DECIMAL(12, 2))
                                    ) * 100.0 AS DECIMAL(12, 2)) AS [PercentUsed]
                        FROM sysfiles)
                    SELECT  MBAllocated,
                            MBUsed,
                            PercentUsed
                    FROM DBI
                    WHERE (SegmentName = '$($LocalFileInfo.SqlFile)');";

            $FileSize = Invoke-Sqlcmd -ServerInstance $SqlInstance -Database $DBName -Query $sqlFileSizes 
            $LocalFileInfo.MBAllocated = $FileSize.MBAllocated
            $LocalFileInfo.MBUsed      = $FileSize.MBUsed
            $LocalFileInfo.MBFree      = [Math]::Ceiling($LocalFileInfo.MBAllocated - $LocalFileInfo.MBUsed)

            # Create a SQLConnectionobject and wire up an event handler for InfoMessages
            #
            if ($LocalFileInfo.MBFree -gt $LocalFileInfo.MBMinimum) {

                if (($LocalFileInfo.MBFree - $LocalFileInfo.MBMinimum) -gt $ChunkSizeMB){
                    $NextSizeMB =  [Math]::Ceiling($LocalFileInfo.MBAllocated - $ChunkSizeMB)
                }
                else {
                    $NextSizeMB =  [Math]::Ceiling($LocalFileInfo.MBAllocated - ($LocalFileInfo.MBFree - $LocalFileInfo.MBMinimum) -1)
                }
                $sqlShrinkFile = "DBCC SHRINKFILE (N'$($LocalFileInfo.SqlFile)' , $($NextSizeMB)) -- Free space: $($LocalFileInfo.MBFree) MB"

                Write-Host (Get-Date) "--" $sqlShrinkFile
                $res = RunSqlQuery -DatabaseName $DBName -Query $sqlShrinkFile -CmdTimeout 1234
                $ShrinkOpCount++
                foreach ($iMsg in $res.Messages) {
                    if ($iMsg.Contains("read only")) {
                        Write-Host (Get-Date) "--> " $iMsg     
                        $LocalFileInfo.Status = 'Complete'
                        $LocalFileInfo.StatusReason = 'Read Only'
                        $continueLoop = $false
                    }
                    elseif ($iMsg.Contains("ghost records")) {
                        # Write-Host (Get-Date) "--> " $iMsg     
                        $LobPage = $iMsg -match "large object page (?<fn>\d+)\:(?<pg>\d+) "
                        $LocalFileInfo.LobID = "$($Matches.fn):$($Matches.pg)"
                        Set-WaitTimer -StatusObj $LocalFileInfo -DelayMinutes $DelayGhost -Reason "Ghost: $($LocalFileInfo.LobID)"
                        Write-Host (Get-Date) "-->> Ghost object remains -- $($LocalFileInfo.LobID) -- Skip to the next file"
                        $continueLoop = $false
                    }      
                    elseif ($iMsg.Contains("cannot be shrunk")) {
                        Set-WaitTimer -StatusObj $LocalFileInfo -DelayMinutes $DelayGhost -Reason "Cannot be shrunk at this time"
                        Write-Host (Get-Date) "-->> cannot be shrunk -- Skip to the next file"
                        $continueLoop = $false
                    }      
                    elseif ($iMsg.Contains("DBCC execution completed.")) {   #
                        # Write-Host (Get-Date) "--> " $iMsg     
                    }      
                    else {
                        Write-Host (Get-Date) "--> " $iMsg     
                        $continueLoop = $false
                    }      
                }
            }
            else {
                $LocalFileInfo.Status = 'Complete'
                $LocalFileInfo.StatusReason = 'Minimum Free Space Reached'
                Write-Host (Get-Date) " $($dbName) -- $($dbfKey) -- Completed -- Free: $($LocalFileInfo.MBFree) MB"
                $ShrinkOpCount++
                $continueLoop = $false
            }

            if ($continueLoop) { 
                $LocalFileInfo.MBFree -= $ChunkSizeMB           # Setup shrink threshold for the next iteration
                Start-Sleep -seconds ($Delay * 60)              # Sleep a bit before another run
            }
        } while ($continueLoop)
        return $ShrinkOpCount
    }


    # #########################################################################################################################
    #  Main Body
    # #########################################################################################################################
    #

    $MinFreeSpaceLockedMB =  $MinFreeLockedMB; # Stop the loop when reduce the free space to this level in MB for a growth locked file
    $MinFreeSpaceNoLockMB =  $MinFreeMB; # Stop the loop when reduce the free space to this level in MB for normal file

    $WaitSlice     = 30  # Number of SECONDS to wait before perfoming a test again
    $DelayBackup   = 2   # Number of MINUTES to wait for a backup to complete
    $DelayGhost    = 2   # Number of MINUTES to wait for ghost page cleanup before retrying the SHRINKFILE
    $DelayQueue    = 2   # Number of MINUTES to wait for the AG REDO/LOG queue size to diminish

    $CompletionStatus = @{};    # Per database/file completion status

    #   Get the list of files in the database except for the log file
    #
    $sqlFileList = "SELECT CAST(RTRIM(name) AS VARCHAR(256)) AS SqlFile,
                        CASE WHEN [growth] <> 0 THEN 1 ELSE 0 END AS FlgGrowth,
                        CAST(size / 128.0 AS DECIMAL(12, 2)) AS [MBAllocated],
                        CAST(FILEPROPERTY(name, 'SpaceUsed') / 128.0 AS DECIMAL(12, 2)) AS [MBUsed]                    FROM sysfiles
                        WHERE groupid >= 1
                        ORDER BY SqlFile;
    ";

    #   Build the shrink status object
    $CompletionStatus[$DatabaseName] = [PSCustomObject]@{
        Database = $DatabaseName
        Status   = "Process"
        StatusReason = $null
        WaitUntil = (Get-Date)
        Files   = @{}
    }

    # Obtain the names of all the database data files
    $FileList = Invoke-Sqlcmd -ServerInstance $SqlInstance -Database $DatabaseName -Query $sqlFileList 

    $dbStatus = $CompletionStatus[$DatabaseName];
    foreach ($dbfl in $FileList) {
        if ($dbfl.FlgGrowth -eq 1) { $MinFreeSpaceMB = [int] $MinFreeSpaceNoLockMB }
        else { $MinFreeSpaceMB = [int] $MinFreeSpaceLockedMB }

        $dbStatus.Files[$dbfl.SqlFile] = [PSCustomObject]@{
            Status      = "Process"
            StatusReason = $null
            SqlFile     = $dbfl.SqlFile
            Database    = $DatabaseName
            GrowthFlag  = $dbfl.FlgGrowth
            GhostLobID  = $null
            MBAllocated = $dbfl.MBAllocated
            MBUsed      = $dbfl.MBUsed
            MBFree      = [Math]::Ceiling($dbfl.MBAllocated - $dbfl.MBUsed)
            MBMinimum   = $MinFreeSpaceMB
            WaitUntil   = (Get-Date)
            LobID       = ''
        }
    }

    # Spin through the CompletionStatus list to find available work
    #
    
    [int] $ShrkOpCnt = 0
    $dbCompleted = 0
    :workLoop do {

        [int]$OpCnt = 0
        $dbWork = 0
  
        :dbLoop foreach ($dbName in $CompletionStatus.Keys) {
            $dbCmplStatus = $CompletionStatus[$dbName]
            if ($dbCmplStatus.Status -ine 'Complete') {
                $dbWork++;                                  # Count database eligible for shrink work
                if ((Get-WaitTimer -StatusObj $dbCmplStatus) -ieq 'Process') {   
                    $dbFilesCompleted = 0
                    :fileLoop foreach ($dbfKey in $dbCmplStatus.Files.Keys) {
                        $dbFile = $dbCmplStatus.Files[$dbfKey]
                        if ((Get-WaitTimer $dbFile) -ine 'Complete') {               # First check if the individual file is completed
                            while ((Get-WaitTimer $dbFile) -eq 'Process') {         # If the file is marked PROCESS, then call the shrink
                                if ((Get-WaitConditions -DBName $dbName) -ine 'Process') { # Check for database level wait conditions
                                    break fileLoop;                                          # Exit this fileLoop if DB WAIT detected
                                }
                                $DSOut = DoShrinkFile -DBName $dbName -LocalFileInfo $dbFile
                                $OpCnt = $OpCnt + ([int] "$($DSOut[1])")        Realllly stupid way to get just the integer result of "DoShrinkFile", pipeline issue
                            }
                        }
                        else {
                            $dbFilesCompleted++;  # Count the completed files
                        }
                    }
                    if ($dbFilesCompleted -eq $dbCmplStatus.Files.Count) {
                        $dbCompleted++
                        $OpCnt++
                        $dbCmplStatus.Status = 'Complete'
                        $dbCmplStatus.StatusReason = "Files Completed - $($dbFilesCompleted)"
                        Write-Host (Get-Date) " $($dbName) -- Database Completed"
                    }
                }
            }

        }

        if (($dbWork -gt 0) -and ($OpCnt -eq 0)) {  # did we have work eligible databases and no shrink work done?
            Start-Sleep -Seconds ($WaitSlice)       # The wait for the next pass
        }

    } until ($dbCompleted -eq $CompletionStatus.Keys.Count)

}



# 9/1/2021 3:17:44 PM -->  DBCC SHRINKFILE: Not all ghost records on the large object page 5:45124176 could be removed. If there are active queries on re
# adable secondary replicas check the current ghost cleanup boundary.

Compress-DbaFileSize 'PBG1SQL20V104' 'ProcessData' 

#Compress-DbaFileSize 'EDR1SQL01S341\SLT' 'StagingEDW' -ChunkSizeMB 100 -MinFreeMB 800  

#Compress-DbaFileSize 'PBG1SQL01V105' 'ProcessData' 
