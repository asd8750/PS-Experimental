 <#
.SYNOPSIS
    Script SQL Server Database

.DESCRIPTION
    Produce DDL Scripts for a given ( or all ) database(s) of a SQLServer Instance
    Including DbCreation statements

.PARAMETER  <Parameter-Name>
    If bound parameters, no need to put them overhere

.EXAMPLE
    powershell .\ALZDBA_ScriptDb_batch.ps1 -Database 'MyDatabase' -SQLServer 'myserver\myinstance' -Filepath = '\\Safezone\SQLServerScriptBackups'

.EXAMPLE
    powershell .\ALZDBA_ScriptDb_batch.ps1 -Database '*' -SQLServer 'myserver\myinstance' -Filepath = '\\Safezone\SQLServerScriptBackups'

.NOTES
    -Date 2012-10-04 - Author Bijnens Johan
    https://www.sqlservercentral.com/articles/scripting-sql-server-databases-with-smo-using-enforcescriptingoptions-1  

#>
[CmdletBinding()]
Param([parameter(Mandatory=$true,HelpMessage="Path to file")] [string]$Filepath, 
      [parameter(Mandatory=$true,HelpMessage="SQLServer name")] [string]$SQLServer, 
      [parameter(Mandatory=$true,HelpMessage="Database name or *")] [string]$Database  
      )

Trap {
  # Handle the error
  $err = $_.Exception
  write-host $err.Message
  while( $err.InnerException ) {
	   $err = $err.InnerException
	   write-host $err.Message
	   };
  # End the script.
  break
  }

function Invoke-ScriptSQLDb (  $db, $Filepath ) {
    $SQLSMO="Microsoft.SqlServer.Management.Smo" 
    # check target locations 
    $ScriptPath = $( Get-ScriptPath $Filepath $db.Parent.Name )

	$CreationScriptOptions = new-object ("$SQLSMO.ScriptingOptions") 
	$CreationScriptOptions.ContinueScriptingOnError = $true # may be needed if encrypted objects exist

	#result file
	$CreationScriptOptions.FileName = $("{0}{1}_Script.sql" -f $ScriptPath, $db.name );   
	$DBAWarning = $('/* 
-- Powershell ALZDBA_ScriptDb_batch.ps1 V1.0 - Script date {0}
Begin processing  
   /Server:[{1}] - {4}
   /Database:[{2}] 
   /TargetFile:[{3}] 
-- ALZScript ende Title
*/

-- REMARK 
   ------- 
 Always check DATABASE-FILES to point to the correct drives and folders !
-- 
-- Excuses always come when it''s to late --
-- 
-- End of remark

 use Master -- gebruik MasterDB
GO 
' -f $(Get-Date -Format "yyyy-MM-dd hh:mm:ss" ), $SQLServer, $db.name, $CreationScriptOptions.FileName, $db.Parent.VersionString )
 
	Out-File -InputObject $DBAWarning  -FilePath $CreationScriptOptions.FileName -Force 
	
	#Overwrite the file if needed
	$CreationScriptOptions.ScriptOwner = $true                                                                                                             
	$CreationScriptOptions.ToFileOnly = $true                                                                                                              
	$CreationScriptOptions.AppendToFile = $true
	
	#Script Db Creation
	$db.Script($CreationScriptOptions)
	
	#add ending GO
	Out-File -InputObject $('use [{0}] {1}GO' -f $db.Name, "`n") -FilePath $CreationScriptOptions.FileName -Append  
	
# set scripting options for database objects 
	##	$CreationScriptOptions.Add = $true 
	#	$CreationScriptOptions.Equals = $true 
	##	$CreationScriptOptions.GetHashCode = $true 
	##	$CreationScriptOptions.GetType = $true 
	#	$CreationScriptOptions.Remove = $true 
	#	$CreationScriptOptions.SetTargetDatabaseEngineType = $true 
	#	$CreationScriptOptions.SetTargetServerVersion = $true 
	#	$CreationScriptOptions.ToString = $true 
	#	$CreationScriptOptions.AgentAlertJob = $true 
	#	$CreationScriptOptions.AgentJobId = $true 
	#	$CreationScriptOptions.AgentNotify = $true 
	#	$CreationScriptOptions.AllowSystemObjects = $true 
	#	$CreationScriptOptions.AnsiFile = $true 
		$CreationScriptOptions.AnsiPadding = $true 
		$CreationScriptOptions.AppendToFile = $true 
	#	$CreationScriptOptions.BatchSize = $true 
		$CreationScriptOptions.Bindings = $true 
		$CreationScriptOptions.ChangeTracking = $true 
	#	$CreationScriptOptions.ClusteredIndexes = $true 
		$CreationScriptOptions.ContinueScriptingOnError = $true # to avoid failures with encrypted objects
	#	$CreationScriptOptions.ConvertUserDefinedDataTypesToBaseType = $true 
	#	$CreationScriptOptions.DdlBodyOnly = $true 
	#	$CreationScriptOptions.DdlHeaderOnly = $true 
	#	$CreationScriptOptions.Default = $true 
		$CreationScriptOptions.DriAll = $true 
	#	$CreationScriptOptions.DriAllConstraints = $true 
	#	$CreationScriptOptions.DriAllKeys = $true 
	#	$CreationScriptOptions.DriChecks = $true 
	#	$CreationScriptOptions.DriClustered = $true 
	#	$CreationScriptOptions.DriDefaults = $true 
	#	$CreationScriptOptions.DriForeignKeys = $true 
	#	$CreationScriptOptions.DriIncludeSystemNames = $true 
	#	$CreationScriptOptions.DriIndexes = $true 
	#	$CreationScriptOptions.DriNonClustered = $true 
	#	$CreationScriptOptions.DriPrimaryKey = $true 
	#	$CreationScriptOptions.DriUniqueKeys = $true 
	#	$CreationScriptOptions.DriWithNoCheck = $true 
	#	$CreationScriptOptions.Encoding = $true 
	##	$CreationScriptOptions.EnforceScriptingOptions = $true 
		$CreationScriptOptions.ExtendedProperties = $true 
	#	$CreationScriptOptions.FileName = "$($FilePath)\$($Database)_Script.sql"; 
		$CreationScriptOptions.FullTextCatalogs = $true 
		$CreationScriptOptions.FullTextIndexes = $true 
		$CreationScriptOptions.FullTextStopLists = $true 
	#-	$CreationScriptOptions.IncludeDatabaseContext = $true--> alzdba CAUSES ScriptTransfer TO FAIL !!
		$CreationScriptOptions.IncludeDatabaseRoleMemberships = $true 
		$CreationScriptOptions.IncludeFullTextCatalogRootPath = $true 
		$CreationScriptOptions.IncludeHeaders = $true 
	#	$CreationScriptOptions.IncludeIfNotExists = $true 
		$CreationScriptOptions.Indexes = $true 
		$CreationScriptOptions.LoginSid = $true 
	#	$CreationScriptOptions.NoAssemblies = $true 
	#	$CreationScriptOptions.NoCollation = $true 
	#	$CreationScriptOptions.NoCommandTerminator = $true 
	#	$CreationScriptOptions.NoExecuteAs = $true 
	#	$CreationScriptOptions.NoFileGroup = $true 
	#	$CreationScriptOptions.NoFileStream = $true 
	#	$CreationScriptOptions.NoFileStreamColumn = $true 
	#	$CreationScriptOptions.NoIdentities = $true 
	#	$CreationScriptOptions.NoIndexPartitioningSchemes = $true 
	#	$CreationScriptOptions.NoMailProfileAccounts = $true 
	#	$CreationScriptOptions.NoMailProfilePrincipals = $true 
	#	$CreationScriptOptions.NonClusteredIndexes = $true 
	#	$CreationScriptOptions.NoTablePartitioningSchemes = $true 
	#	$CreationScriptOptions.NoVardecimal = $true 
	#	$CreationScriptOptions.NoViewColumns = $true 
	#	$CreationScriptOptions.NoXmlNamespaces = $true 
	#	$CreationScriptOptions.OptimizerData = $true 
		$CreationScriptOptions.Permissions = $true 
	#	$CreationScriptOptions.PrimaryObject = $true 
		$CreationScriptOptions.SchemaQualify = $true 
		$CreationScriptOptions.SchemaQualifyForeignKeysReferences = $true 
		$CreationScriptOptions.ScriptBatchTerminator = $true 
	#	$CreationScriptOptions.ScriptData = $true 
		$CreationScriptOptions.ScriptDataCompression = $true 
	#	$CreationScriptOptions.ScriptDrops = $true 
		$CreationScriptOptions.ScriptOwner = $true 
		$CreationScriptOptions.ScriptSchema = $true 
		$CreationScriptOptions.Statistics = $true 
	#	$CreationScriptOptions.TargetDatabaseEngineType = $true 
	#	$CreationScriptOptions.TargetServerVersion = $true 
	#	$CreationScriptOptions.TimestampToBinary = $true 
		$CreationScriptOptions.ToFileOnly = $true 
		$CreationScriptOptions.Triggers = $true 
	#	$CreationScriptOptions.WithDependencies = $true 
		$CreationScriptOptions.XmlIndexes = $true 


	$transfer = new-object ("$SQLSMO.Transfer") $db
	$transfer.options=$CreationScriptOptions # tell the transfer object of our preferences
	$returnValue = $transfer.ScriptTransfer() 
	
	
	#Check disabled triggers
    $Script:DisabledTriggers = @()
    $db.tables | %{ 
        $CurrentTable = $_.Name
        $CurrentSchema = $_.Schema 
        foreach ( $trg in $_.triggers ) {
            if ( $trg.Properties | WHERE-object { $_.Name -eq 'IsEnabled' -and $_.Value -eq $false } ) {
                $Script:DisabledTriggers += $('/* Disabled trigger !!! */ 
DISABLE TRIGGER [{0}] ON [{1}].[{2}] ; 
GO
' -f $trg.Name, $CurrentSchema, $CurrentTable  )
                }
            }
        }

		Out-File -InputObject $Script:DisabledTriggers -FilePath $CreationScriptOptions.FileName -Append  


	}

function invoke-CheckCreate-Path ( [parameter(Position=0,Mandatory=$true)][string] $iPath ) {
#check if path for exists and create if needed
	if ((Test-Path -path $iPath) -ne $True) {
		Write-verbose "Creating folder $iPath"
		Try { New-Item "$iPath" -type directory | out-null 
			}  
        Catch [system.exception]{
	  			Write-Error "error while Creating folder [$iPath].  $_"
                return
                } 
		}
	else { 
		Write-verbose "folder $iPath OK" 
		}
	}

function Get-ScriptPath ( [parameter(Position=0,Mandatory=$true)][String] $Filepath 
						, [parameter(Position=1,Mandatory=$true)][String] $SQLServer ) {
	$w = $SQLServer.Replace('.mydomain.com','')
	
	$newpath = $Filepath + '\' + $w.Replace('\','_') + '\'
	invoke-CheckCreate-Path $( $Filepath + '\' + $w.Replace('\','_') + '\' )
	$newpath
	}


function Invoke-ScriptSQLServer ( $SQLInstance, $DatabaseName, $Filepath ) {

	if ( (!( $DatabaseName )) -or $DatabaseName.trim -eq '' ) { 
		$DatabaseName = '*' 
		}
	
	# check target locations 
	invoke-CheckCreate-Path $Filepath

	# Load SMO assembly, and if we're running SQL 2008 DLLs load the SMOExtended and SQLWMIManagement libraries
	$v = [System.Reflection.Assembly]::LoadWithPartialName( "Microsoft.SqlServer.SMO")
	if ((($v.FullName.Split(','))[1].Split('='))[1].Split('.')[0] -ne '9') {
		[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMOExtended") | out-null
	   }
	   
	$s = new-object ("Microsoft.SqlServer.Management.Smo.Server") $SQLInstance 
	if ($s.Version -eq  $null ){Throw "Can't find the instance $SQLServer"}

	[int]$ctr=0
	[int]$pct=0
	if ( $DatabaseName -eq '*' ) {
		foreach ( $db in $s.Databases ) {
			$ctr+=1
			$pct=$ctr*100/$s.Databases.count
			
			if ( $db.name -eq 'master' -or $db.name -eq 'msdb' -or $db.name -eq 'model' -or $db.name -eq 'tempdb' -or $db.name -eq 'ddbaserverping'){
				Write-Verbose $('SystemDb/DBADb excluded when ALLdb is selected {0} - {1}' -f $db.name, $SQLInstance )
				}
			else {
				Write-Progress -Status $db.name -PercentComplete $pct -Activity 'Scripting Db'
				Write-Verbose $('Scripting {0} - {1}' -f $db.name, $SQLInstance )
				Invoke-ScriptSQLDb $db $FilePath 
				}
			}
		}
	else {
		try {
			$pct = 100
			#$x = $s.Databases | Where-Object { $_.Name -eq "$DatabaseName" } | Select @{Label="Name";Expression={$_.Name }} | sort Name 
			if ( ($s.Databases | Where-Object { $_.Name -eq "$DatabaseName" }) ) {
				$db= $s.Databases[$DatabaseName] 
				Write-Progress -Status $db.name -PercentComplete $pct -Activity 'Scripting Db'
				Invoke-ScriptSQLDb $db $FilePath 
				}
			else {
				Throw $("Can't find the database [{0}] in SQLServer {1} " -f $DatabaseName, $SQLInstance ) 
				}
			}
		catch {
			Throw $_ 
			}
		}
	}


##### Start of script execution ###
#Clear-Host 
#$VerbosePreference = "Continue"
Invoke-ScriptSQLServer -SQLInstance $SQLServer -DatabaseName $Database -Filepath $FilePath

write-host "All done"

#Show result files in explorer
#Invoke-Item $FilePath