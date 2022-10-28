$ScriptPath = Split-Path (split-path $MyInvocation.MyCommand.Definition -Parent) -Parent
#$ScriptPath = split-path -parent $MyInvocation.MyCommand.Definition

$env:PSModulePath
#$ScriptPath = 'C:\Users\fs111257\Documents\PowerShell\Modules;C:\Program Files\PowerShell\Modules;c:\program files\powershell\6\Modules;C:\Windows\system32\WindowsPowerShell\v1.0\Modules;c:\Users\fs111257\.vscode\extensions\ms-vscode.powershell-2019.9.0\modules'

if ($env:PSModulePath.Contains(";" + $ScriptPath) -eq $false) {
    $env:PSModulePath = $env:PSModulePath + ";" + $ScriptPath
}

if ( $null -ne (Get-Module -Name "FS_Deployment")) {
    Remove-Module  "FS_Deployment" -Force
}

$FSDebug = 4

Import-Module  "FS_Deployment" # -Verbose 
$PSModuleRoot
$PSDATA

$PSVersionTable | FT

$cfg = Get-FSDeploymentConfig -FullInstanceName  "EDR1SQL01S003.fs.local\DBA"

#[byte[]] $Cmprsd = 
Checkpoint-FSDeployDirectories -FullInstanceName  "EDR1SQL01S003.fs.local\DBA" -Directory "C:\Users\fs111257\Documents\Deploy Scripts" -DeployDirID 0
#$Cmprsd.Length
