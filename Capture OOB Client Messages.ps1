function ExecuteNonQuery {
    param (
        [Parameter(Mandatory)]
        [System.Data.SqlClient.SqlCommand]
        $SqlCmdObj,
       
        [Parameter(Mandatory)]
        [String]
        $Query,

        [Parameter]
        [int]
        $Timeout = 1800  #command timeout
    )
    $script:ErrorMsgs = @();
    $script:ErrorMsgs += $Query;
    $SqlCmdObj.CommandText = $Query;$SqlCmdObj.CommandTimeout = $Timeout
    $res = $SqlCmdObj.ExecuteNonQuery();
}

$ErrorMsgs = @()

$conn = New-Object System.Data.SqlClient.SqlConnection "Server=EDR1SQL01S004\DBA;Database=MSXJobManagementV2;Integrated Security=SSPI;";

## Attach the InfoMessage Event Handler to the connection to write out the messages
$handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {param($sender, $event) 
    #Write-Host "-->> " $event.Message;
    $script:ErrorMsgs += $event.Message
};
$conn.add_InfoMessage($handler);
$conn.FireInfoMessageEventOnUserErrors = $true;

## 
$conn.Open();

$cmd = $conn.CreateCommand();

ExecuteNonQuery $cmd "PRINT 'This is the message from the PRINT statement'"
#$ErrorMsgs

ExecuteNonQuery $cmd "RAISERROR('This is the message from the RAISERROR statement', 10, 1)"
#$ErrorMsgs

ExecuteNonQuery $cmd "DBCC SHRINKFILE (N'MSXJobManagementV2' , 2400)"
$ErrorMsgs

$conn.Close();
