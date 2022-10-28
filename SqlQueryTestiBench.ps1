
Import-Module SqlServer


$SqlInstanceName = "DMT1SQL01V105.fs.local"
$Database = "master"
$InfoQuery = "SELECT * FROM sys.databases;"
$Database = "ModuleAssembly"
$InfoQuery = "SELECT TOP (1) * FROM dbo.Workflow;"



$sqlConn = New-Object System.Data.SqlClient.SqlConnection
$sqlConn.ConnectionString = "Server=$($SqlInstanceName);Database=$($Database);Integrated Security=True;"
$sqlConn.Open();
$sqlCmd = $sqlConn.CreateCommand()
$sqlCmd.CommandText = $InfoQuery
$sqlRdr = $sqlCmd.ExecuteReader()

$qrySchema = New-Object System.Data.Datatable
$qrySchema = $sqlRdr.GetSchemaTable()

$qryData = New-Object System.Data.Datatable
$qryData.Load($sqlRdr);

$sqlCmd.Dispose();

$sqlConn.Close();
$sqlConn.Dispose();

$Results = [PSCustomObject]@{
    Rows = $qryData.Rows.Count
    Columns = @();
}

$qrySchema.Rows | Select-Object { 
    $CInfo = [PSCustomObject]@{
        ColumnName = $_.ColumnName
        ColumnOrdinal = $_.ColumnOrdinal
        ColumnSize = $_.ColumnSize
        Precision = $_.NumericPrecision
        Scale = $_.NumericScale
        ProvDataType = "System."+$_.ProviderSpecificDataType.Name -replace 'Sql',''
        DataType = $_.DataTypeName
        SqlDef =  $_.DataTypeName
    }
    switch ($CInfo.DataType) {
        'decimal' {$CInfo.SqlDef = "decimal($($CInfo.Precision),$($CInfo.Scale))"}
        'numeric' {$CInfo.SqlDef = "numeric($($CInfo.Precision),$($CInfo.Scale))"}
        'float' {$CInfo.SqlDef = "float($($CInfo.Precision))"}
        'real' {$CInfo.SqlDef = "real($($CInfo.Precision))"}
        'varchar' { if ($CInfo.ColumnSize -le 8000) {
                $CInfo.SqlDef = "varchar($($CInfo.ColumnSize))" 
            } else {
                $CInfo.SqlDef = "varchar(MAX)" 
            }
        }
        'nvarchar' { if ($CInfo.ColumnSize -le 4000) {
                $CInfo.SqlDef = "nvarchar($($CInfo.ColumnSize))" 
            } else {
                $CInfo.SqlDef = "nvarchar(MAX)" 
            }
        }
        'varbinary' { if ($CInfo.ColumnSize -le 8000) {
                $CInfo.SqlDef = "varbinary($($CInfo.ColumnSize))" 
            } else {
                $CInfo.SqlDef = "varbinary(MAX)" 
            }
        }
        'binary' {$CInfo.SqlDef = "binary($($CInfo.ColumnSize))"}
        'datetime2' {$CInfo.SqlDef = "datetime2($($CInfo.Scale))"}
        'datetimeoffset' {$CInfo.SqlDef = "datetimeoffset($($CInfo.Scale))"}
        'rowversion' {$CInfo.SqlDef = "bigint" }
        'timestamp' {$CInfo.SqlDef = "bigint" }
        }

    $Results.Columns += $CInfo
}

$a=1
