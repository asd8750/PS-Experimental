
function Extract-MaximoImages  {
    <#
.SYNOPSIS

Extract-MaximoImage  -  extract an image from binary data in the Maximo DB and store it in a file 

.DESCRIPTION

.PARAMETER InstanceName
Specifies the SQL instance to test and modify

.PARAMETER DatabaseName
Specifies the SQL database name

.PARAMETER OutputDirectory
Directory path to contain the generated image files

.PARAMETER MaxImages
Max number of images returned (DEFAULT = 10)

.OUTPUTS

JPG files

.EXAMPLE

PS> Extract-MaximoImage -InstanceName Pbg1sql02s320.qa.fs -DatabaseName MaxQa -OutputFolder \\share\folder\

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $InstanceName ,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $DatabaseName ,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] $Itemnum = "",

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [int] $MaxImages = 10,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] $FromItem = "",

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] $ToItem = "",

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] $OutputDirectory 
    )


    process {
        #   Verify the OutputFolder exists
        #
        if (-not $OutputDirectory.EndsWith('\')) { $OutputDirectory = $OutputDirectory + '\'}
        if ( -not (Test-Path -LiteralPath $OutputDirectory)) {
            Write-Error "Cannot find OutputFolder: '$($OutputDirectory)'"
            return
        }

        #   Return one item
        if ($Itemnum -eq '') {
            $WhereCondition = "(1=1)"
        }
        else {
            $WhereCondition = "i.itemnum = '$($Itemnum)'"
        }

        #  Return a range to items
        if (($FromItem -ne '') -and ($ToItem -ne '')) {
            $WhereCondition = "(i.itemnum BETWEEN '$($FromItem)' AND '$($ToItem)')"
        }

        #   Query to extract 
        $ExtractQuery = @"
            SELECT  TOP ($($MaxImages))
                    im.image,
                    im.imagename,
                    i.itemnum,
                    i.status
                FROM imglib im
                    JOIN item i
                    ON i.itemid = im.refobjectid
                        AND im.refobject = 'ITEM'
                WHERE
                    (status = 'active')
                    AND ( $($WhereCondition) )
                ORDER BY i.itemnum;
"@

        #   Build the SQLConnection and SqlCommand objects to let us submit a query to the database
        # 
        $sqlConn = New-Object System.Data.SqlClient.SqlConnection 
        $sqlConn.ConnectionString = "Server=$($InstanceName);Database=$($DatabaseName);Integrated Security=True;"
        $sqlConn.Open();
        $sqlCmd = $sqlConn.CreateCommand()
        $sqlCmd.CommandText = $ExtractQuery
        $sqlRdr = $sqlCmd.ExecuteReader()       #Submit the Query

        #   Now loop and process each image. 
        #   Using a SqlReader instead of a DataTable to avoid caching all images in memory
        #
        $FilesWritten = 0
        while ($sqlRdr.Read()) {
            $imgBytes = $sqlRdr.GetSqlBinary($sqlRdr.GetOrdinal('image'));   # Get the image binary 
            $itemNumDB = $sqlRdr.GetString($sqlRdr.GetOrdinal('itemnum'));   # Get the itemnum  
            $FileName = $OutputDirectory + $itemNumDB + '.jpg'
            if (-not(Test-Path -LiteralPath $FileName)) {
                [IO.File]::WriteAllBytes($FileName, $imgBytes.Value);   # Write out the image if it doesn't already exist
                ++$FilesWritten
            }
            else {
                Write-Verbose " Image exists - skipped: $($FileName)"               
            }
        }
        
        $sqlConn.Close();       # Clean up before exit
        $sqlCmd.Dispose();
        $sqlConn.Dispose();
        Write-Host "Image files written: $($FilesWritten)"
    }

}

Extract-MaximoImages -InstanceName "Pbg1sql02s320.qa.fs" -DatabaseName "MaxQA" -OutputDirectory 'C:\Temp\Maximo' -MaxImages 20 -FromItem '6003323' -ToItem '60036025' -VERBOSE

