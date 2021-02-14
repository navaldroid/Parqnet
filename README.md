# Parqnet
Basic functions to read/write a DataTable to/from parquet

## Ever wanted to read some parquet(s) file(s) to a DataTable?
Here you go!
Just call static Parqnet.readParquet(string filename)
And if you have a dirful of parquet, you call readParquets
## Saving DataTable to parquet?
Achieved via WriteParquet()
## It all works on top of...
https://github.com/aloneguid/parquet-dotnet
