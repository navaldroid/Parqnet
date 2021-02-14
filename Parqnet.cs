using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.IO;
using Parquet;


namespace Parqnet
{
    public static class Parqnet
    {
        private static DataTable readParquet(string filename)
        {
            DataTable dt = new DataTable();
            using (Stream fileStream = File.OpenRead(filename))
            {
                using (var parquetReader = new Parquet.ParquetReader(fileStream))
                {
                    Parquet.Data.DataField[] dataFields = parquetReader.Schema.GetDataFields();
                    foreach (var df in dataFields)
                        dt.Columns.Add(new DataColumn(df.Name, df.ClrType));

                    Parquet.Data.DataColumn[] columns = parquetReader.ReadEntireRowGroup();
                    int n_columns = columns.Length;
                    List<ArrayList> data_in_columns = new List<ArrayList>(n_columns);
                    foreach (var dc in columns)
                        data_in_columns.Add(new ArrayList(dc.Data));

                    int n_rows = data_in_columns[0].Count;
                    for (int i = 0; i < n_rows; i++)
                    {
                        object[] values = new object[n_columns];
                        for (int j = 0; j < n_columns; j++)
                            values[j] = data_in_columns[j][i];
                        dt.Rows.Add(values);
                    }
                }
            }
            return dt;
        }

        public static DataTable readParquets(string path, DateTime? start_date)
        {
            DataTable table = null;
            foreach (string f in Directory.GetFiles(path, "*.parquet").OrderBy(x => x))
            {
                DateTime dt = DateTime.ParseExact(Path.GetFileNameWithoutExtension(f), "yyyyMMdd_HHmmss", System.Globalization.CultureInfo.InvariantCulture);
                if (start_date.HasValue && dt < start_date)
                    continue;
                DataTable _tab = readParquet(f);
                if (table == null)
                    table = _tab;
                else
                    table.Merge(_tab);
            }
            return table;
        }

        public static void WriteParquet(DataTable dt, string filename, bool skip_unchanged = true)
        {
            // Parquet schema
            List<Parquet.Data.Field> fields = new List<Parquet.Data.Field>();
            foreach (DataColumn dc in dt.Columns)
            {
                Type T = dc.DataType;
                Parquet.Data.DataField df = (Parquet.Data.DataField)Activator.CreateInstance(typeof(Parquet.Data.DataField<>).MakeGenericType(T), dc.ColumnName);
                fields.Add(df);
            }
            Parquet.Data.Schema schema = new Parquet.Data.Schema(fields);
            Parquet.Data.Rows.Table table = new Parquet.Data.Rows.Table(schema);

            // Row data
            foreach (DataRow dr in dt.Rows)
            {
                if (skip_unchanged && dr.RowState == DataRowState.Unchanged)
                    continue;
                table.Add(new Parquet.Data.Rows.Row(dr.ItemArray));
            }

            // Write parquet
            using (Stream fileStream = File.OpenWrite(filename))
            using (var parquetWriter = new Parquet.ParquetWriter(table.Schema, fileStream))
                parquetWriter.Write(table);

            if (skip_unchanged)
                dt.AcceptChanges();
        }
    }
}
