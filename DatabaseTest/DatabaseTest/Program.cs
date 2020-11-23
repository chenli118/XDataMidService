using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;

namespace DatabaseTest
{
    class Program
    {
        static string conStr = "Server=192.168.1.239;uid=sa;pwd=easXdata; database=master;pooling=true;min pool size=5;max pool size=512;connect timeout = 20;";
        static void Main(string[] args)
        {

            CreateByBak("XDataTemp"); 

        }
        private static void CreateByBak(string dbname)
        { 

            string sql = "RESTORE DATABASE ["+ dbname + "]";
            sql = sql + @" FROM  DISK = N'D:\Database\backup\RestoreXDataTemp_wc.bak' ";
            sql = sql + " WITH  FILE = 1, NOUNLOAD,  "; 
            sql = sql + @" MOVE 'XDataTemp' TO 'D:\Database\data\" + dbname + "_Data.mdf', ";
            sql = sql + @" MOVE 'XDataTemp_log' TO 'D:\Database\Logs\" + dbname + "_log.ldf', ";
            sql = sql + " MAXTRANSFERSIZE = 4194304,BUFFERCOUNT = 100, RECOVERY,REPLACE,STATS = 10; "; 
            try
            {
                ExecuteSqlWithGoSplite(sql, conStr);
                Console.WriteLine("restored ! :" );
            }
            catch(Exception err)
            {
                Console.WriteLine("bad!");
            }
        }
        private static void CreateByScript()
        {
            try
            {
                var rd = new System.Random();
                string dname = rd.Next(100, 999999999).ToString();
                string s1 = " create database ["+ dname + "] ";
                int ret = ExecuteSqlWithGoSplite(s1, conStr);

                SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(conStr);
                csb.InitialCatalog = dname;
                conStr = csb.ConnectionString;
                var StaticStructAndFn = Path.Combine(Directory.GetCurrentDirectory(), "StaticStructAndFn.tsql");
                var sqls = File.ReadAllText(StaticStructAndFn);
                ExecuteSqlWithGoSplite(sqls, conStr);
                Console.WriteLine(dname+" created !");
            }
            catch
            {
                Console.WriteLine("bad!");
            }

        }
        private static void BackUpByScript()
        {
            try
            {
                string s1 = " BACKUP DATABASE [XDataTemp]  ";
                s1 = s1 + @" TO  DISK =  N'D:\Database\backup\RestoreXDataTemp_wc.bak'";
                s1 = s1 + " WITH NOFORMAT, ";
                s1 = s1 + " INIT, ";
                s1 = s1 + " NAME = N'RestoreXDataTempFull Database Backup',";
                s1 = s1 + " SKIP,";
                s1 = s1 + " NOREWIND,";
                s1 = s1 + " NOUNLOAD,";
                s1 = s1 + " COMPRESSION, ";
                s1 = s1 + " STATS = 10"; 
                int ret = ExecuteSqlWithGoSplite(s1, conStr);
                 
                Console.WriteLine("XDataTemp Backup !");
            }
            catch
            {
                Console.WriteLine("bad!");
            }

        }
        public static int ExecuteSqlWithGoSplite(string sql, string conStr)
        {
            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection(conStr))
            {
                try
                {
                    try
                    {
                        if (conn.State != ConnectionState.Open) conn.Open();
                    }
                    catch
                    {
                        if (conn.State != ConnectionState.Open) conn.Open();
                    }
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand(sql, conn);
                    foreach (var sqlBatch in sql.Split(new[] { "GO", "go" }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        sqlCommand.CommandText = sqlBatch;
                        sqlCommand.ExecuteNonQuery();
                    }
                    return 1;
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.Message);
                }
                finally
                {
                    conn.Close();
                    conn.Dispose();
                }

            }
        }
    }
}
