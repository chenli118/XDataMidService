﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XDataMidService.BPImp
{
    internal class SqlServerHelper
    {
        public static string GetLinkServer(string conStr, string sName, string logName, string pwd, string ipAddress)
        {
            string sql = string.Format(" exec sp_addlinkedserver '{0}','','SQLOLEDB','{3}'  " +
                " exec sp_addlinkedsrvlogin '{0}', 'false', NULL, '{1}', '{2}'", sName, logName, pwd, ipAddress);
            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection(conStr))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    string cmdText = "SELECT 1 FROM sys.servers WHERE name='"+sName+"'";
                    System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand(cmdText,conn);
                    var s = sqlCommand.ExecuteScalar();
                    if (s!=null && int.Parse(s.ToString()) == 1)
                    {
                    
                    }
                    else
                    {

                        sqlCommand.CommandText = sql;
                        sqlCommand.ExecuteNonQuery();
                    }
                    return sName;
                }
                catch (Exception err)
                {
                    throw err;
                }
            }
        }

        public async static Task SqlBulkCopy(System.Data.DataTable dt, string conStr)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(conStr))
                {
                    connection.Open();
                    using (var tran = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        using (SqlBulkCopy copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, tran))
                        {
                            copy.DestinationTableName = dt.TableName;

                            foreach (DataColumn column in dt.Columns)
                            {
                                copy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(column.ColumnName, column.ColumnName));
                            }
                            copy.BulkCopyTimeout = 1000*1200;
                             await copy.WriteToServerAsync(dt);
                            tran.Commit();
                            connection.Close();
                        }
                    }
                }               
            }
            catch (Exception err)
            {
                throw err;
            }

        }
        public static int ExecuteProcWithStruct(string pName, string conStr,string typeName,object pValues)
        {
            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection(conStr))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand();
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    SqlParameter tvpParam = sqlCommand.Parameters.AddWithValue("tvpNewValues", pValues);
                    tvpParam.SqlDbType = SqlDbType.Structured;
                    tvpParam.TypeName = typeName;
                    sqlCommand.CommandText = pName;
                    sqlCommand.Connection = conn;
                    sqlCommand.ExecuteNonQuery();
                     
                }
                catch (Exception Err)
                {
                    throw Err;
                }
                
                }
            return -1;
        }
        private static string singleUserCmd = "alter database db-name set SINGLE_USER";
        private static string multiUserCmd = "alter database db-name  set MULTI_USER";

        public static SqlConnection SetSingleUser(bool singleUser, SqlConnectionStringBuilder csb)
        {
            string v;
            if (singleUser)
            {
                v = singleUserCmd.Replace("db-name", csb.InitialCatalog);
            }
            else
            {
                v = multiUserCmd.Replace("db-name", csb.InitialCatalog);
            }
            SqlConnection connection = new SqlConnection(csb.ToString());
            SqlCommand cmd = new SqlCommand(v, connection);

            cmd.Connection.Open();
            cmd.ExecuteNonQuery();

            return connection;
        }

        public static DataTable GetTableBySql(string sql, string conStr)
        {
            DataTable dt = new DataTable();
            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection(conStr))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand(sql, conn);
                    sqlCommand.CommandText = sql;
                    IDataReader dataReader = sqlCommand.ExecuteReader();
                    dt.Load(dataReader);
                    dataReader.Close();
                    return dt;
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
            return dt;
        }

        public static int ExecuteSql(string sql, string conStr)
        {
            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection(conStr))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand(sql, conn);
                    foreach (var sqlBatch in sql.Split(new[] { "GO" ,"go"}, StringSplitOptions.RemoveEmptyEntries))
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
                finally{
                    conn.Close();
                    conn.Dispose();
                }

            }
        }
    }
}
