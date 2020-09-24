using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace XDataMidService.BPImp
{
    public class XDataBP
    {
        public static string connectString { get; set; }
        public static void DropDB(Models.xfile xfile)
        {
            connectString = StaticUtil.GetConfigValueByKey("XDataConn");
            string dbname = StaticUtil.GetLocalDbNameByXFile(xfile);
            string sql = " if exists(select 1 from sys.databases where name = '"+ dbname + "') begin exec dropdb '"+ dbname + "' end";
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    cmd.ExecuteNonQuery();
                }
                catch (Exception err)
                {
                    throw err;
                }
            }
        }
        public static void InsertBadFile(Models.xfile xfile, string ErrMsg)
        { 
            connectString = StaticUtil.GetConfigValueByKey("XDataConn");
            string sql = string.Format(" insert into XData.dbo.[badfiles](XID, [CustomID] ,[CustomName] ,[FileName] ,[ZTID] ,[ZTName] ,[ZTYear],[BeginMonth] ,[EndMonth] ,[PZBeginDate] ,[PZEndDate],[MountType],[ErrMsg]) select " +
                "{0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}' ", xfile.XID, xfile.CustomID, xfile.CustomName, xfile.FileName, xfile.ZTID, xfile.ZTName, xfile.ZTYear, xfile.BeginMonth, xfile.EndMonth, xfile.PZBeginDate, xfile.PZEndDate, xfile.MountType,
                ErrMsg.Replace(@"'", "").Replace("-", ""));
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    cmd.ExecuteNonQuery();
                }
                catch (Exception err)
                {
                   throw err;
                }
            }
        }
        public static void InsertXdata(Models.xfile xfile)
        {
            connectString = StaticUtil.GetConfigValueByKey("XDataConn");
            string sql = string.Format(" insert into XData.dbo.[XFiles](XID, [CustomID] ,[CustomName] ,[FileName] ,[ZTID] ,[ZTName] ,[ZTYear],[BeginMonth] ,[EndMonth] ,[PZBeginDate] ,[PZEndDate],[MountType], [unPackageDate] ) select " +
                "{0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}' ", xfile.XID, xfile.CustomID, xfile.CustomName, xfile.FileName, xfile.ZTID, xfile.ZTName, xfile.ZTYear, xfile.BeginMonth, xfile.EndMonth, xfile.PZBeginDate, xfile.PZEndDate, xfile.MountType, DateTime.Now);
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    cmd.ExecuteNonQuery();
                }
                catch (Exception err)
                {
                    throw err;
                }
            }
        }
        public static void DeleteXdataByID(Models.xfile xfile)
        {
            connectString = StaticUtil.GetConfigValueByKey("XDataConn");
            string sql = "delete from  XData.dbo.[XFiles] where xid="+xfile.XID;
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    cmd.ExecuteNonQuery();
                }
                catch (Exception err)
                {
                    throw err;
                }
            }
        }

    }
}
