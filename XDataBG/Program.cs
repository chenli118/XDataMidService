using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using System.Net.Http;
using System.Text;

namespace XDataBG
{
    class Program
    {
        private static Timer _timer;
        private static string logfile = AppDomain.CurrentDomain.BaseDirectory + "log.log";
        private static Dictionary<string, List<DataRow>> customDb;
        private static DataTable QueueTable;
        private static bool bRunning;
        private static string connectString;
        private static object obj_lock = new object();

        private static Dictionary<int, xfile> ProcessList;
        static void Main(string[] args)
        {
            logfile = logfile.Replace("log.", Guid.NewGuid() + ".");
            //bool bCreate = true;
            //Mutex mt = new Mutex(false, "XDataBG", out bCreate);
            //if (bCreate)
            {

                var autoEvent = new AutoResetEvent(false);
                _timer = new Timer(p => FlushData(), autoEvent, 0, 10000);
                autoEvent.WaitOne();
            }
        }
        static Dictionary<string, int> runDict = new Dictionary<string, int>();
        public static Tuple<int, string> HttpHandlePost(string url, string pjson)
        {

            HttpClientHandler httpHandler = new HttpClientHandler();
            string strRet = string.Empty;
            HttpClient httpClient = new HttpClient();
            httpClient.Timeout = TimeSpan.FromHours(4);
            var cts = new CancellationToken();
            HttpContent postContent = null;
            HttpResponseMessage response = null;
            try
            {
                Uri uri = new System.Uri(url);
                httpClient.DefaultRequestHeaders.Add("Host", uri.Host);
                httpClient.DefaultRequestHeaders.Add("User-Agent", "PostmanRuntime/7.17.1");
                httpClient.DefaultRequestHeaders.Add("Accept", "*/*");
                httpClient.DefaultRequestHeaders.Add("Cache-Control", "no-cache");
                httpClient.DefaultRequestHeaders.Add("Accept-Encoding", "gzip, deflate");
                httpClient.DefaultRequestHeaders.Add("Connection", "keep-alive");
                postContent = new StringContent(pjson, Encoding.UTF8, "application/json");
                response = httpClient.PostAsync(uri, postContent, cts).Result;
                string responseMessage = response.Content.ReadAsStringAsync().Result;
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    return new Tuple<int, string>(1, "");
                }
                else
                {
                    var msg = JsonDocument.Parse(responseMessage);
                    if (msg != null)
                    {
                        foreach (var c in msg.RootElement.EnumerateObject())
                        {
                            if (c.Name == "resultContext")
                            {
                                Console.WriteLine(c.Value);
                                strRet = c.Value.ToString();
                                WriteLog(logfile, DateTime.Now + " " + c.Value);
                            }
                        }

                    }
                    return new Tuple<int, string>(0, strRet);
                }

            }
            catch (TaskCanceledException ex)
            {
                if (ex.CancellationToken == cts)
                {
                    strRet += ex.Message + " ";
                    Console.WriteLine(ex.Message);
                }
                else
                {
                    strRet += ex.Message + "  超时";
                    Console.WriteLine(strRet);
                }
            }
            catch (AggregateException ex)
            {
                foreach (var ee in ex.InnerExceptions)
                {
                    strRet += ee.Message + " ";
                    Console.WriteLine(ee.Message);
                }
            }
            catch (Exception ex)
            {
                strRet = ex.Message;
            }
            finally
            {

                if (postContent != null)
                    postContent.Dispose();
                if (response != null)
                    response.Dispose();
            }
            if (strRet.Length > 0)
            {
                WriteLog(logfile, DateTime.Now + " :  " + strRet);

            }
            return new Tuple<int, string>(-1, strRet);
        }
        private static bool externExec = false;
        private static void FlushData()
        {
            if (bRunning) return;
            try
            {
                ProcessList = new Dictionary<int, xfile>();
                ProcessList.Clear();
                bRunning = true;
                customDb = new Dictionary<string, List<DataRow>>();
                QueueTable = new DataTable();
                LoadQueue();
                if (QueueTable == null || QueueTable.Rows.Count == 0)
                {
                    bRunning = false;
                    Console.WriteLine("Nothing Todo :" + DateTime.Now);                  
                    if (externExec || DateTime.Now.Hour>3 ) return;
                    Task.Factory.StartNew(()=>{
                    externExec = true;
                    BatchDetachDB();
                    var KeepDays = int.Parse(ConnectionString("KeepDays"));
                    BatchDeleteOldFile(@"D:\XData\5002\XJYData", KeepDays);
                    BatchDeleteOldFile(@"D:\XData\5003\XJYData", KeepDays);
                    externExec = false;

                    });                   
                    return;
                }
                else
                {
                    customDb.Clear();
                    foreach (DataRow row in QueueTable.Rows)
                    {
                        string XID = row["XID"].ToString();
                        if (!customDb.ContainsKey(XID))
                        {
                            customDb.Add(XID, QueueTable.Rows.Cast<DataRow>().Where(x => x["XID"].ToString() == XID).ToList());
                        }
                    }
                }
                if (customDb.Count > 0)
                {
                    ParallelOptions parallelOptions = new ParallelOptions();
                    parallelOptions.MaxDegreeOfParallelism = 2;
                    _ = Parallel.ForEach(customDb, parallelOptions, (c, loopstate) =>
                       {

                           DataRow dr = c.Value[0];
                           xfile xfile = new xfile();
                           xfile.wp_GUID = "e703ffdf-cdf9-4111-97ee-0747f531ebb2";
                           xfile.fileName = dr["FileName"].ToString();
                           xfile.customName = dr["CustomName"].ToString();
                           xfile.ztName = dr["ZTName"].ToString();
                           xfile.xID = Convert.ToInt32(dr["XID"]);
                           if (ExistsXID(xfile.xID)) return;
                           xfile.customID = dr["CustomID"].ToString();
                           xfile.ztid = dr["ZTID"].ToString();
                           xfile.ztYear = dr["ZTYear"].ToString();
                           xfile.pzBeginDate = dr["PZBeginDate"].ToString();
                           xfile.pzEndDate = dr["PZEndDate"].ToString();
                           xfile.mountType = dr["MountType"].ToString();
                           if (!ProcessList.ContainsKey(xfile.xID))
                           {
                               //ProcessList.Add(xfile.xID,xfile);
                               var pjson = JsonSerializer.Serialize(xfile);
                               Console.WriteLine(xfile.xID + "  " + xfile.ztName + "start XData2SQL :" + DateTime.Now);
                               Tuple<int, string> ret = null;
                               string XData_Host = ConnectionString("XData_Host");
                               string[] XData_Host_Port = ConnectionString("XData_Host_Port").Split(';');
                               UriBuilder uriBuilder0 = new UriBuilder("http", XData_Host, int.Parse(XData_Host_Port[0]), "XData/XData2SQL");
                               UriBuilder uriBuilder1 = new UriBuilder("http", XData_Host, int.Parse(XData_Host_Port[1]), "XData/XData2SQL");
                               if (xfile.xID % 2 == 0)
                               {
                                   ret = HttpHandlePost(uriBuilder0.Uri.AbsoluteUri, pjson);
                               }
                               else
                               {
                                   ret = HttpHandlePost(uriBuilder1.Uri.AbsoluteUri, pjson);
                               }
                               Console.WriteLine(xfile.xID + "  " + xfile.ztName + "end XData2SQL :" + DateTime.Now);
                               if (ret.Item1 == 1)
                               {
                                   Console.WriteLine(xfile.xID + "  " + xfile.ztName + " Completed !" + DateTime.Now);
                                   xfile.uploadUser = "FACHECK";
                                   xfile.dbName = ConnectionString("EASConn");
                                   pjson = JsonSerializer.Serialize(xfile);
                                   UriBuilder check0 = new UriBuilder("http", XData_Host, int.Parse(XData_Host_Port[0]), "XData/GetXDataCheckByID");
                                   UriBuilder check1 = new UriBuilder("http", XData_Host, int.Parse(XData_Host_Port[1]), "XData/GetXDataCheckByID");
                                   if (xfile.xID % 2 == 0)
                                   {
                                       ret = HttpHandlePost(check0.Uri.AbsoluteUri, pjson);
                                   }
                                   else
                                   {
                                       ret = HttpHandlePost(check1.Uri.AbsoluteUri, pjson);
                                   }
                                   Console.WriteLine(xfile.xID + "  " + xfile.ztName + " EndChecked !" + DateTime.Now);
                               }
                               else
                               {
                                   string sql = " delete from xdata..badfiles   where errmsg like '%登录失%'";
                                   ExecuteSql(sql);
                                   Console.WriteLine(xfile.xID + "  " + xfile.ztName + " Fail !" + DateTime.Now);
                               }
                               //ProcessList.Remove(xfile.xID);
                           }

                       });
                }

            }
            catch (Exception exception)
            {

                Console.WriteLine("ERROR:" + exception.Message + " " + DateTime.Now);
            }
            finally
            {
                bRunning = false;
            }


        }
        private static int ExecuteSql(string sql)
        {
            connectString = ConnectionString("XDataConn");
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    return cmd.ExecuteNonQuery();

                }
                catch (Exception err)
                {
                    Console.WriteLine(err.Message);
                    WriteLog(logfile, DateTime.Now + " : " + err.Message);
                    return -1;
                }
            }
        }
        private static bool ExistsXID(int xid)
        {
            connectString = ConnectionString("XDataConn");
            string sql = "select xid from  XData.dbo.[xfiles] where xid=" + xid;
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    var id = cmd.ExecuteScalar();
                    if (id != null && Convert.ToInt32(id) > 1)
                        return true;

                }
                catch (Exception err)
                {
                    Console.WriteLine(err.Message);
                    WriteLog(logfile, DateTime.Now + " : " + err.Message);
                }
                return false;
            }
        }

        private static DataTable GetTableBySql(string sql)
        {
            connectString = ConnectionString("XDataConn");
            DataTable dt = new DataTable();
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    IDataReader reader = cmd.ExecuteReader();
                    dt.Load(reader);
                    reader.Close();

                }
                catch (Exception err)
                {
                    Console.WriteLine(err.Message);
                }

            }
            return dt;
        }

        private static string ConnectionString(string key)
        {
            string jsonTxt = File.ReadAllText(Path.Combine(Directory.GetCurrentDirectory(), "WebApp.Config.json"));
            var config = JsonDocument.Parse(jsonTxt).RootElement;
            foreach (var prop in config.EnumerateObject())
            {
                if (prop.Name == "ConnectionStrings")
                {
                    var pv = prop.Value;
                    foreach (var con in pv.EnumerateObject())
                    {
                        if (con.Name == key)
                        {
                            return con.Value.GetString();
                        }
                    }
                }
            }
            return string.Empty;
        }
        private static void LoadQueue()
        {
            DataTable xfiles = GetTableBySql("select max(xid) xid from  XData.dbo.[XFiles]  ");
            int maxid = Convert.ToInt32(xfiles.Rows[0].ItemArray[0]);
            connectString = ConnectionString("XDataConn");
            var whereas = ConnectionString("Whereas");
            var xFilesCache = ConnectionString("XFilesCache");
            string linkSvr = GetLinkSrvName(ConnectionString("EASConn"), connectString).Item1;
            string sql = " select XID, [CustomID] ,[CustomName] ,[FileName] ,[ZTID] ,[ZTName] ,[ZTYear],[BeginMonth] ,[EndMonth] ,[PZBeginDate] ,[PZEndDate],[MountType] from " +
                " [" + linkSvr + "].XDB.dbo.XFiles where xid not in" +
                " (select xid from  XData.dbo.[XFiles]) and  DATEPART(yyyy, getdate())- ZTYear <5   and xid not in(select xid from  XData.dbo.[badfiles])" +
                " and xid not in(select xid from  XData.dbo.[repeatdb])" +
                 " and xid> " + (maxid - Convert.ToInt32(xFilesCache)) +
                " and  CustomID in ( select nmclientid from  [" + linkSvr + "].neweasv5.[dbo].[ClientBasicInfo])" +
                " and  " + whereas + "   order by xid  ";
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    IDataReader reader = cmd.ExecuteReader();
                    QueueTable = new DataTable();
                    QueueTable.Load(reader);
                    reader.Close();

                }
                catch (Exception err)
                {
                    Console.WriteLine(" Load data fail: " + err.Message + " " + DateTime.Now);
                    WriteLog(logfile, DateTime.Now + " : " + err.Message);
                }
            }

        }
        public static string GetLinkServer(string conStr, string sName, string logName, string pwd, string ipAddress)
        {
            string sql = string.Format(" exec sp_addlinkedserver '{0}','','SQLOLEDB','{3}'  " +
                " exec sp_addlinkedsrvlogin '{0}', 'false', NULL, '{1}', '{2}'", sName, logName, pwd, ipAddress);
            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection(conStr))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    string cmdText = "SELECT 1 FROM sys.servers WHERE name='" + sName + "'";
                    System.Data.SqlClient.SqlCommand sqlCommand = new System.Data.SqlClient.SqlCommand(cmdText, conn);
                    var s = sqlCommand.ExecuteScalar();
                    if (s != null && int.Parse(s.ToString()) == 1)
                    {
                        sqlCommand.CommandText = " Exec sp_configure 'remote query timeout',0;  ";
                        sqlCommand.ExecuteNonQuery();
                        sqlCommand.CommandText = " RECONFIGURE; ";
                    }
                    else
                    {
                        sqlCommand.CommandText = sql;
                    }
                    sqlCommand.ExecuteNonQuery();
                    return sName;
                }
                catch (Exception err)
                {
                    throw err;
                }
            }
        }
        private static Tuple<string, string> GetLinkSrvName(string connectInfo, string localCon)
        {
            connectInfo = connectInfo.Replace("Asynchronous Processing=true", "");
            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(connectInfo);
            string linkSvrName = GetLinkServer(localCon, csb.DataSource, csb.UserID, csb.Password, csb.DataSource);
            return new Tuple<string, string>(linkSvrName, csb.InitialCatalog);
        }
        private static void WriteLog(string path, string message)
        {
            // await Task.Run(() => { System.IO.File.AppendAllText(path, message); });
        }
        private static void BatchDeleteOldFile(string fPath, int passDays)
        {

            if (!System.IO.Directory.Exists(fPath)) return;
            List<DirectoryInfo> emptyFolders = new List<DirectoryInfo>();
            foreach (string folder in Directory.GetDirectories(fPath))
            {
                DirectoryInfo dir = new DirectoryInfo(folder);
                if (dir.CreationTime < DateTime.Now.AddDays(-passDays))
                {
                    var files = dir.GetFiles("*.*", SearchOption.AllDirectories);
                    if (files.Length == 0)
                    {
                        if (!emptyFolders.Contains(dir))
                            emptyFolders.Add(dir);
                    }
                    foreach (var file in files)
                    {
                        try
                        {
                            File.SetAttributes(file.FullName, FileAttributes.Normal);
                            file.Delete();                           
                            Console.WriteLine("deleted file: " + file.FullName);
                        }
                        catch
                        {

                            Console.WriteLine("deleted file failed :" + file.FullName);
                        }
                    }
                }

            }
            if (emptyFolders.Count > 0)
            {
                try
                {
                    foreach (var dir in emptyFolders)
                    {
                        dir.Delete(true);
                        Console.WriteLine("deleted dir: " + dir.FullName);
                    }
                }
                catch (Exception rr)
                {
                    Console.WriteLine("deleted dir failed: " + rr.Message);
                }
            }
        }
        private static void BatchDetachDB()
        {
            var KeepDays = int.Parse(ConnectionString("KeepDays"));
            string sql = " select * from sys.databases where name <> 'xdata' AND database_id> 4 AND len(name)>20  and create_date< GETDATE()-" + KeepDays;
            DataTable allDB = GetTableBySql(sql);
            if (allDB.Rows.Count == 0) return;

            string fName = "SELECT   name ,   physical_name  FROM sys.master_files ; ";
            DataTable files = GetTableBySql(fName);
            Dictionary<string, List<string>> physiclFiles = new Dictionary<string, List<string>>();
            foreach (DataRow row in files.Rows)
            {
                string dname = row["name"].ToString();
                dname = dname.TrimEnd("_log".ToCharArray());
                if (!physiclFiles.ContainsKey(dname))
                {
                    List<string> list = new List<string>();
                    list.Add(row["physical_name"].ToString());
                    physiclFiles[dname] = list;
                }
                else
                    physiclFiles[dname].Add(row["physical_name"].ToString());

            }




            string xfsql = " select * from xdata.dbo.xfiles ";
            DataTable xfDB = GetTableBySql(xfsql);
            foreach (DataRow dataRow in xfDB.Rows)
            {
                string localdbname = GetLocalDbNameByXFile(dataRow);
                Parallel.ForEach(allDB.Rows.Cast<DataRow>(), dr =>
                {
                    string dbname = dr["name"].ToString();// 
                    if (localdbname == dbname)
                        try
                        {

                            sql = string.Format(" exec sp_detach_db '{0}','true' ", dbname);
                            ExecuteSql(sql);
                            foreach (var item in physiclFiles[dbname])
                            {
                                System.IO.File.Delete(item);
                            }
                            string delsql = string.Format(" delete from xdata.dbo.xfiles where xid={0} ", dataRow["xid"]);
                            ExecuteSql(delsql);
                            Console.WriteLine("detach  database " + dbname);
                        }
                        catch (Exception err)
                        {
                            Console.WriteLine(err.Message);

                        }

                });
            }
        }
        private static string GetLocalDbNameByXFile(DataRow xfile)
        {
            byte[] asciiBytes = Encoding.ASCII.GetBytes(xfile["xID"] + xfile["customID"].ToString().Replace("-", "") + xfile["ztYear"] + xfile["pzBeginDate"] + xfile["pzEndDate"]);
            StringBuilder sb = new StringBuilder();
            Array.ForEach(asciiBytes, (c) =>
            {
                if ((c > 47 && c < 58)
                || (c > 64 && c < 91)
                || (c > 96 && c < 123))
                { sb.Append((char)c); }
            });
            string dbName = sb.ToString();
            if (dbName.Length > 50) dbName = dbName.Substring(0, 49);
            return dbName;
        }
    }
    public class xfile
    {
        //TDS26MPvb select  name  , 'public string '+name+' { get; set; }' from  sys.columns where object_id in (select object_id from sys.objects where name ='AuthorizeXFiles')
        public int xID { get; set; }
        public string customID { get; set; }
        public string customName { get; set; }
        public string fileName { get; set; }
        public string ztid { get; set; }
        public string ztName { get; set; }
        public string ztYear { get; set; }
        public string beginMonth { get; set; }
        public string endMonth { get; set; }
        public string pzBeginDate { get; set; }
        public string pzEndDate { get; set; }
        public string mountType { get; set; }
        public string mountTime { get; set; }
        public string currency { get; set; }
        public int fileSize { get; set; }
        public string uploadUser { get; set; }
        public DateTime uploadTime { get; set; }
        public DateTime unPackageDate { get; set; }
        public string wp_GUID { get; set; }
        public string projectID { get; set; }
        public string dbName { get; set; }
    }
}
