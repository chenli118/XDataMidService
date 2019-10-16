﻿using System;
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
        static void Main(string[] args)
        {
            bool bCreate = true;
            Mutex mt = new Mutex(false, "XDataBG", out bCreate);
            if (bCreate)
            {
                var autoEvent = new AutoResetEvent(false);
                _timer = new Timer(p => FlushData(), autoEvent, 0, 10000);
                autoEvent.WaitOne();
            }
        }
        static Dictionary<string, int> runDict = new Dictionary<string, int>();
        public static int HttpHandlePost(string url, string pjson)
        {
            if (!runDict.ContainsKey(pjson))
            {
                runDict.Add(pjson, 1);
            }
            else
            {
                if (runDict[pjson] >3) return 3;
            }
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
                response = httpClient.PostAsync(uri, postContent,cts).Result;
                string responseMessage = response.Content.ReadAsStringAsync().Result;
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    return 1;
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
                                WriteLog(logfile, DateTime.Now + " " + c.Value);
                            }
                        }
                        
                    } 
                    return 0;
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
                    strRet += ex.Message + "  超时" ;
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
                runDict[pjson]++;
                if (postContent != null)
                    postContent.Dispose();
                if (response != null)
                    response.Dispose();
            }
            if (strRet.Length > 0)
            { 
                WriteLog(logfile, DateTime.Now + " :  " + strRet);
            }
            return -1;
        }

        private static void FlushData()
        {
            if (bRunning) return;
            try
            {                
               
                    bRunning = true;
                    customDb = new Dictionary<string, List<DataRow>>();
                    QueueTable = new DataTable();
                    LoadQueue();
                    if (QueueTable == null || QueueTable.Rows.Count == 0)
                    {
                        bRunning = false;
                        Console.WriteLine("Nothing Todo :" + DateTime.Now);
                        return;
                    }
                  
                    foreach (DataRow row in QueueTable.Rows)
                    {
                        string XID = row["XID"].ToString();
                        if (!customDb.ContainsKey(XID))
                        {
                            customDb.Add(XID, QueueTable.Rows.Cast<DataRow>().Where(x => x["XID"].ToString() == XID).ToList());
                        }
                    }
                    if (customDb.Count > 0)
                    {
                        Parallel.ForEach(customDb, (c, loopstate) =>
                         {
                         lock (obj_lock)
                         {
                             DataRow dr = c.Value[0];
                             xfile xfile = new xfile();
                             xfile.wp_GUID = "e703ffdf-cdf9-4111-97ee-0747f531ebb2";
                             xfile.fileName = dr["FileName"].ToString();
                             xfile.customName = dr["CustomName"].ToString();
                             xfile.ztName = dr["ZTName"].ToString();
                             xfile.xID = Convert.ToInt32(dr["XID"]);
                             xfile.customID = dr["CustomID"].ToString();
                             xfile.ztid = dr["ZTID"].ToString();
                             xfile.ztYear = dr["ZTYear"].ToString();
                             xfile.pzBeginDate = dr["PZBeginDate"].ToString();
                             xfile.pzEndDate = dr["PZEndDate"].ToString();
                             var pjson = JsonSerializer.Serialize(xfile);
                             Console.WriteLine(xfile.customName + "  " + xfile.ztName + "start XData2SQL :" + DateTime.Now);
                             int ps = HttpHandlePost("http://192.168.1.209/XData/XData2SQL", pjson);
                             Console.WriteLine(xfile.customName + "  " + xfile.ztName + "end XData2SQL :" + DateTime.Now);
                             if (ps == 1)
                             {
                                 Console.WriteLine(xfile.customName + "  " + xfile.ztName + "start InsertXdata :" + DateTime.Now);
                                 InsertXdata(dr);
                                 Console.WriteLine(xfile.customName + "  " + xfile.ztName + "end InsertXdata :" + DateTime.Now);
                             }
                                 if (ps == 3)
                                 {
                                     Console.WriteLine(xfile.customName + "  " + xfile.ztName + "  重试次超过3次，未提交执行 :" + DateTime.Now);
                                 }
                             }
                         });
                    }
                
            }
            catch (Exception exception)
            {
                WriteLog(logfile, DateTime.Now + " : " + exception.Message);
            }
            finally
            {
                bRunning = false;
            }


        }

        private static void InsertXdata(DataRow dr)
        {
            connectString = ConnectionString("XDataConn");
            string sql = string.Format(" insert into XData.dbo.[XFiles](XID, [CustomID] ,[CustomName] ,[FileName] ,[ZTID] ,[ZTName] ,[ZTYear],[BeginMonth] ,[EndMonth] ,[PZBeginDate] ,[PZEndDate]) select " +
                "{0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}' ", Convert.ToInt32(dr["XID"]), dr["CustomID"], dr["CustomName"], dr["FileName"], dr["ZTID"],
                dr["ZTName"], dr["ZTYear"], dr["BeginMonth"], dr["EndMonth"], dr["PZBeginDate"], dr["PZEndDate"]);
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
                    WriteLog(logfile, DateTime.Now + " : " + err.Message);
                }
            }
        }

        private static string ConnectionString(string key)
        {
            if (!string.IsNullOrWhiteSpace(connectString)) return connectString;
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
            connectString = ConnectionString("XDataConn");

            string sql = " select XID, [CustomID] ,[CustomName] ,[FileName] ,[ZTID] ,[ZTName] ,[ZTYear],[BeginMonth] ,[EndMonth] ,[PZBeginDate] ,[PZEndDate] from  XData2Eas.XDB.dbo.XFiles where xid not in" +
                " (select xid from  XData.dbo.[XFiles]) ";
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                try
                {
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    IDataReader reader = cmd.ExecuteReader();
                    QueueTable.Load(reader);
                    reader.Close();

                }
                catch (Exception err)
                {
                    WriteLog(logfile, DateTime.Now + " : " + err.Message);
                }
            }

        }
        private static async void WriteLog(string path, string message)
        {
            await Task.Run(() => { System.IO.File.AppendAllText(path, message); });
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

        public string wp_GUID { get; set; }
        public string projectID { get; set; }
        public string dbName { get; set; }
    }
}