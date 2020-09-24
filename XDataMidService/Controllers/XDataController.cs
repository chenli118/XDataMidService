using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using XDataMidService.BPImp;
using XDataMidService.Models;
using Microsoft.Extensions.Configuration;
namespace XDataMidService.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class XDataController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<XDataController> _logger;
        public XDataController(ILogger<XDataController> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            if (StaticData.X2EasList == null) StaticData.X2EasList = new Dictionary<string, string>();
            if (StaticData.X2SqlList == null) StaticData.X2SqlList = new Dictionary<string, int>();
        }
        [HttpPost]
        [Route("XData2SQL")]
        public Task XData2SQL([FromBody] Models.xfile xfile)
        {
            ActionContext _context = this.ControllerContext;
            XDataResponse response = new XDataResponse();
            string key = xfile.XID + xfile.ZTID + xfile.CustomID + xfile.FileName;
            try
            {
                string localDbName = StaticUtil.GetLocalDbNameByXFile(xfile);
                string conStr = StaticUtil.GetConfigValueByKey("XDataConn");
                string sql = " select 1 from sys.databases where  name='" + localDbName + "'";
                int isOne = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                if (isOne == 1)
                {
                    sql = " select 1 from xdata.dbo.xfiles where  xid=" + xfile.XID;
                    isOne = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                    if (isOne == 1)
                    {
                        _logger.LogInformation(xfile.XID + " " + xfile.ZTName + " 数据已存在，跳过解包过程！  " + DateTime.Now);
                        return Ok(response).ExecuteResultAsync(_context);
                    }
                }

                if (!StaticData.X2SqlList.ContainsKey(key))
                {
                    StaticData.X2SqlList.Add(key, 1);
                }
                else if (StaticData.X2SqlList[key] == 1)
                {
                    response.ResultContext = key + " 已经在执行过程中...";
                    return BadRequest(response).ExecuteResultAsync(_context);
                }
                StaticData.X2SqlList[key] = 1;
                PDT2SDT dT2SDT = new PDT2SDT(xfile);
                string strRet = string.Empty;
                if (dT2SDT.DownLoadFile(xfile, out strRet))
                {
                    response = dT2SDT.Start();
                    if (response.HttpStatusCode == 200)
                    {
                        try
                        {
                            BPImp.XDataBP.InsertXdata(xfile);
                            _logger.LogInformation(xfile.ZTName + " " + response.ResultContext + " " + DateTime.Now);
                            return Ok(response).ExecuteResultAsync(_context);
                        }
                        catch (Exception err)
                        {
                            BPImp.XDataBP.DeleteXdataByID(xfile);
                            XDataBP.DropDB(xfile);
                            _logger.LogError(xfile.ZTName + "   " + err.Message + " " + DateTime.Now);
                        }
                    }
                    else
                    {
                        try
                        {
                            response.ResultContext = response.ResultContext + "||" + dT2SDT._xdException.Message;
                            XDataBP.InsertBadFile(xfile, response.ResultContext);
                            XDataBP.DropDB(xfile);
                        }
                        catch (Exception err)
                        {
                            _logger.LogError(xfile.ZTName + " " + response.ResultContext + " " + err.Message + " " + DateTime.Now);
                        }
                        return BadRequest(response).ExecuteResultAsync(_context);
                    }
                }
                string errMsg = string.Format("{0} 从网盘下载账套{1}文件 {2} 失败: " + strRet, xfile.CustomName, xfile.ZTName, xfile.FileName);
                _logger.LogError(errMsg);
                XDataBP.InsertBadFile(xfile, errMsg);
                response.ResultContext = errMsg;
                return BadRequest(response).ExecuteResultAsync(_context);
            }
            finally
            {
                if (StaticData.X2SqlList.ContainsKey(key))
                    StaticData.X2SqlList.Remove(key);
            }
        }

        [HttpPost]
        [Route("GetXDataCheckByID")]
        public Task GetXDataCheckByID([FromBody] Models.xfile xfile)
        {
            ActionContext _context = this.ControllerContext;
            string constr = StaticUtil.GetConfigValueByKey("XDataConn");
            XDataResponse response = new XDataResponse();
            if (xfile.UploadUser == "FACHECK")
            {
                try
                { 
                    var tbv = SqlServerHelper.GetLinkSrvName(StaticUtil.GetConfigValueByKey("EASConn"), constr);
                    string linkSvrName = tbv.Item1;
                    string dbName = tbv.Item2;
                    string qdb = " select XID,CustomID,ZTID,ZTYear,ZTName,CustomName,FileName,PZBeginDate,PZEndDate,MountType from  [" + linkSvrName + "].XDB.dbo.XFiles where xid =" + xfile.XID;
                    var dataTable = SqlServerHelper.GetTableBySql(qdb, constr);
                    if (dataTable.Rows.Count > 0)
                    {
                        if (RepostXfile2Sql(dataTable.Rows[0]) == 1)
                        {
                            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(constr);
                            csb.InitialCatalog = StaticUtil.GetLocalDbNameByXData(dataTable.Rows[0]);
                            var sptext = System.IO.Path.Combine(System.IO.Directory.GetCurrentDirectory(), "VerifyFinancialData.sql");
                            var sqls = System.IO.File.ReadAllText(sptext, Encoding.UTF8);
                            SqlMapperUtil.ExecuteNonQueryBatch(csb.ConnectionString, sqls);
                            SqlMapperUtil.CMDExcute("  exec VerifyFinancialData '" + xfile.XID + "'  ", null, csb.ConnectionString);
                        }
                        else
                        {
                            string pbad = "insert into  xdata.dbo.facheck values(" + xfile.XID + ", '" + xfile.XID + " 解包过程出错！', 3)";
                            var thisdb = SqlMapperUtil.CMDExcute(pbad, null, constr);
                        }
                    }

                }
                catch (Exception err)
                {
                    string pbad = "insert into  xdata.dbo.facheck values(" + xfile.XID + ", '解包过程出错: " + xfile.XID + " " + err.Message.Replace("'", "").Replace(":", "").Replace("?", "") + "    ', 3)";
                    var thisdb = SqlMapperUtil.CMDExcute(pbad, null, constr);
                }
            }
            string sql = "select * from xdata.dbo.facheck where xid=" + xfile.XID;
            try
            {
                var dataTable = SqlServerHelper.GetTableBySql(sql, constr);
                var ret = Newtonsoft.Json.JsonConvert.SerializeObject(dataTable);

                response.ResultContext = ret;
                return Ok(response).ExecuteResultAsync(_context);
            }
            catch (Exception err)
            {
                response.ResultContext = err.Message;
                return BadRequest(response).ExecuteResultAsync(_context);
            }
        }

        [HttpPost]
        [Route("XData2EAS")]
        public Task XData2EAS([FromBody] Models.xfile xfile)
        {
            XDataResponse response = new XDataResponse();
            ActionContext _context = this.ControllerContext;
            string localDbName = StaticUtil.GetLocalDbNameByXFile(xfile);
            XData2EasService easService = new XData2EasService(_logger);
            string key = xfile.XID + xfile.ZTID + xfile.CustomID + xfile.FileName;
            try
            {
                if (StaticData.X2SqlList.ContainsKey(key) && StaticData.X2SqlList[key] == 1)
                {
                    response.ResultContext = localDbName + " 数据正在准备中！ ";
                    response.HttpStatusCode = 500;
                    return BadRequest(response).ExecuteResultAsync(_context);
                }

                string constr = StaticUtil.GetConfigValueByKey("XDataConn");
                string qdb = "select 1 from xdata.dbo.xfiles where xid =" + xfile.XID;
                var thisdb = SqlMapperUtil.SqlWithParamsSingle<int>(qdb, null, constr);
                var tbv = SqlServerHelper.GetLinkSrvName(xfile.DbName, constr);
                string linkSvrName = tbv.Item1;
                string dbName = tbv.Item2;
                int port = new System.Uri(_configuration.GetSection("urls").Value).Port;
                if (thisdb != 1 && port == 80)
                {
                    response.HttpStatusCode = 500;
                    qdb = "select Errmsg from xdata.dbo.badfiles where xid =" + xfile.XID;
                    var errmsg = SqlMapperUtil.SqlWithParamsSingle<string>(qdb, null, constr);
                    if (!string.IsNullOrWhiteSpace(errmsg))
                    {
                        response.ResultContext = xfile.XID + "  " + errmsg + "！ ";
                    }
                    qdb = "select xgroup from xdata.dbo.repeatdb where xid =" + xfile.XID;
                    var xgroup = SqlMapperUtil.SqlWithParamsSingle<string>(qdb, null, constr);
                    if (!string.IsNullOrWhiteSpace(xgroup))
                    {
                        response.ResultContext = xfile.XID + "  数据与" + xgroup + "重复！ ";
                    }

                    if (string.IsNullOrWhiteSpace(errmsg) && string.IsNullOrWhiteSpace(xgroup))
                    {
                        var  linkSrc = SqlServerHelper.GetLinkSrvName(StaticUtil.GetConfigValueByKey("EASConn"), constr);
                        qdb = " select  XID,CustomID,ZTID,ZTYear,ZTName,CustomName,FileName,PZBeginDate,PZEndDate,MountType from  [" + linkSrc.Item1 + "].XDB.dbo.XFiles order by xid desc ";
                        //qdb += " union all ";
                        var srcFiles = SqlServerHelper.GetTableBySql(qdb, constr);
                        if (srcFiles.Rows.Count > 0)
                        {
                            int maxxid = Convert.ToInt32(srcFiles.Rows[0]["XID"]);
                            qdb = "select max(xid) from xdata.dbo.xfiles ";
                            int thexid = SqlMapperUtil.SqlWithParamsSingle<int>(qdb, null, constr);

                            if (xfile.XID < thexid)
                            {
                                _logger.LogInformation("  开始处理缓存外数据 " + xfile.XID + " " + DateTime.Now);
                                DataRow dr = srcFiles.Rows.Cast<DataRow>().Where(r => r.Field<int>("XID") == xfile.XID).FirstOrDefault();
                                if (RepostXfile2Sql(dr) == 1)
                                {
                                    StaticData.X2EasList[key] = "";
                                    var pjson = JsonSerializer.Serialize(xfile);
                                    Tuple<int, string> ret = null;
                                    string XData_Host = StaticUtil.GetConfigValueByKey("XData_Host");
                                    string[] XData_Host_Port = StaticUtil.GetConfigValueByKey("XData_Host_Port").Split(';');
                                    UriBuilder uriBuilder0 = new UriBuilder("http", XData_Host, int.Parse(XData_Host_Port[0]), "XData/XData2EAS");
                                    UriBuilder uriBuilder1 = new UriBuilder("http", XData_Host, int.Parse(XData_Host_Port[1]), "XData/XData2EAS");
                                    if (xfile.XID % 2 == 0)
                                    {
                                        ret = HttpHandlePost(uriBuilder0.Uri.AbsoluteUri, pjson);
                                    }
                                    else
                                    {
                                        ret = HttpHandlePost(uriBuilder1.Uri.AbsoluteUri, pjson);
                                    }
                                    if (ret.Item1 != 1)
                                    {
                                        response.ResultContext = xfile.XID + ": 缓存外数据处理失败，请联系统管理员！";

                                    }
                                    else
                                    {
                                        response.HttpStatusCode = 200;
                                        _logger.LogInformation("  处理完成缓存外数据 " + xfile.XID + " " + DateTime.Now);
                                        return Ok(response).ExecuteResultAsync(_context);
                                    }
                                }
                                else
                                {
                                    response.ResultContext = xfile.XID + ": 缓存外数据处理失败，请联系统管理员！";
                                }
                            }
                            else
                            {
                                response.ResultContext = xfile.XID + string.Format(": 数据准备中，前面还有{0} 个待处理文件！ ", maxxid - xfile.XID);

                            }
                        }
                    }
                    _logger.LogInformation(response.ResultContext + " " + DateTime.Now);

                    return BadRequest(response).ExecuteResultAsync(_context);
                }

                else if (StaticData.X2EasList.ContainsValue(xfile.DbName))
                {
                    System.Threading.Tasks.Task.Factory.StartNew(() =>
                    {
                        easService.Xfile = xfile;
                    });
                    response.HttpStatusCode = 200;
                    response.ResultContext = xfile.XID + "|" + xfile.ProjectID;
                    return Ok(response).ExecuteResultAsync(_context);
                }
                else
                {
                    response = easService.Real2EasImp(xfile);

                    if (response.HttpStatusCode == 200)
                    {
                        return Ok(response).ExecuteResultAsync(_context);
                    }
                    return BadRequest(response).ExecuteResultAsync(_context);
                }
            }
            catch (Exception err)
            {
                response.HttpStatusCode = 500;
                response.ResultContext = string.Format("文件{0} 项目{1}导入EAS失败,请联系系统管理员！", xfile.XID, xfile.ProjectID);
                _logger.LogError(response.ResultContext + " 异常：" + err.Message + " " + DateTime.Now);
                return BadRequest(response).ExecuteResultAsync(_context);
            }
        }
        [HttpPost]
        [Route("SetXDataStatus")]
        public Task SetXDataStatus([FromBody] Models.xfile xfile)
        {
            XDataResponse response = new XDataResponse();
            ActionContext _context = this.ControllerContext;
            string sql = string.Format("update   XData.dbo.XFiles set DataStatus={1} where xid ={0}", xfile.XID, xfile.DataStatus);
            var constr = StaticUtil.GetConfigValueByKey("XDataConn");
            SqlMapperUtil.CMDExcute(sql, null, constr);
            return Ok(response).ExecuteResultAsync(_context);
        }
        private int RepostXfile2Sql(DataRow dr)
        {
            xfile xfile = new xfile();
            xfile.WP_GUID = "e703ffdf-cdf9-4111-97ee-0747f531ebb2";
            xfile.FileName = dr["FileName"].ToString();
            xfile.CustomName = dr["CustomName"].ToString();
            xfile.ZTName = dr["ZTName"].ToString();
            xfile.XID = Convert.ToInt32(dr["XID"]);
            xfile.CustomID = dr["CustomID"].ToString();
            xfile.ZTID = dr["ZTID"].ToString();
            xfile.ZTYear = dr["ZTYear"].ToString();
            xfile.PZBeginDate = dr["PZBeginDate"].ToString();
            xfile.PZEndDate = dr["PZEndDate"].ToString();
            xfile.MountType = dr["MountType"].ToString();
            var pjson = JsonSerializer.Serialize(xfile);
            Tuple<int, string> ret = null;
            string XData_Host = StaticUtil.GetConfigValueByKey("XData_Host");
            string[] XData_Host_Port = StaticUtil.GetConfigValueByKey("XData_Host_Port").Split(';');
            UriBuilder uriBuilder0 = new UriBuilder("http", XData_Host, int.Parse(XData_Host_Port[0]), "XData/XData2SQL");
            UriBuilder uriBuilder1 = new UriBuilder("http", XData_Host, int.Parse(XData_Host_Port[1]), "XData/XData2SQL");
            if (xfile.XID % 2 == 0)
            {
                ret = HttpHandlePost(uriBuilder0.Uri.AbsoluteUri, pjson);
            }
            else
            {
                ret = HttpHandlePost(uriBuilder1.Uri.AbsoluteUri, pjson);
            }
            return ret.Item1;
        }
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
            return new Tuple<int, string>(-1, strRet);
        }


    }
}