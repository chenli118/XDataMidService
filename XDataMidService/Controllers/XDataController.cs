using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using XDataMidService.BPImp;
using XDataMidService.Models;

namespace XDataMidService.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class XDataController : ControllerBase
    {

        private readonly ILogger<XDataController> _logger;
        public XDataController(ILogger<XDataController> logger)
        {
            _logger = logger;
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
                if (dT2SDT.DownLoadFile(xfile))
                {
                    response = dT2SDT.Start();
                    if (response.HttpStatusCode == 200)
                    {
                        try
                        {
                            BPImp.XDataBP.InsertXdata(xfile);
                            StaticData.X2SqlList[key]++;
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
                            StaticData.X2SqlList[key]++;
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
                StaticData.X2SqlList[key] = 0;
                string errMsg = string.Format("{0} 从网盘下载账套{1}文件 {2} 失败: ", xfile.CustomName, xfile.ZTName, xfile.FileName);
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
                if (StaticData.X2EasList.ContainsKey(key) 
                    &&StaticData.X2EasList[key] == localDbName)
                {
                    response.ResultContext = key + " 已经在执行过程中...";

                    return BadRequest(response).ExecuteResultAsync(_context);
                }
                string constr = StaticUtil.GetConfigValueByKey("XDataConn");
                string qdb = "select 1 from xdata.dbo.xfiles where xid =" + xfile.XID;
                var thisdb = SqlMapperUtil.SqlWithParamsSingle<int>(qdb, null, constr);
                var tbv = SqlServerHelper.GetLinkSrvName(xfile.DbName, constr);
                string linkSvrName = tbv.Item1;
                string dbName = tbv.Item2;
                if (thisdb != 1)
                {

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
                        qdb = " select max(xid) from  [" + linkSvrName + "].XDB.dbo.XFiles ";
                        var maxxid = SqlMapperUtil.SqlWithParamsSingle<int>(qdb, null, constr);
                        if (maxxid > 0)
                        {
                            response.ResultContext = xfile.XID + string.Format(": 数据准备中，前面还有{0} 个待处理文件！ ", maxxid - xfile.XID);
                            response.HttpStatusCode = 500;
                        }
                    }
                    _logger.LogInformation(response.ResultContext + " " + DateTime.Now);

                    return BadRequest(response).ExecuteResultAsync(_context);
                }

                else if (StaticData.X2EasList.ContainsValue(localDbName))
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



    }
}