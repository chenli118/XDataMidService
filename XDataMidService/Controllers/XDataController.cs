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
                        _logger.LogError(xfile.ZTName + " " + response.ResultContext + " " +err.Message + " " + DateTime.Now);
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
        [HttpPost]
        [Route("XData2EAS")]
        public Task XData2EAS([FromBody] Models.xfile xfile)
        {
            XDataResponse response = new XDataResponse();
            ActionContext _context = this.ControllerContext;
            string localDbName = StaticUtil.GetLocalDbNameByXFile(xfile);
            XData2EasService easService = new XData2EasService(_logger);
            if (StaticData.X2EasList.ContainsValue(localDbName))
            {
                System.Threading.Tasks.Task.Factory.StartNew(()=> {
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
        [HttpPost]
        [Route("SetXDataStatus")]
        public Task SetXDataStatus([FromBody] Models.xfile xfile)
        {
            XDataResponse response = new XDataResponse();
            ActionContext _context = this.ControllerContext;
            string sql = string.Format("update   XData.dbo.XFiles set DataStatus={1} where xid ={0}",xfile.XID,xfile.DataStatus);
            var constr = StaticUtil.GetConfigValueByKey("XDataConn");
            SqlMapperUtil.CMDExcute(sql, null, constr);
            return Ok(response).ExecuteResultAsync(_context);
        }



    }
}