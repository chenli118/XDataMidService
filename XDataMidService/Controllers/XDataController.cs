using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
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
            if (StaticData.X2EasList == null) StaticData.X2EasList = new Dictionary<string, int>();
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
            else if (StaticData.X2SqlList[key]==1)
            {
                response.ResultContext = key + " 已经在执行过程中...";
                return BadRequest(response).ExecuteResultAsync(_context);
            }
            PDT2SDT dT2SDT = new PDT2SDT(xfile);
            if (dT2SDT.DownLoadFile(xfile)) 
            {
                response= dT2SDT.Start();
                if (response.HttpStatusCode == 200)
                {
                    StaticData.X2SqlList[key]++;
                    _logger.LogInformation(xfile.ZTName + " " + response.ResultContext + " " + DateTime.Now);
                    return Ok(response).ExecuteResultAsync(_context);
                }
                else
                {
                    StaticData.X2SqlList[key]++;
                    _logger.LogError(xfile.ZTName + " "+ response.ResultContext +" "+DateTime.Now);
                    response.ResultContext = response.ResultContext+"||"+dT2SDT._xdException.Message;
                    return BadRequest(response).ExecuteResultAsync(_context);
                }
            }
            string errMsg = string.Format("{0} 从网盘下载账套{1}文件 {2} 失败: ",xfile.CustomName, xfile.ZTName, xfile.FileName);
            _logger.LogError(errMsg);
            response.ResultContext = errMsg;
            return BadRequest(response).ExecuteResultAsync(_context); 

        }
        [HttpPost]
        [Route("XData2EAS")]
        public Task XData2EAS([FromBody] Models.xfile xfile)
        {
            ActionContext _context = this.ControllerContext;
            XDataResponse response = new XDataResponse();
            string key = xfile.XID + xfile.ZTID + xfile.CustomID + xfile.FileName;
            if (!StaticData.X2EasList.ContainsKey(key))
            {
                StaticData.X2EasList.Add(key, 1);
            }
            else if (StaticData.X2EasList[key] == 1)
            {
                response.ResultContext = key+" 已经在执行过程中...";
                return BadRequest(response).ExecuteResultAsync(_context);
            }
            var dapper = DapperHelper<xfile>.Create("XDataConn");
            dapper.conStr = StaticUtil.GetConfigValueByKey("XDataConn").Replace("master", xfile.ZTID);
         
            string projectID = xfile.ProjectID;
            string logName = string.Empty;
            string pwd = string.Empty;
            string ip = string.Empty;
            string dbName = string.Empty;
            string scon = xfile.DbName; 
            string[] sValue = scon.Split(';');
            foreach (var s in sValue)
            {
                if (s.StartsWith("Server="))
                {
                    ip = s.Replace("Server=", "");
                }
                if (s.StartsWith("Database="))
                {
                    dbName = s.Replace("Database=", "");
                }
                if (s.StartsWith("User ID="))
                {
                    logName = s.Replace("User ID=", "");
                }
                if (s.StartsWith("Password="))
                {
                    pwd = s.Replace("Password=", "");
                }
            }
            string linkSvrName=  SqlServerHelper.GetLinkServer(StaticUtil.GetConfigValueByKey("XDataConn"), ip, logName, pwd, ip);
            if (!string.IsNullOrEmpty(linkSvrName))
            {
                linkSvrName = "[" + linkSvrName + "]";
                StringBuilder sb = new StringBuilder();
                sb.Append(" SET XACT_ABORT ON   go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[ProjectType] where projectid ='{0}' ", projectID,linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[ProjectType](ProjectID,TYPECODE,TYPENAME) select '{0}' as ProjectID,TYPECODE,TYPENAME from [ProjectType] ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[ProJect] where projectid ='{0}' ", projectID, linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[ProJect](ProjectID,TYPECODE,PROJECTCODE,PROJECTNAME,UPPERCODE,JB,ISMX) select '{0}' as ProjectID,TYPECODE,PROJECTCODE,PROJECTNAME,UPPERCODE,JB,ISMX from [ProJect] ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.ACCOUNT where projectid ='{0}' ", projectID, linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[ACCOUNT](ProjectID,AccountCode,UpperCode,AccountName,Attribute,Jd,Hsxms,TypeCode,Jb,IsMx,Ncye,Qqccgz,Jfje,Dfje,Ncsl,Syjz) select '{0}' as ProjectID,AccountCode,UpperCode,AccountName,Attribute,Jd,Hsxms,TypeCode,Jb,IsMx,Ncye,Qqccgz,Jfje,Dfje,Ncsl,Syjz from ACCOUNT ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBVoucher] where projectid ='{0}' ", projectID, linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBVoucher](VoucherID,ProjectID,Clientid,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID) select  NEWID() as VoucherID, '{0}' as ProjectID,Clientid,IncNo,Date,left(CONVERT(varchar(12) ,Date, 112),6) as Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID from  TBVoucher ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[AuxiliaryFDetail] where projectid ='{0}' ", projectID, linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[AuxiliaryFDetail] (ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear) select '{0}' as ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear from AuxiliaryFDetail ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBAux] where projectid ='{0}' ", projectID,linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBAux](ProjectID,AccountCode,AuxiliaryCode,AuxiliaryName,FSCode,kmsx,YEFX,TBGrouping,Sqqmye,Qqccgz,jfje,dfje,qmye) select '{0}' as ProjectID,AccountCode,AuxiliaryCode,AuxiliaryName,FSCode,kmsx,YEFX,TBGrouping,Sqqmye,Qqccgz,jfje,dfje,qmye from [TBAux] ", projectID,linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBDetail] where projectid ='{0}' ", projectID, linkSvrName, dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBDetail] (ProjectID,[ID],[FSCode],[AccountCode],[AuxiliaryCode],[AccAuxName],[DataType],[TBGrouping],[TBType],[IsAccMx],[IsMx],[IsAux],[kmsx],[Yefx],[SourceFSCode],[Sqqmye],[Qqccgz],[jfje],[dfje],[CrjeJF],[CrjeDF] ,[AjeJF] ,[AjeDF],[RjeJF],[RjeDF],[TaxBase],[PY1],[jfje1],[dfje1],[jfje2],[dfje2]) select  '{0}' as ProjectID,[ID],[FSCode],[AccountCode],[AuxiliaryCode],[AccAuxName],[DataType],[TBGrouping],[TBType],[IsAccMx],[IsMx],[IsAux],[kmsx],[Yefx],[SourceFSCode],[Sqqmye],[Qqccgz],[jfje],[dfje],[CrjeJF],[CrjeDF] ,[AjeJF] ,[AjeDF],[RjeJF],[RjeDF],[TaxBase],[PY1],[jfje1],[dfje1],[jfje2],[dfje2] from [TBDetail] ", projectID, linkSvrName, dbName);
                               
                string[] sqlarr = sb.ToString().Split(new[] { " GO ", " go " }, StringSplitOptions.RemoveEmptyEntries);                
                int ret = dapper.ExecuteTransaction(sqlarr);
                if (ret > 0)
                {
                    dapper.Execute(" update  xdata.dbo.XFiles set datastatus =1 where xid="+xfile.XID , null);
                    response.HttpStatusCode = 200;
                    response.ResultContext = string.Format("{0}项目导入EAS成功", xfile.ProjectID, xfile.CustomID);
                    _logger.LogInformation(response.ResultContext);
                    StaticData.X2EasList[key]++;
                    return Ok(response).ExecuteResultAsync(_context); ;

                }
                dapper.Execute(" update  xdata.dbo.XFiles set datastatus =2 where xid=" + xfile.XID, null); 
            }
            StaticData.X2EasList[key] = 0;
            response.HttpStatusCode = 500;
            response.ResultContext = string.Format("{0}项目导入EAS失败", xfile.ProjectID, xfile.CustomID);
            _logger.LogError(response.ResultContext);
            return BadRequest(response).ExecuteResultAsync(_context); ;

        }
    }
}