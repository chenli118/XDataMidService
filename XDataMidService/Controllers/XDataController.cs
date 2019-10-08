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
        }
        [HttpPost]
        [Route("XData2SQL")]
        public Task<HttpResponseMessage> XData2SQL([FromBody] Models.xfile xfile)
        {
            string targetPath = xfile.FileName;
            string wp_GUID = xfile.CustomID;
            string projectID = xfile.ZTID.Replace("-", "");
            PDT2SDT dT2SDT = new PDT2SDT(targetPath, wp_GUID, projectID, xfile);
            if (dT2SDT.DownLoadFile(xfile))
                return dT2SDT.Start();
            HttpRequestMessage requestMessage = new HttpRequestMessage();
            return new XDataReqResult(string.Format("{0}下载{1}文件失败",xfile.CustomName,xfile.FileName), "从网盘下载文件失败", System.Net.HttpStatusCode.ExpectationFailed, requestMessage).ExecuteAsync();

        }
        [HttpPost]
        [Route("XData2EAS")]
        public Task<HttpResponseMessage> XData2EAS([FromBody] Models.xfile xfile)
        {
            HttpRequestMessage requestMessage = new HttpRequestMessage();
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
            string linkSvrName=  SqlServerHelper.GetLinkServer(StaticUtil.GetConfigValueByKey("XDataConn"), "XData2Eas", logName, pwd, ip);
            if (!string.IsNullOrEmpty(linkSvrName)) 
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(" SET XACT_ABORT ON   go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[ProjectType] where projectid ='{0}' ", projectID,linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[ProjectType] select '{0}' as ProjectID,TYPECODE,TYPENAME from [ProjectType] ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[ProJect] where projectid ='{0}' ", projectID, linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[ProJect] select '{0}' as ProjectID,TYPECODE,PROJECTCODE,PROJECTNAME,UPPERCODE,JB,ISMX from [ProJect] ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.ACCOUNT where projectid ='{0}' ", projectID, linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[ACCOUNT] select '{0}' as ProjectID,AccountCode,UpperCode,AccountName,Attribute,Jd,Hsxms,TypeCode,Jb,IsMx,Ncye,Qqccgz,Jfje,Dfje,Ncsl,Syjz from ACCOUNT ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBVoucher] where projectid ='{0}' ", projectID, linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBVoucher] select  NEWID() as VoucherID, '{0}' as ProjectID,Clientid,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID from  TBVoucher ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[AuxiliaryFDetail] where projectid ='{0}' ", projectID, linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[AuxiliaryFDetail] select '{0}' as ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear from AuxiliaryFDetail ", projectID, linkSvrName,dbName);
                sb.Append(" go ");
                sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBAux] where projectid ='{0}' ", projectID,linkSvrName,dbName);
                sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBAux] select '{0}' as ProjectID,AccountCode,AuxiliaryCode,AuxiliaryName,FSCode,kmsx,YEFX,TBGrouping,Sqqmye,Qqccgz,jfje,dfje,qmye from [TBAux] ", projectID,linkSvrName,dbName);
                
                string[] sqlarr = sb.ToString().Split(new[] { " GO ", " go " }, StringSplitOptions.RemoveEmptyEntries);
                var dapper = DapperHelper<xfile>.Create("XDataConn");
                dapper.conStr = StaticUtil.GetConfigValueByKey("XDataConn").Replace("master", xfile.ZTID);
                int ret = dapper.ExecuteTransaction(sqlarr);
                if (ret > 0)
                {
                    return new XDataReqResult(string.Format("{0}项目导入EAS成功", xfile.ProjectID, xfile.CustomID), "EAS导数成功", System.Net.HttpStatusCode.OK, requestMessage).ExecuteAsync();

                }

                //string xRecords = "select *  from  XData2Eas.neweasv5.dbo.AuthorizeXFiles x where   HASHBYTES('SHA1', (select x.* FOR XML RAW, BINARY BASE64)) " +
                //    " not in(select    HASHBYTES('SHA1', (select x0.* FOR XML RAW, BINARY BASE64)) hashcode from xdata.dbo.AuthorizeXFiles x0) ";
                //List<xfile> xlist= DapperHelper<xfile>.Create("XDataConn").Query(xRecords, null);
                //foreach (var xf in xlist)
                //{
                //    XData2SQL(xf).ContinueWith(delegate {
                //        //SqlMapperUtil.InsertUpdateOrDeleteSql()
                //        //DapperHelper<xfile>.Create("XDataConn").Execute("insert", null);
                //    });                    

                //}


            }


            return new XDataReqResult(string.Format("{0}项目导入EAS失败", xfile.CustomName, xfile.CustomID), "EAS导数失败", System.Net.HttpStatusCode.InternalServerError, requestMessage).ExecuteAsync();

        }
    }
}