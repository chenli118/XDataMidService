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
            StaticData.X2SqlList[key] = 1;
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
            StaticData.X2SqlList[key] = 0;
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
            string constr = StaticUtil.GetConfigValueByKey("XDataConn");
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
            StaticData.X2EasList[key] = 1;
            var dapper = DapperHelper<xfile>.Create("XDataConn");
            string localDbName = StaticUtil.GetLocalDbNameByXFile(xfile);
            _logger.LogInformation("开始转换数据： " +localDbName);
            string qdb = "select 1 from sys.databases where name ='"+ localDbName + "'";
            var thisdb = SqlMapperUtil.SqlWithParamsSingle<int>(qdb, null,constr);
            if (thisdb != 1)
            {
                response.ResultContext = key + " 数据没有准备！ ";
                StaticData.X2EasList[key] = 0;
                return BadRequest(response).ExecuteResultAsync(_context);
            }
            dapper.conStr = constr.Replace("master", localDbName);         
            string projectID = xfile.ProjectID;
            var tbv = GetLinkSrvName(xfile.DbName, constr);
            string linkSvrName =tbv.Item1;
            string dbName = tbv.Item2;           
            if (!string.IsNullOrEmpty(linkSvrName))
            {
                linkSvrName = "[" + linkSvrName + "]";
                localDbName ="["+ localDbName+"]";
                StringBuilder sb = new StringBuilder();
                sb.Append(" SET XACT_ABORT ON   go ");
               
                if (xfile.periodType == 0)
                {                    
                    sb.AppendFormat(" delete from  {1}.{2}.dbo.[kjqj] where projectid ='{0}' ", projectID, linkSvrName, dbName);
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.kjqj ([ProjectID],[KJDate]) select '{0}' as ProjectID,[KJDate] from {3}.dbo.[kjqj] ", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" delete from  {1}.{2}.dbo.[ProjectType] where projectid ='{0}' ", projectID, linkSvrName, dbName);
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[ProjectType](ProjectID,TYPECODE,TYPENAME) select '{0}' as ProjectID,TYPECODE,TYPENAME from {3}.dbo.[ProjectType] ", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" delete from  {1}.{2}.dbo.[ProJect] where projectid ='{0}' ", projectID, linkSvrName, dbName);
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[ProJect](ProjectID,TYPECODE,PROJECTCODE,PROJECTNAME,UPPERCODE,JB,ISMX) select '{0}' as ProjectID,TYPECODE,PROJECTCODE,PROJECTNAME,UPPERCODE,JB,ISMX from {3}.dbo.[ProJect] ", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" delete from  {1}.{2}.dbo.ACCOUNT where projectid ='{0}' ", projectID, linkSvrName, dbName);
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[ACCOUNT](ProjectID,AccountCode,UpperCode,AccountName,Attribute,Jd,Hsxms,TypeCode,Jb,IsMx,Ncye,Qqccgz,Jfje,Dfje,Ncsl,Syjz) select '{0}' as ProjectID,AccountCode,UpperCode,AccountName,Attribute,Jd,Hsxms,TypeCode,Jb,IsMx,Ncye,Qqccgz,Jfje,Dfje,Ncsl,Syjz from {3}.dbo.ACCOUNT ", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBVoucher] where projectid ='{0}' ", projectID, linkSvrName, dbName);
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBVoucher](ProjectID,Clientid,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID,HashCode) select  '{0}' as ProjectID,'" + xfile.ClientID + "' as ClientID,IncNo,Date,left(CONVERT(varchar(12) ,Date, 112),6) as Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID, HashCode " +
                        " from  {3}.dbo.TBVoucher where Date<='" + xfile.periodEndDate +"'", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" delete from  {1}.{2}.dbo.[AuxiliaryFDetail] where projectid ='{0}' ", projectID, linkSvrName, dbName);
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[AuxiliaryFDetail] (ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear) select '{0}' as ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear from {3}.dbo. AuxiliaryFDetail ", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBAux] where projectid ='{0}' ", projectID, linkSvrName, dbName);
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBAux](ProjectID,AccountCode,AuxiliaryCode,AuxiliaryName,FSCode,kmsx,YEFX,TBGrouping,Sqqmye,Qqccgz,jfje,dfje,qmye) select '{0}' as ProjectID,AccountCode,AuxiliaryCode,AuxiliaryName,FSCode,kmsx,YEFX,TBGrouping,Sqqmye,Qqccgz,jfje,dfje,qmye from {3}.dbo.[TBAux] ", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBDetail] where projectid ='{0}' ", projectID, linkSvrName, dbName);
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBDetail] (ProjectID,[ID],[FSCode],[AccountCode],[AuxiliaryCode],[AccAuxName],[DataType],[TBGrouping],[TBType],[IsAccMx],[IsMx],[IsAux],[kmsx],[Yefx],[SourceFSCode],[Sqqmye],[Qqccgz],[jfje],[dfje],[CrjeJF],[CrjeDF] ,[AjeJF] ,[AjeDF],[RjeJF],[RjeDF],[TaxBase],[PY1],[jfje1],[dfje1],[jfje2],[dfje2]) select  '{0}' as ProjectID,[ID],[FSCode],[AccountCode],[AuxiliaryCode],[AccAuxName],[DataType],[TBGrouping],[TBType],[IsAccMx],[IsMx],[IsAux],[kmsx],[Yefx],[SourceFSCode],[Sqqmye],[Qqccgz],[jfje],[dfje],[CrjeJF],[CrjeDF] ,[AjeJF] ,[AjeDF],[RjeJF],[RjeDF],[TaxBase],[PY1],[jfje1],[dfje1],[jfje2],[dfje2] from {3}.dbo.[TBDetail] ", projectID, linkSvrName, dbName, localDbName);
                }

                else if (xfile.periodType == 1)
                {
                    sb.AppendFormat(" select hashcode into #h1 from {1}.{2}.dbo.tbvoucher t where t.projectid ='{0}'", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" insert into {1}.{2}.dbo.qhjzpz (ClientID,ProjectID,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,Dfje,Jfsl,Dfsl,ZDR,dfkm,FDetailID) " +
                        " select '" + xfile.ClientID + "' as ClientID,'{0}' as ProjectID,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,Dfje,Jfsl,Dfsl,ZDR,dfkm,FDetailID " +
                        " from TBVoucher a where a.hashcode not in ( select hashcode from #h1) ",projectID,linkSvrName,dbName);
                    sb.Append(" go ");
                }
                else if (xfile.periodType == -1) 
                {
                    sb.AppendFormat(" select hashcode into #h2 from {1}.{2}.dbo.tbvoucher t where t.projectid ='{0}'", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" insert into {1}.{2}.dbo.Qcwljzpz (ClientID,ProjectID,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,Dfje,Jfsl,Dfsl,ZDR,dfkm,FDetailID) " +
                         " select '" + xfile.ClientID + "' as ClientID,'{0}' as ProjectID,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,Dfje,Jfsl,Dfsl,ZDR,dfkm,FDetailID " +
                         " from TBVoucher a where a.hashcode not in ( select hashcode from  #h2 ) ", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                }               
                string[] sqlarr = sb.ToString().Split(new[] { " GO ", " go " }, StringSplitOptions.RemoveEmptyEntries);                
                int ret = dapper.ExecuteTransaction(sqlarr);
                if (ret > 0)
                {
                    dapper.Execute(" update  xdata.dbo.XFiles set datastatus =1 where xid="+xfile.XID , null);
                    response.HttpStatusCode = 200;
                    response.ResultContext = string.Format("{0}项目数据{1}导入EAS成功", xfile.ProjectID, localDbName);
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
        
        private Tuple<string, string> GetLinkSrvName(string connectInfo,string localCon)
        {
            connectInfo = connectInfo.Replace("Asynchronous Processing=true", "");
            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(connectInfo); 
            string linkSvrName = SqlServerHelper.GetLinkServer(localCon, csb.DataSource, csb.UserID, csb.Password, csb.DataSource);
            return  new Tuple<string,string>(linkSvrName, csb.InitialCatalog);
        }
        private bool UpdateTBDetailAndTBAux(string conStr ,DateTime pzEndDate)
        {
            try
            {
                var p = new DynamicParameters();
                p.Add("@pzEndDate", pzEndDate);
                SqlMapperUtil.InsertUpdateOrDeleteStoredProc("UpdateTBDetailTBAuxJE", p, conStr);
            }
            catch (Exception err)
            {
                _logger.LogError(err.Message);
                return false;
            }
            return true;
        }
    }
}