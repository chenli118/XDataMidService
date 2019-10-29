﻿using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using XDataMidService.Controllers;
using XDataMidService.Models;

namespace XDataMidService.BPImp
{
    public class XData2EasService
    {
        private static ILogger<XDataController> _logger;
        private static System.Threading.Timer _timer;
        public static  List<xfile> _stack ;
        public List<xfile> stack
        {
            get { return _stack; } 
        }
        public XData2EasService(ILogger<XDataController> logger)
        {
            _logger = logger; 
            if (_stack == null)
            {
                _stack = new List<xfile>();
            }
            if (_timer == null && _stack.Count>0)
            {
                var autoEvent = new AutoResetEvent(false);
                _timer = new Timer(p => DoProcessList(), autoEvent, 0, 10000); //定时清理队列;
                autoEvent.WaitOne();
            }
        }

        private void DoProcessList()
        {
            if (_stack.Count > 0)
            {
                var xf = _stack.Where(x => !StaticData.X2EasList.ContainsValue(StaticUtil.GetLocalDbNameByXFile(x))).FirstOrDefault();
                if (xf != null)
                {
                    var dapper = DapperHelper<xfile>.Create("XDataConn");
                    XDataResponse response = Real2EasImp(xf);
                    if (response.HttpStatusCode == 200)
                    {
                        dapper.Execute(string.Format(" update  xdata.dbo.XFiles set datastatus =999,projectid='{1}' where xid={0} ", xf.XID, xf.ProjectID), null);
                    }
                    else
                    {
                        dapper.Execute(string.Format(" update  xdata.dbo.XFiles set datastatus =998,projectid='{1}' where xid={0} ", xf.XID, xf.ProjectID), null);
                    }
                    _stack.Remove(xf);
                }
            }
            else
            {
                _timer.Dispose();
                _timer = null;
            
            }
        }

        public XDataResponse Real2EasImp(xfile xfile)
        {
            XDataResponse response = new XDataResponse();
            string constr = StaticUtil.GetConfigValueByKey("XDataConn");
            string localDbName = StaticUtil.GetLocalDbNameByXFile(xfile);
            string key = xfile.XID + xfile.ZTID + xfile.CustomID + xfile.FileName;
            if (!StaticData.X2EasList.ContainsKey(key))
            {
                StaticData.X2EasList.Add(key, localDbName);
            }
            else if (StaticData.X2EasList[key] == localDbName)
            {
                response.ResultContext = key + " 已经在执行过程中...";
                return response;
            }
            StaticData.X2EasList[key] = localDbName;
            var dapper = DapperHelper<xfile>.Create("XDataConn");
            _logger.LogInformation("开始转换数据： " + localDbName);
            string qdb = "select 1 from sys.databases where name ='" + localDbName + "'";
            var thisdb = SqlMapperUtil.SqlWithParamsSingle<int>(qdb, null, constr);
            if (thisdb != 1)
            {
                response.ResultContext = localDbName + " 数据没有准备！ ";
                response.HttpStatusCode = 500;
                return response;
            }
            dapper.conStr = constr.Replace("master", localDbName);
            string projectID = xfile.ProjectID;
            var tbv = GetLinkSrvName(xfile.DbName, constr);
            string linkSvrName = tbv.Item1;
            string dbName = tbv.Item2;
            if (!string.IsNullOrEmpty(linkSvrName))
            {
                linkSvrName = "[" + linkSvrName + "]";
                localDbName = "[" + localDbName + "]";
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
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBVoucher](ProjectID,Clientid,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID,HashCode) select  '{0}' as ProjectID,'" + xfile.ClientID + "' as ClientID,IncNo,Date, Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID, HashCode " +
                        " from  {3}.dbo.TBVoucher where Date<='" + xfile.periodEndDate + "'", projectID, linkSvrName, dbName, localDbName);
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
                    sb.AppendFormat(" select hashcode into #h1 from {1}.{2}.dbo.qhjzpz t where t.projectid ='{0}'", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" insert into {1}.{2}.dbo.qhjzpz ( HashCode,ClientID,ProjectID,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,Dfje,Jfsl,Dfsl,ZDR,dfkm,FDetailID) " +
                        " select HashCode,'" + xfile.ClientID + "' as ClientID,'{0}' as ProjectID,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,Dfje,Jfsl,Dfsl,ZDR,dfkm,FDetailID " +
                        " from TBVoucher a where a.hashcode not in ( select hashcode from #h1) ", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" select HASHBYTES('SHA1', (select z.Accountcode,z.AuxiliaryCode,z.FDetailID,z.DataYear FOR XML RAW, BINARY BASE64)) as HashCode  into #f2 from {1}.{2}.dbo.AuxiliaryFDetail  z  where z.projectid ='{0}'", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[AuxiliaryFDetail] (ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear) select '{0}' as ProjectID,AccountCode,auxiliaryCode,fdetailid," + xfile.periodType + " as datatype,datayear " +
                       " from {3}.dbo. AuxiliaryFDetail f where f.hashcode not in (select hashcode from #f2) ", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");

                }
                else if (xfile.periodType == -1)
                {
                    sb.AppendFormat(" select hashcode into #h2 from {1}.{2}.dbo.Qcwljzpz t where t.projectid ='{0}'", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" insert into {1}.{2}.dbo.Qcwljzpz ( HashCode,ClientID,ProjectID,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,Dfje,Jfsl,Dfsl,ZDR,dfkm,FDetailID) " +
                         " select HashCode,'" + xfile.ClientID + "' as ClientID,'{0}' as ProjectID,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,Dfje,Jfsl,Dfsl,ZDR,dfkm,FDetailID " +
                         " from TBVoucher a where a.hashcode not in ( select hashcode from  #h2 ) ", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" select HASHBYTES('SHA1', (select z.Accountcode,z.AuxiliaryCode,z.FDetailID,z.DataYear FOR XML RAW, BINARY BASE64)) as HashCode  into #f1 from {1}.{2}.dbo.AuxiliaryFDetail  z  where z.projectid ='{0}'", projectID, linkSvrName, dbName);
                    sb.Append(" go ");
                    sb.AppendFormat(" insert into  {1}.{2}.dbo.[AuxiliaryFDetail] (ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear) select '{0}' as ProjectID,AccountCode,auxiliaryCode,fdetailid," + xfile.periodType + " as datatype,datayear " +
                       " from {3}.dbo. AuxiliaryFDetail f where f.hashcode not in (select hashcode from #f1) ", projectID, linkSvrName, dbName, localDbName);
                    sb.Append(" go ");


                }
                string[] sqlarr = sb.ToString().Split(new[] { " GO ", " go " }, StringSplitOptions.RemoveEmptyEntries);
                int ret = dapper.ExecuteTransactionAndDBSigleUser(sqlarr);
                if (ret > 0)
                {
                    dapper.Execute(string.Format(" update  xdata.dbo.XFiles set datastatus =1,projectid='{1}' where xid={0} ", xfile.XID, projectID), null);
                    response.HttpStatusCode = 200;
                    response.ResultContext = string.Format("{0}项目数据{1}导入EAS成功", xfile.ProjectID, localDbName);
                    _logger.LogInformation(response.ResultContext);
                    StaticData.X2EasList[key] = "";
                    return response;

                }

            }
            dapper.Execute(string.Format(" update  xdata.dbo.XFiles set datastatus =2,projectid='{1}' where xid={0} ", xfile.XID, projectID), null);
            response.HttpStatusCode = 500;
            response.ResultContext = string.Format("{0}项目导入EAS失败", xfile.ProjectID, xfile.CustomID);
            _logger.LogError(response.ResultContext);
            StaticData.X2EasList[key] = "";
            return response;
        }


        private Tuple<string, string> GetLinkSrvName(string connectInfo, string localCon)
        {
            connectInfo = connectInfo.Replace("Asynchronous Processing=true", "");
            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(connectInfo);
            string linkSvrName = SqlServerHelper.GetLinkServer(localCon, csb.DataSource, csb.UserID, csb.Password, csb.DataSource);
            return new Tuple<string, string>(linkSvrName, csb.InitialCatalog);
        }
    }
}