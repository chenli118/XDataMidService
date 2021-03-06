﻿using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
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
        private static System.Timers.Timer _timer;
        public static List<xfile> _stack;
        public xfile Xfile
        {
            set
            {
                _stack.Add(value);
                if (_timer == null)
                {
                    _timer = new System.Timers.Timer(10000);
                    _timer.Elapsed += _timer_Elapsed; ;
                    _timer.AutoReset = true;
                    _timer.Enabled = true;
                    _timer.Start();
                }
            }
        }

        private void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            DoProcessList();
        }

        public XData2EasService(ILogger<XDataController> logger)
        {
            _logger = logger;
            if (_stack == null)
            {
                _stack = new List<xfile>();
            }
        }
        [MethodImpl(MethodImplOptions.Synchronized)]
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
                if (_timer != null)
                {
                    _timer.Stop();
                    _timer.Enabled = false;
                }
                _timer = null;

            }
        }

        public XDataResponse Real2EasImp(xfile xfile)
        {
            XDataResponse response = new XDataResponse();
            string key = xfile.XID + xfile.ZTID + xfile.CustomID + xfile.FileName;
            try
            {
                var dapper = DapperHelper<xfile>.Create("XDataConn");
                string constr = StaticUtil.GetConfigValueByKey("XDataConn");
                string localDbName = StaticUtil.GetLocalDbNameByXFile(xfile);
                string qdb = "select 1 from sys.databases where name = '" + localDbName+"'";
                var thisdb = SqlMapperUtil.SqlWithParamsSingle<int>(qdb, null, constr);
                if (thisdb != 1)
                {
                    dapper.Execute(" delete from xdata.dbo.xfiles where xid = "+xfile.XID,null);
                    response.HttpStatusCode = 500;
                    response.ResultContext = string.Format("文件{0} 项目{1} 数据未准备好，请重试！", xfile.XID, xfile.ProjectID);
                    _logger.LogError(response.ResultContext  +" " + DateTime.Now);

                    return response;
                }
                if (StaticData.X2EasList.ContainsKey(key))
                    StaticData.X2EasList[key] = xfile.DbName;
                else
                    StaticData.X2EasList.Add(key, xfile.DbName);

                _logger.LogInformation(xfile.XID + " 开始转换 " + xfile.ProjectID + " 数据到EAS" + DateTime.Now);

                string projectID = xfile.ProjectID;
                SqlConnectionStringBuilder sqlConnectionStringBuilder = new SqlConnectionStringBuilder(dapper.conStr);
                sqlConnectionStringBuilder.InitialCatalog = localDbName;

                dapper.conStr = sqlConnectionStringBuilder.ConnectionString;
                var tbv = SqlServerHelper.GetLinkSrvName(xfile.DbName, constr);
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
                        //sb.AppendFormat(" select hashcode into #hc from {1}.{2}.dbo.TBVoucher t where t.projectid ='{0}' ", projectID, linkSvrName, dbName);
                        //sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBVoucher] where projectid ='{0}' and hashcode not in(select hashcode from #hc) ", projectID, linkSvrName, dbName);
                        //sb.Append(" go ");
                        sb.AppendFormat(" delete from  {1}.{2}.dbo.[TBVoucher] where projectid ='{0}'  ", projectID, linkSvrName, dbName);
                        sb.AppendFormat(" insert into  {1}.{2}.dbo.[TBVoucher](ProjectID,Clientid,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID,HashCode) " +
                               " select  '{0}' as ProjectID,'" + xfile.ClientID + "' as ClientID,IncNo,Date, Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID, HashCode " +
                               " from  {3}.dbo.TBVoucher where Date<='" + xfile.periodEndDate + "'", projectID, linkSvrName, dbName, localDbName);
                        if (xfile.periodBeginDate != null && xfile.periodBeginDate > DateTime.MinValue)
                        {
                            sb.Append("  and Date>'" + xfile.periodBeginDate + "'");
                        }
                       
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
                            " from {3}.dbo.TBVoucher  a  where a.date>'{4}' and a.hashcode not in ( select hashcode from #h1) ", projectID, linkSvrName, dbName, localDbName, xfile.periodEndDate);
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
                             " from {3}.dbo.TBVoucher a where a.hashcode not in ( select hashcode from  #h2 ) ", projectID, linkSvrName, dbName, localDbName);
                        sb.Append(" go ");
                        sb.AppendFormat(" select HASHBYTES('SHA1', (select z.Accountcode,z.AuxiliaryCode,z.FDetailID,z.DataYear FOR XML RAW, BINARY BASE64)) as HashCode  into #f1 from {1}.{2}.dbo.AuxiliaryFDetail  z  where z.projectid ='{0}'", projectID, linkSvrName, dbName);
                        sb.Append(" go ");
                        sb.AppendFormat(" insert into  {1}.{2}.dbo.[AuxiliaryFDetail] (ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear) select '{0}' as ProjectID,AccountCode,auxiliaryCode,fdetailid," + xfile.periodType + " as datatype,datayear " +
                           " from {3}.dbo. AuxiliaryFDetail f where f.hashcode not in (select hashcode from #f1) ", projectID, linkSvrName, dbName, localDbName);
                        sb.Append(" go ");


                    }
                    string[] sqlarr = sb.ToString().Split(new[] { " GO ", " go " }, StringSplitOptions.RemoveEmptyEntries);
                    var ret = dapper.ExecuteTransactionAndDBSigleUser(sqlarr);
                    if (ret.Item1 > 0)
                    {
                        dapper.Execute(string.Format(" update  xdata.dbo.XFiles set datastatus =999,projectid='{1}' where xid={0} ", xfile.XID, projectID), null);
                        response.HttpStatusCode = 200;
                        response.ResultContext = string.Format("项目{0}已导入EAS", xfile.ProjectID, localDbName);
                        _logger.LogInformation(response.ResultContext + " " + DateTime.Now);

                        return response;

                    }
                    else
                    {
                        response.ResultContext += ret.Item2;
                    }

                }
                dapper.Execute(string.Format(" update  xdata.dbo.XFiles set datastatus =2,projectid='{1}' where xid={0} ", xfile.XID, projectID), null);
                response.HttpStatusCode = 500;
                response.ResultContext = string.Format("{0}项目导入EAS失败,因为：{1}", xfile.ProjectID, response.ResultContext);
                _logger.LogError(response.ResultContext + " " + DateTime.Now);

                return response;
            }
            catch (Exception et)
            {

                response.HttpStatusCode = 500;
                response.ResultContext = string.Format("文件{0} 项目{1}导入EAS失败,请联系系统管理员！", xfile.XID, xfile.ProjectID);
                _logger.LogError(response.ResultContext + " 异常：" + et.Message + " " + DateTime.Now);

                return response;
            }
            finally
            {
                StaticData.X2EasList[key] = "";
            }
        }
        public XDataResponse AppedXDataByXFile(xfile xfile)
        {
            XDataResponse response = new XDataResponse();
            string key = xfile.XID + "_AppedXDataByXFile";
            try
            {
                var dapper = DapperHelper<xfile>.Create("XDataConn");
                string constr = StaticUtil.GetConfigValueByKey("XDataConn");
                string localDbName = StaticUtil.GetLocalDbNameByXFile(xfile);
                string qdb = "select 1 from sys.databases where name = '" + localDbName + "'";
                var thisdb = SqlMapperUtil.SqlWithParamsSingle<int>(qdb, null, constr);
                if (thisdb != 1)
                {
                    dapper.Execute(" delete from xdata.dbo.xfiles where xid = " + xfile.XID, null);
                    response.HttpStatusCode = 500;
                    response.ResultContext = string.Format("文件{0} 项目{1} 数据未准备好，请重试！", xfile.XID, xfile.ProjectID);
                    _logger.LogError(response.ResultContext + " " + DateTime.Now);

                    return response;
                }
                if (StaticData.X2EasList.ContainsKey(key))
                    StaticData.X2EasList[key] = xfile.DbName;
                else
                    StaticData.X2EasList.Add(key, xfile.DbName);

                _logger.LogInformation(xfile.XID + " 开始转换 " + xfile.ProjectID + " 数据到EAS" + DateTime.Now);


                string projectID = xfile.ProjectID;
                SqlConnectionStringBuilder sqlConnectionStringBuilder = new SqlConnectionStringBuilder(dapper.conStr);
                sqlConnectionStringBuilder.InitialCatalog = localDbName;

                dapper.conStr = sqlConnectionStringBuilder.ConnectionString;
                var tbv = SqlServerHelper.GetLinkSrvName(xfile.DbName, constr);
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
                        sb.AppendFormat(" select hashcode,VoucherID into #remotePZ from {1}.{2}.dbo.TBVoucher t where t.projectid ='{0}' ", projectID, linkSvrName, dbName);
                        sb.Append(" go ");
                        sb.AppendFormat(" select  NEWID() as VoucherID,  '{0}' as ProjectID,'" + xfile.ClientID + "' as ClientID,IncNo,Date, Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID, HashCode " +
                               "   into #localPZ  from  {1}.dbo.TBVoucher a where a.Date<='" + xfile.periodEndDate + "' and a.hashcode not in (select hashcode from #remotePZ)", projectID, localDbName);
                        if (xfile.periodBeginDate != null && xfile.periodBeginDate > DateTime.MinValue)
                        {
                            sb.Append("  and Date>'" + xfile.periodBeginDate + "'");
                        }
                        sb.Append(" go ");
                        sb.AppendFormat(" insert into  {0}.{1}.dbo.[TBVoucher](VoucherID,ProjectID,Clientid,IncNo,Date,Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID,HashCode) " +
                               " select VoucherID,ProjectID,ClientID,IncNo,Date, Period,Pzlx,Pzh,Djh,AccountCode,ProjectCode,Zy,Jfje,dfje,jfsl,dfsl,zdr,dfkm,jd,Fsje,Wbdm,wbje,Hl,FLLX,SampleSelectedYesNo,SampleSelectedType,TBGrouping,EASREF,AccountingAge,qmyegc,Stepofsample,ErrorYesNo,FDetailID, HashCode " +
                               " from  #localPZ ", linkSvrName, dbName); 
                        sb.Append(" go ");
                        sb.AppendFormat(" select HASHBYTES('SHA1', (select z.Accountcode,z.AuxiliaryCode,z.FDetailID,z.DataYear FOR XML RAW, BINARY BASE64)) as HashCode,Accountcode+AuxiliaryCode as VoucherID  into #remoteFZ from {1}.{2}.dbo.AuxiliaryFDetail  z  where z.projectid ='{0}' and z.datatype ={3} ", projectID, linkSvrName, dbName, xfile.periodType);
                        sb.Append(" go ");
                        sb.AppendFormat(" select '{0}' as ProjectID,AccountCode,auxiliaryCode,fdetailid,{2} as datatype,datayear " +
                           " into #localFZ  from {1}.dbo. AuxiliaryFDetail f where f.hashcode not in (select hashcode from #remoteFZ) ", projectID, localDbName, xfile.periodType);


                        sb.Append(" go ");
                        sb.AppendFormat(" insert into  {0}.{1}.dbo.[AuxiliaryFDetail] (ProjectID,AccountCode,auxiliaryCode,fdetailid,datatype,datayear) select  ProjectID,AccountCode,auxiliaryCode,fdetailid, datatype,datayear " +
                           " from  #localFZ ", linkSvrName, dbName);


                        //差异记录表AppedXDataDiff

                        sb.Append(" go ");
                        sb.Append(" IF OBJECT_ID(N'dbo.AppedXDataDiff', N'U') IS  NOT  NULL  DROP TABLE dbo.AppedXDataDiff; ");
                        sb.Append(" go ");
                        sb.Append(" Create TABLE AppedXDataDiff(PZNewMore nvarchar(50) null,PZOldMore nvarchar(50) null,FZNewMore nvarchar(248) null,FZOldMore nvarchar(248) null) ");
                        sb.Append(" go ");
                        sb.AppendFormat(" select ROW_NUMBER() over(order by VoucherID) id,VoucherID as PZNewMore into #PZNewMore from  #localPZ ", localDbName);
                        sb.Append(" go ");
                        sb.AppendFormat(" select ROW_NUMBER() over(order by VoucherID) id,VoucherID as  PZOldMore into #PZOldMore from #remotePZ  where hashcode not in (select hashcode from {0}.dbo.TBVoucher) ", localDbName);
                        sb.Append(" go ");
                        sb.AppendFormat(" select ROW_NUMBER() over(order by Accountcode) id,Accountcode+AuxiliaryCode as FZNewMore into #FZNewMore from  {0}.dbo.AuxiliaryFDetail z where hashcode not in (select hashcode from #remoteFZ) ", localDbName);
                        sb.Append(" go ");
                        sb.AppendFormat(" select ROW_NUMBER() over(order by VoucherID) id,VoucherID as  FZOldMore into #FZOldMore from #remoteFZ  where hashcode not in (select hashcode from {0}.dbo.AuxiliaryFDetail z) ", localDbName);
                        sb.Append(" go ");
                        sb.AppendFormat(" insert into  {0}.dbo.[AppedXDataDiff] (PZNewMore,PZOldMore,FZNewMore,FZOldMore) select PZNewMore,PZOldMore,FZNewMore,FZOldMore from  #PZNewMore p1 full join #PZOldMore p2 on p1.id=p2.id full join   #FZNewMore f1 on p1.ID=f1.ID full join #FZOldMore f2 on f1.id=f2.id", localDbName);
                        sb.Append(" go ");
                    }
                    string[] sqlarr = sb.ToString().Split(new[] { " GO ", " go " }, StringSplitOptions.RemoveEmptyEntries);
                    var ret = dapper.ExecuteTransactionAndDBSigleUser(sqlarr);
                    if (ret.Item1 > 0)
                    {
                        dapper.Execute(string.Format(" update  xdata.dbo.XFiles set datastatus =999,projectid='{1}' where xid={0} ", xfile.XID, projectID), null);
                        response.HttpStatusCode = 200; 
                        _logger.LogInformation(string.Format(xfile.XID+ "  项目{0}已追加导入EAS", xfile.ProjectID, localDbName) + " " + DateTime.Now);
                        return response;
                    }
                    else
                    {
                        response.ResultContext += ret.Item2;
                    }

                }
                dapper.Execute(string.Format(" update  xdata.dbo.XFiles set datastatus =2,projectid='{1}' where xid={0} ", xfile.XID, projectID), null);
                response.HttpStatusCode = 500;
                response.ResultContext = string.Format("{0}项目导入EAS失败,因为：{1}", xfile.ProjectID, response.ResultContext);
                _logger.LogError(response.ResultContext + " " + DateTime.Now);

                return response;
            }
            catch (Exception et)
            {

                response.HttpStatusCode = 500;
                response.ResultContext = string.Format("文件{0} 项目{1}导入EAS失败,请联系系统管理员！", xfile.XID, xfile.ProjectID);
                _logger.LogError(response.ResultContext + " 异常：" + et.Message + " " + DateTime.Now);

                return response;
            }
            finally
            {
                StaticData.X2EasList[key] = "";
            }
        }


    }
}
