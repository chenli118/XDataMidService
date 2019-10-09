using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using System.Configuration;
using System.Data;
using Dapper;
using System.Threading;
using XDataMidService.Models;
using System.Net.Http;
using System.Net;
using Microsoft.Extensions.Configuration;
using NetDiskLibrary;

namespace XDataMidService.BPImp
{
    internal class PDT2SDT
    {
        private int _auditYear;
        private string conStr, _zzero1F, _wp_GUID, _clientid, _projectID, _content, _reasonPhrase, _tempFile;
        private DateTime _beginDate, _endDate;
        private HttpRequestMessage requestMessage;
        private HttpStatusCode _statusCode;
        public PDT2SDT(string zzero1F, string wp_GUID, string projectID, Models.xfile cInfo)
        {
            _zzero1F = zzero1F;
            _wp_GUID = wp_GUID;
            _projectID = projectID;
            _auditYear = int.Parse(cInfo.ZTYear.Trim());
            _beginDate = DateTime.Parse(_auditYear + "/" + cInfo.PZBeginDate.Trim());
            _endDate = DateTime.Parse(_auditYear + "/" + cInfo.PZEndDate.Trim());
            _clientid = cInfo.CustomID;

        }
        public bool DownLoadFile(xfile xfile)
        {
            string XdataAccount = StaticUtil.GetConfigValueByKey("XdataAccount");
            string wp_host = StaticUtil.GetConfigValueByKey("WP_HOST");
            if (!string.IsNullOrEmpty(XdataAccount))
            {
                string logname = XdataAccount.Split('#')[1];
                string logpwd = XdataAccount.Split('#')[0];
                var ndc = new  MinniDown(wp_host, logname, logpwd);

                FileInfo fileInfo = new FileInfo(xfile.FileName);
                var tempFolder = Path.Combine(Directory.GetCurrentDirectory(), "XJYData", _projectID);
                if (!Directory.Exists(tempFolder)) Directory.CreateDirectory(tempFolder);
                _tempFile = Path.Combine(tempFolder.Replace('/', '\\'), fileInfo.Name);
                if (File.Exists(_tempFile))
                {
                    File.SetAttributes(_tempFile, FileAttributes.Normal);
                    File.Delete(_tempFile);
                }
                string strRet = string.Empty;
                bool bRet = ndc.DownloadFile(_wp_GUID, _zzero1F, _tempFile, out strRet);
                if (bRet)
                {
                    return true;
                }

            }
            return false;
        }
        public Task<HttpResponseMessage> Start()
        {
            XDataReqResult result = null;
            requestMessage = new HttpRequestMessage();
            bool stepRet = false;           
            if (!File.Exists(_tempFile))
            {
                _statusCode = HttpStatusCode.FailedDependency;
                _content = "XData File No Found";
                _reasonPhrase = "PDT2SDT.Start";
                return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
            }
            var files = UnZipFile(_tempFile);
            stepRet = DBInit(files);
            if (!stepRet)
            {
                _statusCode = HttpStatusCode.FailedDependency;
                _content = "ERROR：基础数据加载失败";
                _reasonPhrase = "PDT2SDT.DBInit";
                return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
            }
            stepRet = InitProject();
            if (!stepRet)
            {
                _statusCode = HttpStatusCode.FailedDependency;
                _content = "ERROR：项目表数据加载失败";
                _reasonPhrase = "PDT2SDT.InitProject";
                return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
            }
            stepRet = InitAccount();
            if (!stepRet)
            {
                _statusCode = HttpStatusCode.FailedDependency;
                _content = "ERROR：科目表数据加载失败";
                _reasonPhrase = "PDT2SDT.InitAccount";
                return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
            }
            stepRet = InitVoucher();
            if (!stepRet)
            {
                _statusCode = HttpStatusCode.FailedDependency;
                _content = "ERROR：凭证表数据加载失败";
                _reasonPhrase = "PDT2SDT.InitVoucher";
                return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
            }
            bool isBaseAccount = GetIsExsitsItemClass();
            if (isBaseAccount)
            {
                stepRet = InitFdetail();
                if (!stepRet)
                {
                    _statusCode = HttpStatusCode.FailedDependency;
                    _content = "ERROR：AuxiliaryFDetail数据加载失败";
                    _reasonPhrase = "PDT2SDT.InitFdetail";
                    return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
                }
                Thread.Sleep(1000);
                stepRet = InitTBAux();
                if (!stepRet)
                {
                    _statusCode = HttpStatusCode.FailedDependency;
                    _content = "ERROR：TBAux数据加载失败";
                    _reasonPhrase = "PDT2SDT.InitTBAux";
                    return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
                }
            }
            stepRet = InitTBFS();
            stepRet = InitTbDetail();
            if (!stepRet)
            {
                _statusCode = HttpStatusCode.FailedDependency;
                _content = "ERROR：TBDetail数据加载失败";
                _reasonPhrase = "PDT2SDT.InitTbDetail";
                return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
            }
            stepRet = UpdateTBDetailAndTBAux();
            if (!stepRet)
            {
                _statusCode = HttpStatusCode.FailedDependency;
                _content = "ERROR：更新Tbdetail、TBAux失败";
                _reasonPhrase = "PDT2SDT.UpdateTBDetailAndTBAux";
                return new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage).ExecuteAsync();
            }
            _statusCode = HttpStatusCode.OK;
            _content = "SUCCESS : 导入数据成功";
            _reasonPhrase = "PDT2SDT.Start";
            result = new XDataReqResult(_content, _reasonPhrase, _statusCode, requestMessage);
            return result.ExecuteAsync();
        }
        private bool UpdateTBDetailAndTBAux()
        {
            try
            {
                var p = new DynamicParameters();
                p.Add("@pzEndDate", _endDate);
                SqlMapperUtil.InsertUpdateOrDeleteStoredProc("UpdateTBDetailTBAuxJE", p, conStr);
            }
            catch (Exception err)
            {
                return false;
            }
            return true;
        }
        private bool InitTbDetail()
        {
            try
            {
                #region old
                /*
                DataTable dtDetail = new DataTable();
                dtDetail.TableName = "TBDetail";
                #region columns
                dtDetail.Columns.Add("ID");
                dtDetail.Columns.Add("ProjectID");
                dtDetail.Columns.Add("FSCode");
                dtDetail.Columns.Add("AccountCode");
                dtDetail.Columns.Add("AuxiliaryCode");
                dtDetail.Columns.Add("AccAuxName");
                dtDetail.Columns.Add("DataType", typeof(int));
                dtDetail.Columns.Add("TBGrouping");
                dtDetail.Columns.Add("TBType", typeof(int));
                dtDetail.Columns.Add("IsAccMx", typeof(int));
                dtDetail.Columns.Add("IsMx", typeof(int));
                dtDetail.Columns.Add("IsAux", typeof(int));
                dtDetail.Columns.Add("kmsx");
                dtDetail.Columns.Add("Yefx", typeof(int));
                dtDetail.Columns.Add("SourceFSCode");
                dtDetail.Columns.Add("Sqqmye", typeof(decimal));
                dtDetail.Columns.Add("Qqccgz", typeof(decimal));
                dtDetail.Columns.Add("jfje", typeof(decimal));
                dtDetail.Columns.Add("dfje", typeof(decimal));
                dtDetail.Columns.Add("CrjeJF", typeof(decimal));
                dtDetail.Columns.Add("CrjeDF", typeof(decimal));
                dtDetail.Columns.Add("AjeJF", typeof(decimal));
                dtDetail.Columns.Add("AjeDF", typeof(decimal));
                dtDetail.Columns.Add("RjeJF", typeof(decimal));
                dtDetail.Columns.Add("RjeDF", typeof(decimal));
                dtDetail.Columns.Add("TaxBase", typeof(decimal));
                dtDetail.Columns.Add("PY1", typeof(decimal));
                dtDetail.Columns.Add("jfje1", typeof(decimal));
                dtDetail.Columns.Add("dfje1", typeof(decimal));
                dtDetail.Columns.Add("jfje2", typeof(decimal));
                dtDetail.Columns.Add("dfje2", typeof(decimal));
                #endregion
                string qsql = "select distinct NEWID() ID ,a.AccountCode,space(0) AS SourceFSCode," +
                    " a.AccountName as AccAuxName,a.jb as TBType,0 AS IsMx, a.UpperCode TBGrouping, a.Ncye AS Sqqmye,space(0) fscode,1 yefx,0 kmsx," +
                    "0 AS isAux,a.ismx AS isAccMx,0 AS DataType,Qqccgz,Hsxms,TypeCode from dbo.Account a with(nolock)   ";

                dynamic ds = SqlMapperUtil.SqlWithParams<dynamic>(qsql, null, conStr);
                foreach (var vd in ds)
                {
                    DataRow dr = dtDetail.NewRow();
                    dr["ID"] = vd.ID;
                    dr["ProjectID"]=dbName;
                    dr["FSCode"] = vd.fscode;
                    dr["AccountCode"] = vd.AccountCode;
                    dr["AuxiliaryCode"] = vd.TypeCode;
                    dr["AccAuxName"] = vd.AccAuxName;
                    dr["DataType"] = vd.DataType;
                    dr["TBGrouping"] = vd.TBGrouping;
                    dr["TBType"] = vd.TBType;
                    dr["IsAccMx"] = vd.isAccMx;
                    dr["IsMx"] = vd.IsMx;
                    dr["IsAux"] = 0;
                    dr["kmsx"] = vd.kmsx;
                    dr["Yefx"] = vd.yefx;
                    dr["SourceFSCode"] = vd.SourceFSCode;
                    dr["Sqqmye"] = vd.Sqqmye==null?0M:vd.Sqqmye;
                    dr["Qqccgz"] = vd.Qqccgz;
                    dr["jfje"] = 0M;
                    dr["dfje"] = 0M;
                    dr["CrjeJF"] = 0M;
                    dr["CrjeDF"] = 0M;
                    dr["AjeJF"] = 0M;
                    dr["AjeDF"] = 0M;
                    dr["RjeJF"] = 0M;
                    dr["RjeDF"] = 0M;
                    dr["TaxBase"] = 0M;
                    dr["PY1"] = 0M;
                    dr["jfje1"] = 0M;
                    dr["dfje1"] = 0M;
                    dr["jfje2"] = 0M;
                    dr["dfje2"] = 0M;
                    dtDetail.Rows.Add(dr);

                }
                string execSQL = " truncate table  " + dtDetail.TableName;
                SqlMapperUtil.CMDExcute(execSQL, null, conStr);
                SqlServerHelper.SqlBulkCopy(dtDetail, conStr); */
                #endregion
                var p = new DynamicParameters();
                p.Add("@ProjectID", _projectID);
                SqlMapperUtil.InsertUpdateOrDeleteStoredProc("InitTbAccTable", p, conStr);
            }
            catch (Exception err)
            {
                return false;
            }
            return true;
        }
        private bool InitTBFS()
        {
            try
            {
                string execSQL = "Insert TBFS  SELECT * FROM Pack_TBFS  where projectid='audCas' \n\r update TBFS set projectid='" + _projectID + "'";
                SqlMapperUtil.CMDExcute(execSQL, null, conStr);
            }
            catch (Exception err)
            {
                return false;
            }
            return true;
        }
        private bool InitTBAux()
        {
            try
            {
                DataTable auxTable = new DataTable();
                auxTable.TableName = "TBAux";
                auxTable.Columns.Add("ProjectID");
                auxTable.Columns.Add("AccountCode");
                auxTable.Columns.Add("AuxiliaryCode");
                auxTable.Columns.Add("AuxiliaryName");
                auxTable.Columns.Add("FSCode");
                auxTable.Columns.Add("kmsx");
                auxTable.Columns.Add("YEFX", typeof(int));
                auxTable.Columns.Add("TBGrouping");
                auxTable.Columns.Add("Sqqmye", typeof(decimal));
                auxTable.Columns.Add("Qqccgz", typeof(decimal));
                auxTable.Columns.Add("jfje", typeof(decimal));
                auxTable.Columns.Add("dfje", typeof(decimal));
                auxTable.Columns.Add("qmye", typeof(decimal));
                string qsql = "select distinct idet.accountcode,idet.AuxiliaryCode, isnull(xm.xmmc,space(0)) as AuxiliaryName,xmye.ncye as Sqqmye  from AuxiliaryFDetail idet with(nolock) join  xm xm   on LTRIM(rtrim(xm.xmdm)) COLLATE Chinese_PRC_CS_AS_KS_WS=idet.AuxiliaryCode COLLATE Chinese_PRC_CS_AS_KS_WS      join xmye xmye on idet.accountcode COLLATE Chinese_PRC_CS_AS_KS_WS = ltrim(rtrim(xmye.kmdm)) COLLATE Chinese_PRC_CS_AS_KS_WS and idet.AuxiliaryCode COLLATE Chinese_PRC_CS_AS_KS_WS = LTRIM(rtrim(xmye.xmdm)) COLLATE Chinese_PRC_CS_AS_KS_WS  ";
                dynamic ds = SqlMapperUtil.SqlWithParams<dynamic>(qsql, null, conStr);
                foreach (var vd in ds)
                {
                    DataRow dr = auxTable.NewRow();
                    dr["ProjectID"] = _projectID;
                    dr["AccountCode"] = vd.accountcode;
                    dr["AuxiliaryCode"] = vd.AuxiliaryCode;
                    dr["AuxiliaryName"] = vd.AuxiliaryName;
                    dr["FSCode"] = string.Empty;
                    dr["kmsx"] = 0;
                    dr["YEFX"] = 0;
                    dr["TBGrouping"] = vd.accountcode;
                    dr["Sqqmye"] = vd.Sqqmye == null ? 0M : vd.Sqqmye;
                    dr["Qqccgz"] = 0M;
                    dr["jfje"] = 0M;
                    dr["dfje"] = 0M;
                    dr["qmye"] = 0M;
                    auxTable.Rows.Add(dr);
                }
                if (auxTable.Rows.Count == 0)
                {
                    return false;
                }
                else
                {
                    string execSQL = " truncate table  " + auxTable.TableName;
                    SqlMapperUtil.CMDExcute(execSQL, null, conStr);
                    SqlServerHelper.SqlBulkCopy(auxTable, conStr).Wait();
                }
            }
            catch (Exception err)
            {
                return false;
            }
            return true;

        }
        private bool InitFdetail()
        {
            try
            {
                DataTable auxfdetail = new DataTable();
                auxfdetail.TableName = "AuxiliaryFDetail";
                auxfdetail.Columns.Add("projectid");
                auxfdetail.Columns.Add("Accountcode");
                auxfdetail.Columns.Add("AuxiliaryCode");
                auxfdetail.Columns.Add("Ncye", typeof(decimal));
                auxfdetail.Columns.Add("Jfje1", typeof(decimal));
                auxfdetail.Columns.Add("Dfje1", typeof(decimal));
                auxfdetail.Columns.Add("FDetailID", typeof(int));
                auxfdetail.Columns.Add("DataType", typeof(int));
                auxfdetail.Columns.Add("DataYear", typeof(int));

                string itemclass = "select * from t_itemclass";
                var tab_ic = SqlMapperUtil.SqlWithParams<dynamic>(itemclass, null, conStr);
                List<string> xmField = new List<string>();
                foreach (var iid in tab_ic)
                {
                    xmField.Add("F" + iid.FItemClassID);
                }
                string sql1 = "select  * from t_itemdetail  t join t_fzye f on t.FDetailID = f.FDetailID  ";
                var d1 = SqlMapperUtil.SqlWithParams<dynamic>(sql1, null, conStr);
                foreach (var d in d1)
                {
                    Array.ForEach(xmField.ToArray(), f =>
                    {

                        foreach (var xv in d)
                        {
                            if (xv.Key == f)
                            {
                                if (!string.IsNullOrWhiteSpace(xv.Value))
                                {
                                    DataRow dr1 = auxfdetail.NewRow();
                                    dr1["projectid"] = _projectID;
                                    dr1["Accountcode"] = d.Kmdm;
                                    dr1["AuxiliaryCode"] = xv.Value;
                                    dr1["Ncye"] = d.Ncye;
                                    dr1["Jfje1"] = d.Jfje1;
                                    dr1["Dfje1"] = d.Dfje1;
                                    dr1["FDetailID"] = d.FDetailID;
                                    dr1["DataType"] = 0;
                                    dr1["DataYear"] = _auditYear;
                                    auxfdetail.Rows.Add(dr1);
                                }
                            }
                        }

                    });
                }
                string execSQL = " truncate table  " + auxfdetail.TableName;
                SqlMapperUtil.CMDExcute(execSQL, null, conStr);
                SqlServerHelper.SqlBulkCopy(auxfdetail, conStr).Wait();
            }
            catch (Exception err)
            {
                return false;
            }
            return true;

        }
        private bool GetIsExsitsItemClass()
        {
            string sql = "select 1 as be from t_itemclass";
            object ret = DapperHelper<int>.Create("XDataConn",conStr).ExecuteScalar(sql, null);
            return ret != null;
        }
        private bool InitVoucher()
        {

            try
            {
                string jzpzSQL = " truncate table TBVoucher" +
                    " insert  TBVoucher(VoucherID,Clientid,ProjectID,IncNo,Date,Period,Pzh,Djh,AccountCode,Zy,Jfje,Dfje,jfsl,fsje,jd,dfsl, ZDR,dfkm,Wbdm,Wbje,Hl,fllx,FDetailID) ";
                jzpzSQL += "select  newid() as VoucherID,'" + _clientid + "' as clientID, '" + _projectID + "' as ProjectID,IncNo, Pz_Date as [date],Kjqj as Period ,Pzh,fjzs as Djh,Kmdm as AccountCode ," +
                   " zy,case when jd = '借' then rmb else 0 end as jfje,  " +
                   " case when jd = '贷' then rmb else 0 end as dfje,  " +
                   " case when jd = '借' then isnull(sl,0)  else 0 end as jfsl,  " +
                   " case when jd = '借' and rmb>0	then 1 else -1 end *(rmb) as fsje," +
                   " case when jd = '借' and rmb>0	then 1 else -1 end	as jd, " +
                   " case when jd = '贷' then isnull(sl,0)  else 0 end as dfsl,  sr as ZDR, DFKM,Wbdm,Wbje,isnull(Hl,0) as Hl,  1 as fllx, FDetailID from jzpz ";
                SqlMapperUtil.CMDExcute(jzpzSQL, null, conStr);

                string expzk = " select 	Pzk_TableName	from	pzk	where	Pzk_TableName!='jzpz' and Pzk_TableName like 'jzpz%' ";
                dynamic ds = SqlMapperUtil.SqlWithParams<dynamic>(expzk, null, conStr);
                string pzkname = "jzpz";
                foreach (var d in ds)
                {
                    jzpzSQL = jzpzSQL.Replace("from " + pzkname, "from " + d.Pzk_TableName).Replace("truncate table TBVoucher", "");
                    SqlMapperUtil.CMDExcute(jzpzSQL, null, conStr);
                    pzkname = d.Pzk_TableName;
                }
                string incNoSql = ";with t1 as( select ROW_NUMBER() OVER (ORDER BY period) AS RowNumber,	period,pzh from TBVoucher group by period,pzh	having  COUNT(period)>1 AND count(pzh)>1)" +
                   "   update vv set vv.IncNo = v.RowNumber  from TBVoucher vv  ,	t1 v    where vv.period = v.period and vv.pzh = v.pzh; ";
                SqlMapperUtil.CMDExcute(incNoSql, null, conStr);

                string updatesql = " update v set v.fllx = case when a.Syjz = 0 then 1 else a.Syjz end   from dbo.tbvoucher v     join ACCOUNT a on a.AccountCode = v.AccountCode  ";
                SqlMapperUtil.CMDExcute(updatesql, null, conStr);
                updatesql = "update z set z.HashCode =HASHBYTES('SHA1', (select z.* FOR XML RAW, BINARY BASE64)) from  TBVoucher  z";
                SqlMapperUtil.CMDExcute(updatesql, null, conStr);
            }
            catch (Exception err)
            {
                throw err;
            }
            return true;
        }
        private bool InitAccount()
        {
            try
            {
                string sql = "select 1 as be from km where Kmdm !=Kmdm_Jd";               
                object ret = DapperHelper<int>.Create("XDataConn",conStr).ExecuteScalar(sql, null);
                if (ret != null)
                {
                    return false;
                }
                DataTable accountTable = new DataTable();
                accountTable.TableName = "ACCOUNT";
                accountTable.Columns.Add("ProjectID");
                accountTable.Columns.Add("AccountCode");
                accountTable.Columns.Add("UpperCode");
                accountTable.Columns.Add("AccountName");
                //accountTable.Columns.Add("Attribute",typeof(int));
                accountTable.Columns.Add("Jd", typeof(int));
                accountTable.Columns.Add("Hsxms", typeof(int));
                accountTable.Columns.Add("TypeCode");
                accountTable.Columns.Add("Jb", typeof(int));
                accountTable.Columns.Add("IsMx", typeof(int));
                accountTable.Columns.Add("Ncye", typeof(decimal));
                accountTable.Columns.Add("Qqccgz", typeof(decimal));
                accountTable.Columns.Add("Jfje", typeof(decimal));
                accountTable.Columns.Add("Dfje", typeof(decimal));
                accountTable.Columns.Add("Ncsl", typeof(int));
                accountTable.Columns.Add("Syjz", typeof(int));
                //按级别排序
                string qsql = "SELECT km.kmdm,km.kmmc,Xmhs,Kmjb,IsMx,Ncye,Jfje1,Dfje1,Ncsl  FROM KM   left join kmye  on km.kmdm = kmye.kmdm  order by Kmjb";
                dynamic ds = SqlMapperUtil.SqlWithParams<dynamic>(qsql, null, conStr);

                foreach (var vd in ds)
                {
                    DataRow dr = accountTable.NewRow();
                    dr["ProjectID"] = _projectID;
                    dr["AccountCode"] = vd.kmdm;
                    dr["UpperCode"] = DBNull.Value;
                    dr["AccountName"] = vd.kmmc;
                    //dr["Attribute"] = vd.KM_TYPE == "损益" ? 1 : 0;
                    dr["Jd"] = 1;//default(1)
                    dr["Hsxms"] = 0;
                    dr["TypeCode"] = "";
                    dr["Jb"] = vd.Kmjb;
                    dr["IsMx"] = vd.IsMx == null ? 0 : 1;
                    dr["Ncye"] = vd.Ncye == null ? 0M : vd.Ncye;
                    dr["Qqccgz"] = 0M;
                    dr["Jfje"] = vd.Jfje1 == null ? 0M : vd.Jfje1;
                    dr["Dfje"] = vd.Dfje1 == null ? 0M : vd.Dfje1;
                    dr["Ncsl"] = vd.Ncsl == null ? 0M : vd.Ncsl;
                    dr["Syjz"] = 0;
                    accountTable.Rows.Add(dr);
                }
                BuildUpperCode(accountTable, conStr);
                BuildTypeCode(accountTable, conStr);
                string execSQL = " truncate table ACCOUNT ";
                SqlMapperUtil.CMDExcute(execSQL, null, conStr);
                SqlServerHelper.SqlBulkCopy(accountTable, conStr).Wait();
            }
            catch (Exception err)
            {
                return false;
            }
            return true;

        }
        private void BuildTypeCode(DataTable accountTable, string conStr)
        {
            string typeSql = "; with s1 as( SELECT DISTINCT _xmye.KMDM,_xmye.XMDM,icl.FITEMID as typecode FROM XMYE _xmye JOIN xm xm ON _xmye.Xmdm COLLATE Chinese_PRC_CS_AS_KS_WS = xm.Xmdm COLLATE Chinese_PRC_CS_AS_KS_WS  INNER JOIN t_itemclass icl   ON LEFT(xm.Xmdm, LEN(icl.FItemId))= icl.FItemId )   SELECT DISTINCT KMDM, typecode from s1 ;";
            dynamic ds = SqlMapperUtil.SqlWithParams<dynamic>(typeSql, null, conStr);

            Dictionary<string, List<string>> dicTypeCode = new Dictionary<string, List<string>>();

            foreach (var vd in ds)
            {
                if (!dicTypeCode.ContainsKey(vd.KMDM))
                {
                    List<string> list = new List<string>();
                    list.Add(vd.typecode);
                    dicTypeCode.Add(vd.KMDM, list);
                }
                else
                {
                    dicTypeCode[vd.KMDM].Add(vd.typecode);
                }
            }
            foreach (string k in dicTypeCode.Keys)
            {
                var row = accountTable.Rows.Cast<DataRow>().Where(x => x["AccountCode"].ToString() == k).SingleOrDefault();
                if (row != null)
                {
                    row["TypeCode"] = string.Join(";", dicTypeCode[k].ToArray());
                    row["Hsxms"] = dicTypeCode[k].Count;
                }
            }
        }
        private void BuildUpperCode(DataTable accountTable, string conStr)
        {

            string syjzSql = " select * from   Accountclass ac with(nolock) ";
            dynamic syjzdt = SqlMapperUtil.SqlWithParams<dynamic>(syjzSql, null, conStr);

            foreach (DataRow dr in accountTable.Rows)
            {
                int jb = -1;
                int.TryParse(dr["Jb"].ToString(), out jb);
                if (jb < 1) continue;
                if (jb == 1)
                {
                    foreach (var s in syjzdt)
                    {
                        if (dr["AccountName"].ToString().StartsWith(s.Accountname))
                        {
                            dr["Syjz"] = s.syjz;
                        }
                    }

                }
                else
                {
                    var uprow = accountTable.Rows.Cast<DataRow>().Where(x => x["Jb"].ToString() == (jb - 1).ToString()
                    && dr["AccountCode"].ToString().StartsWith(x["AccountCode"].ToString())).SingleOrDefault();
                    dr["UpperCode"] = uprow["AccountCode"];
                    dr["Syjz"] = uprow["Syjz"];
                }

            }
        }
        private bool InitProject()
        {
            try
            {
                string projectsql = " truncate table PROJECT  ; INSERT  PROJECT   SELECT Distinct '" + _projectID + "', LEFT(XMDM, CHARINDEX('.', XMDM)),XMDM,isnull(XMMC,space(0)),NULL,XMJB,XMMX     FROM XM " +
                    " ; update PROJECT set ProjectCode=LTRIM(rtrim(ProjectCode)),TypeCode=LTRIM(rtrim(TypeCode))  ";
                SqlMapperUtil.CMDExcute(projectsql, null, conStr);
                string jbsql = "update  p1 set  p1.UPPERCODE = p2.PROJECTCODE  from ProJect p1 join ProJect p2 on p1.JB =p2.JB+1  and p1.TYPECODE = p2.TYPECODE   and  left(p1.PROJECTCODE,len(p2.PROJECTCODE)) = p2.PROJECTCODE and p1.jb>1 ";
                SqlMapperUtil.CMDExcute(jbsql, null, conStr);

                string projecttypesql = " truncate table ProjectType  ; INSERT  ProjectType  SELECT   '" + _projectID + "', FITEMID,FName FROM t_itemclass" +
                    " ; update  PROJECTTYPE set TypeCode=LTRIM(rtrim(TypeCode))   ";
                SqlMapperUtil.CMDExcute(projecttypesql, null, conStr);
            }
            catch (Exception err)
            {
                return false;
            }
            return true;
        }
        private void InitDataBase(string dbName)
        {
            conStr = StaticUtil.GetConfigValueByKey("XDataConn");
            SqlMapperUtil.GetOpenConnection(conStr);
            string exsitsDB = "select count(1) from sys.sysdatabases where name =@dbName";
            int result = SqlMapperUtil.SqlWithParamsSingle<int>(exsitsDB, new { dbName = dbName });

            if (result == 0)
            {
                string s1 = " create database [" + dbName + "]";
                int ret = SqlMapperUtil.InsertUpdateOrDeleteSql(s1, null);
            }
            conStr = conStr.Replace("master", dbName);
            var StaticStructAndFn = Path.Combine(Directory.GetCurrentDirectory(), "StaticStructAndFn.tsql");
            var sqls = File.ReadAllText(StaticStructAndFn);
            SqlServerHelper.ExecuteSql(sqls, conStr);
            string kjqjInsert = "delete dbo.kjqj where Projectid='{0}'  " +
                " insert  dbo.kjqj(ProjectID,CustomerCode,CustomerName,BeginDate,EndDate,KJDate)" +
                "  select '{0}','{1}','{1}','{2}','{3}','{4}'";
            SqlServerHelper.ExecuteSql(string.Format(kjqjInsert, dbName, _projectID, _beginDate, _endDate, _auditYear), conStr);

        }
        private bool DBInit(string[] pfiles)
        {
            try
            {
                var accountinfofile = pfiles.Where(x => x.ToLower().EndsWith("ztsjbf.ini")).FirstOrDefault();
                if (!File.Exists(accountinfofile)) { return false; }
                string[] files = pfiles.Where(s => s != null && (s.EndsWith(".db")
                                || s.EndsWith(".ini"))).ToArray();

                List<string> dbFilter = new List<string> { "km", "kmye", "xm", "xmye", "bm", "bmye", "wl", "wlye", "t_fzye", "t_itemclass", "t_itemdetail", "jzpz", "pzk" };
                //过滤需要导入的db文件
                #region 001ToDb
                var dbFiles = files.Where(p => dbFilter.Exists(s =>
                    s == Path.GetFileNameWithoutExtension(p).ToLower()
                    || (Path.GetFileNameWithoutExtension(p).ToLower() != "jzpz" &&
                        Path.GetFileNameWithoutExtension(p).ToLower().IndexOf("jzpz") > -1)));
                if (dbFiles.Count() == 0) return false;
                InitDataBase(_projectID);
                Array.ForEach(dbFiles.ToArray(), (string dbfile) =>
               {
                   PD2SqlDB(dbfile, _projectID);

                    //else if (importType != 0 &&
                    //         tables.Count(x => dbfile.ToLower().IndexOf(x) > -1) > 0)
                    //{
                    //    importTxtTable(dbname, dbfile);
                    //}
                });

                #endregion
            }
            catch (Exception err)
            {
                throw err;
            }
            return true;

        }
        private bool PD2SqlDB(string filepath, String dbName)
        {
            bool bRet = false;
            string filename = Path.GetFileNameWithoutExtension(filepath);
            try
            {
                var _ParadoxTable = new ParadoxReader.ParadoxTable(Path.GetDirectoryName(filepath), filename);
                var columns = _ParadoxTable.FieldNames;
                var fieldtypes = _ParadoxTable.FieldTypes;
                DataTable dt = new DataTable();
                dt.TableName = Path.GetFileNameWithoutExtension(filepath);//_ParadoxTable.TableName;
                if (columns.Length == 0 || _ParadoxTable.RecordCount == 0)
                    return bRet;

                string tableName = dt.TableName;
                string typeName = "[dbo].[" + dt.TableName + "Type]";
                string procName = "usp_insert" + dt.TableName;

                StringBuilder strSpt = new StringBuilder(string.Format("IF object_id('{0}') IS NOT NULL  drop table  {0}", tableName));
                strSpt.AppendLine(" create    table   " + tableName + "(" + Environment.NewLine);

                StringBuilder strTypetv = new StringBuilder(string.Format("IF type_id('{0}') IS NOT NULL  drop TYPE  " + typeName, typeName));
                strTypetv.AppendLine(" create    TYPE  " + typeName + " as TABLE(" + Environment.NewLine);

                string preProc = " IF EXISTS (SELECT * FROM dbo.sysobjects WHERE type = 'P' AND name = '" + procName + "')   " +
                    " BEGIN       DROP  Procedure " + procName + "   END  ";
                string createProc = " CREATE PROCEDURE " + procName + "    (@tvpNewValues " + typeName + " READONLY)" +
                    "as  insert into " + tableName + "   select *   from  @tvpNewValues  ";

                for (int i = 0; i < columns.Length; i++)
                {
                    string fieldName = columns[i];
                    DataColumn dc = new DataColumn(fieldName);
                    ParadoxReader.ParadoxFieldTypes fieldType = fieldtypes[i].fType;
                    switch (fieldType)
                    {
                        case ParadoxReader.ParadoxFieldTypes.BCD:
                        case ParadoxReader.ParadoxFieldTypes.Number:
                        case ParadoxReader.ParadoxFieldTypes.Currency:
                        case ParadoxReader.ParadoxFieldTypes.Logical:
                        case ParadoxReader.ParadoxFieldTypes.Short:
                            strSpt.AppendLine(fieldName + " " + "decimal(19,3) null DEFAULT 0,");
                            strTypetv.AppendLine(fieldName + " " + "decimal(19,3) null DEFAULT 0,");
                            dc.DataType = typeof(System.Decimal);
                            break;
                        default:
                            strSpt.AppendLine(fieldName + " " + "nvarchar(1000)  collate Chinese_PRC_CS_AS_KS_WS null,");
                            strTypetv.AppendLine(fieldName + " " + "nvarchar(1000)  collate Chinese_PRC_CS_AS_KS_WS null,");
                            dc.DataType = typeof(System.String);
                            break;
                    }
                    dt.Columns.Add(dc);
                }
                string dtstring = strSpt.ToString().Substring(0, strSpt.Length - 3) + ")   " + strTypetv.ToString().Substring(0, strTypetv.Length - 3) + ")";
                string createDTSql = preProc + dtstring + " GO " + createProc;
                if (!string.IsNullOrEmpty(createDTSql))
                {
                    SqlServerHelper.ExecuteSql(createDTSql, conStr);
                }

                int idx = 0;
                foreach (var rec in _ParadoxTable.Enumerate())
                {
                    if (idx % 1000 == 0)
                    {
                        SqlServerHelper.ExecuteProcWithStruct(procName, conStr, typeName, dt);
                        dt.Rows.Clear();
                    }
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < _ParadoxTable.FieldCount; i++)
                    {
                        object OV = rec.DataValues[i];
                        if (!DBNull.Value.Equals(OV) && OV != null)
                            dr[_ParadoxTable.FieldNames[i]] = OV;
                    }
                    dt.Rows.Add(dr);
                    idx++;
                }

                _ParadoxTable.Dispose();
                _ParadoxTable = null;
                SqlServerHelper.ExecuteProcWithStruct(procName, conStr, typeName, dt);
                dt.Dispose();
                dt = null;
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("转换数据失败", filename), ex);

            }
            return true;
        }

        private string[] UnZipFile(string zzero1F)
        {
            var tmpFolder = zzero1F.Remove(zzero1F.LastIndexOf('.'));
            if (Directory.Exists(tmpFolder))
            {
                tmpFolder = tmpFolder + DateTime.Now.Ticks;
            } 
            Directory.CreateDirectory(tmpFolder);
            try
            {
                using (var stream = new FileStream(zzero1F, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    UnZipByCom.UnZipFile(stream, tmpFolder);
                    //获取所有文件添加到
                    var files = Directory.GetFiles(tmpFolder, "*.*",
                        SearchOption.AllDirectories).Where(s => s != null && (s.EndsWith(".db")
                            || s.EndsWith(".ini"))).ToArray();
                    return files;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("解压001文件错误:" + ex.Message, ex);
            }
        }
    }
}
