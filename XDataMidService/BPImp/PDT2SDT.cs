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
using System.Security.Cryptography;
using Microsoft.AspNetCore.Connections;
using System.Data.SqlClient;
using System.Diagnostics.Eventing.Reader;

namespace XDataMidService.BPImp
{
    internal class PDT2SDT
    {

        private int _auditYear;
        private string conStr, localDbName, _tempFile;
        private DateTime _beginDate, _endDate;
        private xfile xfile;
        private bool IsAux = true;
        XDataResponse response;
        public Exception _xdException;
        public PDT2SDT(Models.xfile xf)
        {
            xfile = xf;
            _xdException = new Exception(xf.ZTName);
            response = new XDataResponse();
            response.ResultContext = xf.ZTName;
            localDbName = StaticUtil.GetLocalDbNameByXFile(xf);
            _tempFile = Path.Combine(Directory.GetCurrentDirectory(), "XJYData", localDbName, xf.FileName);
        }
        public bool DownLoadFile(xfile xf, out string strRet)
        {
            if (System.IO.File.Exists(_tempFile) && new FileInfo(_tempFile).Length > 100)
            {
                strRet = _tempFile;
                return true;
            }
            string strReult = string.Empty;
            string XdataAccount = StaticUtil.GetConfigValueByKey("XdataWPAccount");
            string wp_host = StaticUtil.GetConfigValueByKey("WP_HOST");
            if (!string.IsNullOrEmpty(XdataAccount))
            {
                string logname = XdataAccount.Split('#')[1];
                string logpwd = XdataAccount.Split('#')[0];
                var ndc = new MinniDown(wp_host, logname, logpwd);
                FileInfo fileInfo = new FileInfo(_tempFile);
                string struPath = xf.CustomID + "/" + xf.FileName;
                try
                {
                    if (!fileInfo.Directory.Exists) fileInfo.Directory.Create();
                    if (fileInfo.Exists)
                    {
                        File.SetAttributes(_tempFile, FileAttributes.Normal);
                        File.Delete(_tempFile);
                    }
                    bool bRet = ndc.DownloadFile(xf.WP_GUID, struPath, _tempFile, out strReult);
                    if (bRet)
                    {
                        strRet = strReult;
                        return true;
                    }
                }
                catch (Exception err)
                {
                    if (DownLoadFile(xf, out strRet))
                        return true;
                    strReult = err.Message;
                }
            }
            strRet = strReult;
            return false;
        }
        public XDataResponse Start()
        {
            bool stepRet = false;
            response.HttpStatusCode = 500;
            try
            {
                _auditYear = int.Parse(xfile.ZTYear.Trim());
                _beginDate = DateTime.Parse(_auditYear + "/" + xfile.PZBeginDate.Trim());
                _endDate = DateTime.Parse(_auditYear + "/" + xfile.PZEndDate.Trim());
            }
            catch
            {
                response.ResultContext += " 账套期间时间格式异常！ ";
                return response;
            }
            localDbName = StaticUtil.GetLocalDbNameByXFile(xfile);
            xfile.ProjectID = localDbName;
            conStr = StaticUtil.GetConfigValueByKey("XDataConn");
            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(conStr);
            csb.InitialCatalog = localDbName;
            conStr = csb.ConnectionString;
            if (!File.Exists(_tempFile))
            {
                response.ResultContext += "XData File No Found";
                return response;
            }
            lock (ofile)
            {
                var files = UnZipFile(_tempFile);
                if (files == null || files.Length == 0)
                    stepRet = false;
                stepRet = DBInit(files);
            }
            if (!stepRet)
            {
                response.ResultContext += "ERROR：基础数据加载失败";
                return response;
            }
            stepRet = InitProject();
            if (!stepRet)
            {
                response.ResultContext += "ERROR：项目表数据加载失败";
                return response;
            }
            stepRet = InitAccount();
            if (!stepRet)
            {
                response.ResultContext += "ERROR：科目表数据加载失败";
                return response;
            }
            stepRet = InitVoucher();
            if (!stepRet)
            {
                response.ResultContext += "ERROR：凭证表数据加载失败";
                return response;
            }
            bool isNotBaseAccount = GetIsExsitsItemClass();
            if (isNotBaseAccount && IsAux)
            {
                stepRet = InitFdetail();
                if (!stepRet)
                {
                    response.ResultContext += "ERROR：AuxiliaryFDetail数据加载失败";
                    return response;
                }
                Thread.Sleep(1000);
                stepRet = InitTBAux();
                if (!stepRet)
                {
                    response.ResultContext += "ERROR：TBAux数据加载失败";
                    return response;
                }
            }
            stepRet = InitTBFS();
            stepRet = InitTbDetail();
            if (!stepRet)
            {
                response.ResultContext = "ERROR：TBDetail数据加载失败";
                return response;
            }
            stepRet = UpdateTBDetailAndTBAux();
            if (!stepRet)
            {
                response.ResultContext = "ERROR：更新Tbdetail、TBAux失败";
                return response;
            }
            response.HttpStatusCode = 200;
            response.ResultContext = " 已生成中间数据！";
            return response;
        }

        private bool UpdateTBDetailAndTBAux()
        {
            try
            {
                string sql = " ;with s1 as(select ROW_NUMBER() OVER(ORDER BY accountcode) AS ID, accountcode, fscode, kmsx, yefx   from dbo.TbDetail with(nolock) where auxiliarycode = '' and datatype = 0 and isaux = 0)" +
                    " MERGE DBO.TBAux AS AUX USING s1 AS d ON AUX.ACCOUNTCODE COLLATE Chinese_PRC_CS_AS_KS_WS = d.ACCOUNTCODE  COLLATE Chinese_PRC_CS_AS_KS_WS " +
                    " WHEN MATCHED THEN UPDATE SET aux.fscode = d.fscode,aux.KMSX = d.kmsx,aux.yefx = d.yefx; ";
                SqlMapperUtil.CMDExcute(sql, null, conStr);
                return true;
            }
            catch
            { return false; }
        }

        private bool UpdateTBDetailTBAuxMny()
        {
            var a1sql = " ;with a1 as (  select  accountcode from account with(nolock)   where accountname = '以前年度损益调整'  UNION ALL select a.projectid,a.accountcode from account a with(nolock)" +
                "inner join a1 on  a.uppercode = a1.accountcode and a.accountcode != a.accountcode  )  SELECT * FROM a1   ";
            DataTable a1 = SqlServerHelper.GetTableBySql(a1sql, conStr);
            var accountsql = "select * from dbo.Account with(nolock) ";
            DataTable account = SqlServerHelper.GetTableBySql(accountsql, conStr);
            var vouchersql = "select v.accountcode,v.FDetailID,v.fllx,v.jfje,v.dfje,a.syjz from dbo.tbvoucher v  with(nolock)  inner join Account  a   on v.accountcode = a.accountcode   where date <=@pzEndDate ";
            DataTable voucher = SqlServerHelper.GetTableBySql(vouchersql, conStr);

            string[] astr = new string[a1.Rows.Count];
            Array.ForEach(a1.Rows.Cast<DataRow>().ToArray(), (dr) =>
             astr.Append(dr.ItemArray[0])
            );

            var result = from r in voucher.AsEnumerable()
                         group r by new { AccountCode = r["AccountCode"] }
              into g
                         select new
                         {
                             jfje = g.Sum(x => Convert.ToDecimal(x["jfje"])),
                             dfje = g.Sum(x => Convert.ToDecimal(x["dfje"])),
                         };



            return false;
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
                p.Add("@ProjectID", xfile.ProjectID);
                SqlMapperUtil.InsertUpdateOrDeleteStoredProc("InitTbAccTable", p, conStr);
            }
            catch (Exception err)
            {
                _xdException = err;
                return false;
            }
            return true;
        }
        private bool InitTBFS()
        {
            try
            {
                string execSQL = "Insert TBFS  SELECT * FROM Pack_TBFS  where projectid='audCas' \n\r update TBFS set projectid='" + xfile.ProjectID + "'";
                SqlMapperUtil.CMDExcute(execSQL, null, conStr);
            }
            catch (Exception err)
            {
                _xdException = err;
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
                auxTable.Columns.Add("XID", typeof(Int32));
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
                    dr["XID"] = 0;
                    dr["ProjectID"] = xfile.ProjectID;
                    dr["AccountCode"] = vd.accountcode.Trim();
                    dr["AuxiliaryCode"] = vd.AuxiliaryCode.Trim();
                    dr["AuxiliaryName"] = vd.AuxiliaryName;
                    dr["FSCode"] = string.Empty;
                    dr["kmsx"] = 0;
                    dr["YEFX"] = 0;
                    dr["TBGrouping"] = vd.accountcode.Trim();
                    dr["Sqqmye"] = vd.Sqqmye == null ? 0M : vd.Sqqmye;
                    dr["Qqccgz"] = 0M;
                    dr["jfje"] = 0M;
                    dr["dfje"] = 0M;
                    dr["qmye"] = 0M;
                    auxTable.Rows.Add(dr);
                }
                if (auxTable.Rows.Count == 0)
                {
                    response.ResultContext += " auxtable is null";
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
                _xdException = err;
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
                auxfdetail.Columns.Add("XID", typeof(Int32));
                auxfdetail.Columns.Add("projectid");
                auxfdetail.Columns.Add("Accountcode");
                auxfdetail.Columns.Add("AuxiliaryCode");
                auxfdetail.Columns.Add("Ncye", typeof(decimal));
                auxfdetail.Columns.Add("Jfje1", typeof(decimal));
                auxfdetail.Columns.Add("Dfje1", typeof(decimal));
                auxfdetail.Columns.Add("FDetailID", typeof(int));
                auxfdetail.Columns.Add("DataType", typeof(int));
                auxfdetail.Columns.Add("DataYear", typeof(int));
                auxfdetail.Columns.Add("HashCode", typeof(byte[]));
                string itemclass = "select * from t_itemclass";
                var tab_ic = SqlMapperUtil.SqlWithParams<dynamic>(itemclass, null, conStr);
                List<string> xmField = new List<string>();
                foreach (var iid in tab_ic)
                {
                    xmField.Add("F" + iid.FItemClassID);
                }
                string sql1 = "select  * from t_itemdetail  t join t_fzye f on t.FDetailID = f.FDetailID  ";
                var d1 = SqlMapperUtil.SqlWithParams<dynamic>(sql1, null, conStr);
                using (SHA1Managed sha1 = new SHA1Managed())
                {
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
                                        dr1["XID"] = 0;
                                        dr1["projectid"] = xfile.ProjectID;
                                        string dm = d.Kmdm;
                                        dr1["Accountcode"] = dm.Trim();
                                        string acode = xv.Value;
                                        dr1["AuxiliaryCode"] = acode.Trim();
                                        object ncye = d.Ncye;
                                        if (!DBNull.Value.Equals(ncye) && ncye != null)
                                            dr1["Ncye"] = d.Ncye;
                                        else
                                            dr1["Ncye"] = decimal.Zero;
                                        object j1 = d.Jfje1;
                                        if (!DBNull.Value.Equals(j1) && j1 != null)
                                            dr1["Jfje1"] = d.Jfje1;
                                        else
                                            dr1["Jfje1"] = decimal.Zero;
                                        object df1 = d.Dfje1;
                                        if (!DBNull.Value.Equals(df1) && df1 != null)
                                            dr1["Dfje1"] = d.Dfje1;
                                        else
                                            dr1["Dfje1"] = decimal.Zero;
                                        dr1["FDetailID"] = d.FDetailID;
                                        dr1["DataType"] = 0;
                                        dr1["DataYear"] = _auditYear;
                                    //var hvalue = string.Join("", dr1.ItemArray.ToArray() + "");
                                    // dr1["HashCode"] = sha1.ComputeHash(Encoding.Unicode.GetBytes(hvalue)).Select(b => b.ToString("x2"));
                                    dr1["HashCode"] = DBNull.Value;
                                        auxfdetail.Rows.Add(dr1);
                                    }
                                }
                            }

                        });
                    }
                }
                string execSQL = " truncate table  " + auxfdetail.TableName;
                SqlMapperUtil.CMDExcute(execSQL, null, conStr);
                SqlServerHelper.SqlBulkCopy(auxfdetail, conStr).Wait();
                execSQL = " update z set z.HashCode = HASHBYTES('SHA1', (select z.Accountcode,z.AuxiliaryCode,z.FDetailID,z.DataYear FOR XML RAW, BINARY BASE64)) from AuxiliaryFDetail z";
                SqlMapperUtil.CMDExcute(execSQL, null, conStr);
            }
            catch (Exception err)
            {
                _xdException = err;
                return false;
            }
            return true;

        }
        private bool GetIsExsitsItemClass()
        {

            if (IsAux)
            {
                string sql = "select 1  from sysobjects  where id = object_id('t_itemclass')    and type = 'U'";
                int pzqj = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                if (pzqj == 1) return true;
            }
            return false;
        }
        private bool InitVoucher()
        {

            try
            {
                string sql = "   select 1 from sys.objects where name = 'jzpz' ";
                int ispz = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                if (ispz != 1)
                {
                    sql = "    SELECT  abs(max(ncye))+abs(min(ncye)) FROM [dbo].[kmye]   ";
                    if (SqlMapperUtil.SqlWithParamsSingle<decimal>(sql, null, conStr) > 0)
                    {
                        return true;
                    }
                    return false;
                }
                sql = " truncate table TBVoucher ; ";
                SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                sql = "select 1 from sys.columns  where object_id in(select object_id from sys.objects where name = 'jzpz') and name = 'kjqj'";
                int pzqj = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                string fdid = ", FDetailID";
                string jzpzSQL = "  insert  TBVoucher(VoucherID,Clientid,ProjectID,IncNo,Date,Period,Pzh,Djh,AccountCode,Zy,Jfje,Dfje,jfsl,fsje,jd,dfsl, ZDR,dfkm,Wbdm,Wbje,Hl,fllx" + fdid + ") ";
                if (pzqj == 1)
                {
                    jzpzSQL += "select  newid() as VoucherID,'" + xfile.ProjectID + "' as clientID, '" + xfile.ProjectID + "' as ProjectID,IncNo, Pz_Date as [date], DATENAME(year,pz_date)+DATENAME(month,pz_date)   as Period ,Pzh,isnull(fjzs,space(0)) as Djh," +
                        "ltrim(rtrim(Kmdm)) as AccountCode ," +
                       " zy,case when jd = '借' then rmb else 0 end as jfje,  " +
                       " case when jd = '贷' then rmb else 0 end as dfje,  " +
                       " case when jd = '借' then isnull(sl,0)  else 0 end as jfsl,  " +
                       " case when jd = '借' and rmb>0	then 1 else -1 end *(rmb) as fsje," +
                       " case when jd = '借' and rmb>0	then 1 else -1 end	as jd, " +
                       " case when jd = '贷' then isnull(sl,0)  else 0 end as dfsl,  sr as ZDR, DFKM,Wbdm,Wbje,isnull(Hl,0) as Hl,  1 as fllx" + fdid + " from jzpz ";
                }
                else
                {
                    jzpzSQL += "select  newid() as VoucherID,'" + xfile.ProjectID + "' as clientID, '" + xfile.ProjectID + "' as ProjectID,IncNo, CONVERT(date, '" + xfile.ZTYear + "'+ '/'+ ltrim(rtrim(SUBSTRING(pzrq,3,2))) + '/'+ ltrim(rtrim(SUBSTRING(pzrq,5,2)))) as [date]," +
                        "  '" + xfile.ZTYear + "'+   DATENAME(month, CONVERT(date, '" + xfile.ZTYear + "'+ '/'+ ltrim(rtrim(SUBSTRING(pzrq,3,2))) + '/'+ ltrim(rtrim(SUBSTRING(pzrq,5,2)))))  as Period ,Pzh,isnull(fjzs,space(0)) as Djh," +
                        "ltrim(rtrim(Kmdm)) as AccountCode ," +
                       " zy,case when jd = '借' then rmb else 0 end as jfje,  " +
                       " case when jd = '贷' then rmb else 0 end as dfje,  " +
                       " case when jd = '借' then isnull(sl,0)  else 0 end as jfsl,  " +
                       " case when jd = '借' and rmb>0	then 1 else -1 end *(rmb) as fsje," +
                       " case when jd = '借' and rmb>0	then 1 else -1 end	as jd, " +
                       " case when jd = '贷' then isnull(sl,0)  else 0 end as dfsl,  sr as ZDR, DFKM,Wbdm,Wbje,isnull(Hl,0) as Hl,  1 as fllx" + fdid + " from jzpz ";
                }
                sql = "select 1 from sys.columns  where object_id in(select object_id from sys.objects where name = 'jzpz') and name = 'FDetailID'";
                int fid = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                if (fid != 1)
                {
                    jzpzSQL = jzpzSQL.Replace(fdid, "");
                }
                sql = " select IncNo from jzpz with(nolock) ";
                DataTable dtIncNo = SqlServerHelper.GetTableBySql(sql, conStr);
                int startidx = 0;
                int stepidx = 2000;
                int rowcount = dtIncNo.Rows.Count;
                string tmpWhere = " ";
                if (rowcount > 20000)
                {
                    while (stepidx + startidx < rowcount)
                    {
                        tmpWhere = " where incno <= " + (stepidx + startidx) + " and incno>" + startidx;
                        jzpzSQL += tmpWhere;
                        SqlMapperUtil.CMDExcute(jzpzSQL, null, conStr);
                        jzpzSQL = jzpzSQL.Replace(tmpWhere, " ");
                        startidx = startidx + stepidx;

                    }
                    tmpWhere = " where incno>" + startidx; ;
                    jzpzSQL += tmpWhere;
                    SqlMapperUtil.CMDExcute(jzpzSQL, null, conStr);
                }
                else
                {
                    SqlMapperUtil.CMDExcute(jzpzSQL, null, conStr);
                }
                jzpzSQL = jzpzSQL.Replace(tmpWhere, " ");
                string expzk = " select 	Pzk_TableName	from	pzk	where	Pzk_TableName!='Jzpz' and Pzk_TableName like 'Jzpz%' ";
                dynamic ds = SqlMapperUtil.SqlWithParams<dynamic>(expzk, null, conStr);
                string pzkname = "jzpz";
                foreach (var d in ds)
                {
                    jzpzSQL = jzpzSQL.Replace("from " + pzkname, "from " + d.Pzk_TableName).Replace("truncate table TBVoucher", "");
                    sql = " select IncNo from " + d.Pzk_TableName + " with(nolock) ";
                    dtIncNo = SqlServerHelper.GetTableBySql(sql, conStr);
                    startidx = 0;
                    stepidx = 2000;
                    rowcount = dtIncNo.Rows.Count;
                    tmpWhere = "";
                    if (rowcount > 20000)
                    {
                        while (stepidx + startidx < rowcount)
                        {
                            tmpWhere = " where incno <= " + (stepidx + startidx) + " and incno>" + startidx;
                            jzpzSQL += tmpWhere;
                            SqlMapperUtil.CMDExcute(jzpzSQL, null, conStr);
                            jzpzSQL = jzpzSQL.Replace(tmpWhere, " ");
                            startidx = startidx + stepidx;

                        }
                        tmpWhere = " where incno>" + startidx;
                        jzpzSQL += tmpWhere;
                        SqlMapperUtil.CMDExcute(jzpzSQL, null, conStr);
                        jzpzSQL = jzpzSQL.Replace(tmpWhere, " ");
                    }
                    else
                    {
                        SqlMapperUtil.CMDExcute(jzpzSQL, null, conStr);
                    }
                    pzkname = d.Pzk_TableName;
                }
                string updatesql = "update z set z.HashCode =HASHBYTES('SHA1', (select z.ProjectID,z.Date,z.Pzh,z.Djh,z.AccountCode,z.Zy,z.Jfje,z.Dfje,z.jfsl,z.fsje,z.jd,z.dfsl,z.ZDR,z.dfkm,z.Wbdm,z.Wbje,z.Hl,z.FDetailID FOR XML RAW, BINARY BASE64)) from  TBVoucher  z";
                SqlMapperUtil.CMDExcute(updatesql, null, conStr);

                string incNoSql = " ;with t1 as( select ROW_NUMBER() OVER (ORDER BY pzh) AS IncNO,CONVERT(varchar,date,102) as period,pzh from TBVoucher group by CONVERT(varchar,date,102) ,pzh)  " +
                   "  update vv set vv.IncNo = t1.IncNO  from TBVoucher vv join t1  on CONVERT(varchar, vv.date, 102) = t1.period and vv.pzh = t1.pzh";
                SqlMapperUtil.CMDExcute(incNoSql, null, conStr);

                incNoSql = " with a1 as( select distinct t.incno,a.syjz from dbo.tbvoucher t	with(nolock)	join dbo.Account a	with(nolock)	on t.AccountCode=a.AccountCode	),a2 as (select incno,max(syjz) maxsyjz,min(syjz) minSyjz" +
                    " from a1  group by incno) " +
                    " 	select ROW_NUMBER() OVER(ORDER BY NEWID()) AS ID, * into #a2 from a2 ;	update v set v.fllx=case when a.maxsyjz=3 then 3 when a.maxsyjz=2 and a.minSyjz=1 then 2 else	1	end" +
                    " from dbo.tbvoucher v inner join #a2 a on v.incno=a.incno ;drop table #a2  ";
                SqlMapperUtil.CMDExcute(incNoSql, null, conStr);

            }
            catch (Exception err)
            {
                _xdException = err;
                return false;
            }
            return true;
        }
        private bool InitAccount()
        {
            try
            {
                //string sql = "select 1 as be from km where left(Kmdm,len(kmdm_jd)) !=Kmdm_Jd";
                //object ret = DapperHelper<int>.Create("XDataConn", conStr).ExecuteScalar(sql, null);
                //if (ret != null)
                //{
                //    return false;
                //}
                DataTable accountTable = new DataTable();
                accountTable.TableName = "ACCOUNT";
                accountTable.Columns.Add("XID", typeof(Int32));
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
                string qsql = " SELECT km.kmdm,km.kmmc,Xmhs,Kmjb,IsMx,Ncye,Jfje1,Dfje1,Ncsl  FROM KM   left join kmye  on km.kmdm  COLLATE Chinese_PRC_CS_AS_KS_WS= kmye.kmdm COLLATE Chinese_PRC_CS_AS_KS_WS   order by Kmjb  ";

                dynamic ds = SqlMapperUtil.SqlWithParams<dynamic>(qsql, null, conStr);

                foreach (var vd in ds)
                {
                    DataRow dr = accountTable.NewRow();
                    dr["XID"] = 0;
                    dr["ProjectID"] = xfile.ProjectID;
                    string dm = vd.kmdm;
                    dr["AccountCode"] = dm.Trim(); //dm.TrimEnd('.');
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
                if (IsAux)
                    BuildTypeCode(accountTable, conStr);
                string execSQL = " truncate table ACCOUNT ";
                SqlMapperUtil.CMDExcute(execSQL, null, conStr);
                SqlServerHelper.SqlBulkCopy(accountTable, conStr).Wait();
            }
            catch (Exception err)
            {
                _xdException = err;
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
                var row = accountTable.Rows.Cast<DataRow>().Where(x => x["AccountCode"].ToString() == k.Trim()).SingleOrDefault();
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

                string sql = "select 1 from sys.columns  where object_id in(select object_id from sys.objects where name = 'xm') and name = 'xmdm'";
                int pzqj = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                if (pzqj != 1)
                {
                    IsAux = false;
                    return true;
                }
                sql = "select 1  from sysobjects  where id = object_id('xmye')    and type = 'U'";
                pzqj = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                if (pzqj != 1)
                {
                    IsAux = false;
                    return true;
                }
                string projectsql = " truncate table PROJECT  ; insert into  project(projectid,typecode,projectcode,projectname,uppercode,jb,ismx) " +
                    "   SELECT '' as projectid, LTRIM(LEFT(XMDM, CHARINDEX('.', XMDM))) as TypeCode ,LTRIM(rtrim((XMDM))) as ProjectCode,isnull(XMMC, space(0)) as ProjectName,NULL as uppercode,XMJB as jb,XMMX as ismx    FROM XM;                ";
                SqlMapperUtil.CMDExcute(projectsql, null, conStr);

                //string mxjb = " select MAX(JB) from [ProJect]";
                //int mj = SqlMapperUtil.SqlWithParamsSingle<int>(mxjb, null, conStr);
                //if (mj < 4)
                //{
                //    string jbsql = "update  p1 set  p1.UPPERCODE = p2.PROJECTCODE  from ProJect p1 join ProJect p2 on p1.JB =p2.JB+1  and p1.TYPECODE = p2.TYPECODE   and  left(p1.PROJECTCODE,len(p2.PROJECTCODE)) = p2.PROJECTCODE and p1.jb>1 AND p1.UPPERCODE IS NULL	 ";
                //    SqlMapperUtil.CMDExcute(jbsql, null, conStr);
                //}
                //else
                //{
                //    int m = 1;
                //    while (m != mj)
                //    {
                //        m = m + 1;
                //        string jbsql = "update  p1 set  p1.UPPERCODE = p2.PROJECTCODE  from ProJect p1 join ProJect p2 on p1.JB =p2.JB+1  and p1.TYPECODE = p2.TYPECODE   and  left(p1.PROJECTCODE,len(p2.PROJECTCODE)) = p2.PROJECTCODE and p1.jb>1  AND p1.UPPERCODE IS NULL  and p2.JB<=" + m;
                //        SqlMapperUtil.CMDExcute(jbsql, null, conStr);
                //    }
                //}
                sql = "select 1  from sysobjects  where id = object_id('t_itemclass')    and type = 'U'";
                pzqj = SqlMapperUtil.SqlWithParamsSingle<int>(sql, null, conStr);
                if (pzqj != 1)
                {
                    IsAux = false;
                    return true;
                }
                string projecttypesql = " truncate table ProjectType  ; INSERT  ProjectType  SELECT   '" + xfile.ProjectID + "', FITEMID,FName FROM t_itemclass" +
                    " ; update  PROJECTTYPE set TypeCode=LTRIM(rtrim(TypeCode))   ";
                SqlMapperUtil.CMDExcute(projecttypesql, null, conStr);
            }
            catch (Exception err)
            {
                _xdException = err;
                return false;
            }
            return true;
        }
        private void InitDataBase(string dbName)
        {
            string conStr = StaticUtil.GetConfigValueByKey("XDataConn");
            SqlMapperUtil.GetOpenConnection(conStr);
            string exsitsDB = "select count(1) from sys.sysdatabases where name =@dbName";
            int result = SqlMapperUtil.SqlWithParamsSingle<int>(exsitsDB, new { dbName = dbName });

            if (result == 0)
            {
                string s1 = " create database [" + dbName + "]";
                int ret = SqlMapperUtil.InsertUpdateOrDeleteSql(s1, null);
            }
            else
            {
                string sql = "  exec dropdb '" + dbName + "'";
                int ret = SqlMapperUtil.InsertUpdateOrDeleteSql(sql, null);
            }
            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(conStr);
            csb.InitialCatalog = dbName;
            conStr = csb.ConnectionString;
            var StaticStructAndFn = Path.Combine(Directory.GetCurrentDirectory(), "StaticStructAndFn.tsql");
            var sqls = File.ReadAllText(StaticStructAndFn);
            SqlServerHelper.ExecuteSqlWithGoSplite(sqls, conStr);
            string kjqjInsert = "delete dbo.kjqj where Projectid='{0}'  " +
                " insert  dbo.kjqj(ProjectID,CustomerCode,CustomerName,BeginDate,EndDate,KJDate)" +
                "  select '{0}','{1}','{1}','{2}','{3}','{4}'";
            SqlServerHelper.ExecuteSqlWithGoSplite(string.Format(kjqjInsert, dbName, xfile.ProjectID, _beginDate, _endDate, _auditYear), conStr);

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
                InitDataBase(localDbName);
                Array.ForEach(dbFiles.ToArray(), (string dbfile) =>
                {
                   PD2SqlDB(dbfile);
                 });

                #endregion
            }
            catch (Exception err)
            {
                _xdException = err;
                return false;
            }
            return true;

        }
        private bool PD2SqlDB(string filepath)
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
                    SqlServerHelper.ExecuteSqlWithGoSplite(createDTSql, conStr);
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
            catch (Exception err)
            {
                _xdException = err;
                return false;
            }
            return true;
        }
        private readonly object ofile = new object();
        private string[] UnZipFile(string zzero1F)
        {

            var tmpFolder = zzero1F.Remove(zzero1F.LastIndexOf('.'));
            if (Directory.Exists(tmpFolder))
            {
                //tmpFolder = tmpFolder + DateTime.Now.Ticks;
                //Directory.CreateDirectory(tmpFolder);
                string[] fs = Directory.GetFiles(tmpFolder);
                if (fs.Length > 0)
                {
                    foreach (var f in fs)
                    {
                        File.SetAttributes(f, FileAttributes.Normal);
                        File.Delete(f);
                    }
                }
            }
            else
            {
                Directory.CreateDirectory(tmpFolder);
            }
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
                //throw new Exception("解压001文件错误:" + ex.Message, ex);
                response.ResultContext = " " + ex.Message;
                Console.WriteLine(ex.Message);
                return null;
            }

        }
    }
}
