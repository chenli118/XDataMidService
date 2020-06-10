using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Dapper;
using Microsoft.Extensions.Configuration;

namespace XDataMidService
{
    public static class SqlMapperUtil
    {
         private static string connectionString =string.Empty;

        /// <summary>
        /// Gets the open connection.
        /// </summary>
        /// <param name="name">The name of the connection string (optional).</param>
        /// <returns></returns>
        public static SqlConnection GetOpenConnection(string name = null)
        {
            string connString = "";
            if (connectionString.Length == 0)
            { 
                connString = connectionString = StaticUtil.GetConfigValueByKey(name);
            }
            if (name != null) connectionString = connString = name; else connString = connectionString;
            var connection = new SqlConnection(connString);
            try
            {
                if (connection.State != ConnectionState.Open) connection.Open();
            }
            catch
            {
                if (connection.State != ConnectionState.Open) connection.Open();
            }
            if (connection.State != ConnectionState.Open) connection.Open();
            return connection;
        }


        public static int InsertMultiple<T>(string sql, IEnumerable<T> entities, string connectionName = null) where T : class, new()
        {
            using (SqlConnection cnn = GetOpenConnection(connectionName))
            {
                int records = 0;

                foreach (T entity in entities)
                {
                    records += cnn.Execute(sql, entity);
                }
                return records;
            }
        }

        public static DataTable ToDataTable<T>(this IList<T> list)
        {
            PropertyDescriptorCollection props = TypeDescriptor.GetProperties(typeof(T));
            DataTable table = new DataTable();
            for (int i = 0; i < props.Count; i++)
            {
                PropertyDescriptor prop = props[i];
                table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            }
            object[] values = new object[props.Count];
            foreach (T item in list)
            {
                for (int i = 0; i < values.Length; i++)
                    values[i] = props[i].GetValue(item) ?? DBNull.Value;
                table.Rows.Add(values);
            }
            return table;
        }

        public static DynamicParameters GetParametersFromObject(object obj, string[] propertyNamesToIgnore)
        {
            if (propertyNamesToIgnore == null) propertyNamesToIgnore = new string[] { String.Empty };
            DynamicParameters p = new DynamicParameters();
            PropertyInfo[] properties = obj.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (PropertyInfo prop in properties)
            {
                if (!propertyNamesToIgnore.Contains(prop.Name))
                    p.Add("@" + prop.Name, prop.GetValue(obj, null));
            }
            return p;
        }

        public static void SetIdentity<T>(IDbConnection connection, Action<T> setId)
        {
            dynamic identity = connection.Query("SELECT @@IDENTITY AS Id").Single();
            T newId = (T)identity.Id;
            setId(newId);
        }


        public static object GetPropertyValue(object target, string propertyName)
        {
            PropertyInfo[] properties = target.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

            object theValue = null;
            foreach (PropertyInfo prop in properties)
            {
                if (string.Compare(prop.Name, propertyName, true) == 0)
                {
                    theValue = prop.GetValue(target, null);
                }
            }
            return theValue;
        }

        public static void SetPropertyValue(object p, string propName, object value)
        {
            Type t = p.GetType();
            PropertyInfo info = t.GetProperty(propName);
            if (info == null)
                return;
            if (!info.CanWrite)
                return;
            info.SetValue(p, value, null);
        }

        /// <summary>
        /// Stored proc.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="procname">The procname.</param>
        /// <param name="parms">The parms.</param>
        /// <returns></returns>
        public static List<T> StoredProcWithParams<T>(string procname, dynamic parms, string connectionName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                return connection.Query<T>(procname, (object)parms, commandType: CommandType.StoredProcedure).ToList();
            }

        }


        /// <summary>
        /// Stored proc with params returning dynamic.
        /// </summary>
        /// <param name="procname">The procname.</param>
        /// <param name="parms">The parms.</param>
        /// <param name="connectionName">Name of the connection.</param>
        /// <returns></returns>
        public static List<dynamic> StoredProcWithParamsDynamic(string procname, dynamic parms, string connectionName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                return connection.Query(procname, (object)parms, commandType: CommandType.StoredProcedure).ToList();
            }
        }

        /// <summary>
        /// Stored proc insert with ID.
        /// </summary>
        /// <typeparam name="T">The type of object</typeparam>
        /// <typeparam name="U">The Type of the ID</typeparam>
        /// <param name="procName">Name of the proc.</param>
        /// <param name="parms">instance of DynamicParameters class. This        should include a defined output parameter</param>
      /// <returns>U - the @@Identity value from output parameter</returns>
      public static U StoredProcInsertWithID<T, U>(string procName, DynamicParameters parms, string connectionName = null)
        {
            using (SqlConnection connection = SqlMapperUtil.GetOpenConnection(connectionName))
            {
                var x = connection.Execute(procName, (object)parms, commandType: CommandType.StoredProcedure);
                return parms.Get<U>("@ID");
            }
        }


        /// <summary>
        /// SQL with params.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql">The SQL.</param>
        /// <param name="parms">The parms.</param>
        /// <returns></returns>
        public static List<T> SqlWithParams<T>(string sql, dynamic parms, string connectionnName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionnName))
            {
                return connection.Query<T>(sql, (object)parms).ToList();
            }
        }

        /// <summary>
        /// Insert update or delete SQL.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <param name="parms">The parms.</param>
        /// <returns></returns>
        public static int InsertUpdateOrDeleteSql(string sql, dynamic parms, string connectionName = null)
        {   
            using (SqlConnection connection = GetOpenConnection(connectionName))
            { 
                return connection.Execute(sql, (object)parms);
            }
        }
        public static int CMDExcute(string sql, dynamic parms, string connectionName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                SqlCommand sqlCommand = new SqlCommand(sql, connection);
                sqlCommand.CommandTimeout = 1000*1200;
                return sqlCommand.ExecuteNonQuery();
            }
        }
        public static void ExecuteNonQueryBatch(string connectionName, string sql)
        {
            if (sql == null) throw new ArgumentNullException("sqlStatements");
            if (connectionName == null) throw new ArgumentNullException("connectionString");

            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                Regex r = new Regex(@"^(\s|\t)*go(\s\t)?.*", RegexOptions.Multiline | RegexOptions.IgnoreCase);

                foreach (string s in r.Split(sql))
                {
                    //Skip empty statements, in case of a GO and trailing blanks or something
                    string thisStatement = s.Trim();
                    if (String.IsNullOrEmpty(thisStatement)) continue;

                    using (SqlCommand cmd = new SqlCommand(thisStatement, connection))
                    {
                        cmd.CommandType = CommandType.Text;

                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }
        /// <summary>
        /// Insert update or delete stored proc.
        /// </summary>
        /// <param name="procName">Name of the proc.</param>
        /// <param name="parms">The parms.</param>
        /// <returns></returns>
        public static int InsertUpdateOrDeleteStoredProc(string procName, dynamic parms, string connectionName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                return connection.Execute(procName, (object)parms, commandType: CommandType.StoredProcedure);
            }
        }

        /// <summary>
        /// SQLs the with params single.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql">The SQL.</param>
        /// <param name="parms">The parms.</param>
        /// <param name="connectionName">Name of the connection.</param>
        /// <returns></returns>
        public static T SqlWithParamsSingle<T>(string sql, dynamic parms, string connectionName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                return connection.Query<T>(sql, (object)parms).FirstOrDefault();
            }
        }

        /// <summary>
        ///  proc with params single returning Dynamic object.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql">The SQL.</param>
        /// <param name="parms">The parms.</param>
        /// <param name="connectionName">Name of the connection.</param>
        /// <returns></returns>
        public static System.Dynamic.DynamicObject DynamicProcWithParamsSingle<T>(string sql, dynamic parms, string connectionName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                return connection.Query(sql, (object)parms, commandType: CommandType.StoredProcedure).FirstOrDefault();
            }
        }

        /// <summary>
        /// proc with params returning Dynamic.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql">The SQL.</param>
        /// <param name="parms">The parms.</param>
        /// <param name="connectionName">Name of the connection.</param>
        /// <returns></returns>
        public static IEnumerable<dynamic> DynamicProcWithParams<T>(string sql, dynamic parms, string connectionName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                return connection.Query(sql, (object)parms, commandType: CommandType.StoredProcedure);
            }
        }


        /// <summary>
        /// Stored proc with params returning single.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="procname">The procname.</param>
        /// <param name="parms">The parms.</param>
        /// <param name="connectionName">Name of the connection.</param>
        /// <returns></returns>
        public static T StoredProcWithParamsSingle<T>(string procname, dynamic parms, string connectionName = null)
        {
            using (SqlConnection connection = GetOpenConnection(connectionName))
            {
                return connection.Query<T>(procname, (object)parms, commandType: CommandType.StoredProcedure).SingleOrDefault();
            }
        }
    }

    public class DapperHelper<T>
    {
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public static  string connectionString = string.Empty;
        public string conStr { get { return connectionString; } set { connectionString = value; } }

        public static DapperHelper<T> Create(string confKey = "",string conStr="")
        {
            if (conStr.Length == 0)
                connectionString = StaticUtil.GetConfigValueByKey(confKey);
            else
                connectionString = conStr;
            return new DapperHelper<T>();
        }
            
        

        /// <summary>
        /// 查询列表
        /// </summary>
        /// <param name="sql">查询的sql</param>
        /// <param name="param">替换参数</param>
        /// <returns></returns>
        public  List<T> Query(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.Query<T>(sql, param).ToList();
            }
        }
        /// <summary>
        /// 查询列表
        /// </summary>
        /// <param name="sql">查询的sql</param>
        /// <param name="param">替换参数</param>
        /// <returns></returns>
        public  List<T> QuerySP(string spName, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.Query<T>(spName, param, commandType: CommandType.StoredProcedure).ToList();
            }
        }

        /// <summary>
        /// 查询第一个数据
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  T QueryFirst(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.QueryFirst<T>(sql, param);
            }
        }

        /// <summary>
        /// 查询第一个数据没有返回默认值
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  T QueryFirstOrDefault(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.QueryFirstOrDefault<T>(sql, param);
            }
        }

        /// <summary>
        /// 查询单条数据
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  T QuerySingle(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.QuerySingle<T>(sql, param);
            }
        }

        /// <summary>
        /// 查询单条数据没有返回默认值
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  T QuerySingleOrDefault(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.QuerySingleOrDefault<T>(sql, param);
            }
        }

        /// <summary>
        /// 增删改
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  int Execute(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.Execute(sql, param);
            }
        }

        /// <summary>
        /// 增删改
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  int ExecuteSP(string spName, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                var p = new DynamicParameters();
                p.Add("@a", 11);
                p.Add("@b", dbType: DbType.Int32, direction: ParameterDirection.Output);
                p.Add("@c", dbType: DbType.Int32, direction: ParameterDirection.ReturnValue);

                con.Execute("spMagicProc", p, commandType: CommandType.StoredProcedure);

                int b = p.Get<int>("@b");
                int c = p.Get<int>("@c");
                return c;
            }
        }

        /// <summary>
        /// Reader获取数据
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  IDataReader ExecuteReader(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.ExecuteReader(sql, param);
            }
        }

        /// <summary>
        /// Scalar获取数据
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  object ExecuteScalar(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.ExecuteScalar(sql, param);
            }
        }

        /// <summary>
        /// Scalar获取数据
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  T ExecuteScalarForT(string sql, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                return con.ExecuteScalar<T>(sql, param);
            }
        }

        /// <summary>
        /// 带参数的存储过程
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public  List<T> ExecutePro(string proc, object param)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                List<T> list = con.Query<T>(proc,
                    param,
                    null,
                    true,
                    null,
                    CommandType.StoredProcedure).ToList();
                return list;
            }
        }


        /// <summary>
        /// 事务1 - 全SQL
        /// </summary>
        /// <param name="sqlarr">多条SQL</param>
        /// <param name="param">param</param>
        /// <returns></returns>
        public  Tuple<int,string> ExecuteTransactionAndDBSigleUser(string[] sqlarr)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                try
                {
                    if (con.State != ConnectionState.Open) con.Open();
                }
                catch
                {
                    if (con.State != ConnectionState.Open) con.Open();
                }
                if (con.State != ConnectionState.Open) con.Open();
                using (var transaction = con.BeginTransaction())
                {
                    try
                    {
                        int result = 0;
                        foreach (var sql in sqlarr)
                        {
                            result += con.Execute(sql, null, transaction,0);
                        }

                        transaction.Commit();
                        return new Tuple<int, string>(1,"Committed");
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return new Tuple<int, string>(0,ex.Message);
                    }
                }
            }
        }

        /// <summary>
        /// 事务2 - 声明参数
        ///demo:
        ///dic.Add("Insert into Users values (@UserName, @Email, @Address)",
        ///        new { UserName = "jack", Email = "380234234@qq.com", Address = "上海" });
        /// </summary>
        /// <param name="Key">多条SQL</param>
        /// <param name="Value">param</param>
        /// <returns></returns>
        public  int ExecuteTransaction(Dictionary<string, object> dic)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                using (var transaction = con.BeginTransaction())
                {
                    try
                    {
                        int result = 0;
                        foreach (var sql in dic)
                        {
                            result += con.Execute(sql.Key, sql.Value, transaction);
                        }

                        transaction.Commit();
                        return result;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        return 0;
                    }
                }
            }
        }
    }
}