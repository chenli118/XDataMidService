using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XDataMidService
{
    public static class StaticUtil
    {
        public static IConfigurationBuilder _xDataConfig{ get; set; }
        public static IConfigurationBuilder  XDataConfig { get {
                if (_xDataConfig == null)
                {
                    _xDataConfig = new ConfigurationBuilder().AddJsonFile("WebApp.Config.json", optional: false, reloadOnChange: true);
                    return _xDataConfig;
                }
                else return _xDataConfig;
            } 
        }
        public static string GetConfigValueByKey(string confKey="")
        {
            if (confKey.Length == 0) confKey = "XDataConn"; 
            var configure = XDataConfig.Build();    
            return  configure.GetConnectionString(confKey);
        }
        public static string GetLocalDbNameByXFile(XDataMidService.Models.xfile xfile)
        {
            byte[] asciiBytes = Encoding.ASCII.GetBytes(xfile.XID+xfile.CustomID.Replace("-","")+ xfile.ZTYear + xfile.PZBeginDate + xfile.PZEndDate);
            StringBuilder sb = new StringBuilder();
            Array.ForEach(asciiBytes, (c) =>
            {
                if ((c > 47 && c < 58)
                || (c > 64 && c < 91)
                || (c > 96 && c < 123))
                { sb.Append((char)c); }
            });
            string dbName = sb.ToString();
            if (dbName.Length > 50) dbName = dbName.Substring(0, 49);
            return dbName;
        }
    }
    public static class StaticData
    {
        public static Dictionary<string,int> X2SqlList { get; set; }
        public static Dictionary<string, string> X2EasList { get; set; }
    }
}
