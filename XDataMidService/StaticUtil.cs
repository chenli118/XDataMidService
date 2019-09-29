using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace XDataMidService
{
    public static class StaticUtil
    {
        public static string GetConfigValueByKey(string confKey="")
        {
            if (confKey.Length == 0) confKey = "DefaultConnection";
            var builder = new ConfigurationBuilder().AddJsonFile("WebApp.Config.json", optional: false, reloadOnChange: true);
            var configure = builder.Build();    
            return  configure.GetConnectionString(confKey);
        }

    }
}
