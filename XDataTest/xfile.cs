using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace XDataTest
{
    public class xfile
    {
        //TDS26MPvb select  name  , 'public string '+name+' { get; set; }' from  sys.columns where object_id in (select object_id from sys.objects where name ='AuthorizeXFiles')
        public int xID { get; set; }
        public string customID { get; set; }
        public string customName { get; set; }
        public string fileName { get; set; }
        public string ztid { get; set; }
        public string ztName { get; set; }
        public string ztYear { get; set; }
        public string beginMonth { get; set; }
        public string endMonth { get; set; }
        public string pzBeginDate { get; set; }
        public string pzEndDate { get; set; }
        public string mountType { get; set; }
        public string mountTime { get; set; }
        public string currency { get; set; }
        public int fileSize { get; set; }
        public string uploadUser { get; set; }
        public DateTime uploadTime { get; set; }
        public int DataStatus { get; set; }
        public string wp_GUID { get; set; }
        public string projectID { get; set; }
        public string dbName { get; set; }
    }
}
