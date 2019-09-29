using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace XDataMidService.Models
{

    public class Person
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int Age { get; set; }
    }
    public class xfile
    {
        //TDS26MPvb select  name  , 'public string '+name+' { get; set; }' from  sys.columns where object_id in (select object_id from sys.objects where name ='AuthorizeXFiles')

        public string CustomID { get; set; }
        public string CustomName { get; set; }
        public string FileName { get; set; }
        public string ZTID { get; set; }
        public string ZTName { get; set; }
        public string ZTYear { get; set; }
        public string BeginMonth { get; set; }
        public string EndMonth { get; set; }
        public string PZBeginDate { get; set; }
        public string PZEndDate { get; set; }
        public string MountType { get; set; }
        public string MountTime { get; set; }
        public string Currency { get; set; }
        public int FileSize { get; set; }
        public string UploadUser { get; set; }
        public DateTime UploadTime { get; set; }
    }
}
