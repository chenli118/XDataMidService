using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
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
        }
        [HttpPost]
        [Route("XData2SQL")]
        public Task<HttpResponseMessage> XData2SQL([FromBody] Models.xfile xfile)
        {
            string targetPath = xfile.FileName;
            string wp_GUID = xfile.CustomID;
            string projectID = xfile.ZTID.Replace("-", "");
            PDT2SDT dT2SDT = new PDT2SDT(targetPath, wp_GUID, projectID, xfile);
            if (dT2SDT.DownLoadFile(xfile))
                return dT2SDT.Start();
            HttpRequestMessage requestMessage = new HttpRequestMessage();
            return new XDataReqResult(string.Format("{0}下载{1}文件失败",xfile.CustomName,xfile.FileName), "从网盘下载文件失败", System.Net.HttpStatusCode.ExpectationFailed, requestMessage).ExecuteAsync();

        }
    }
}