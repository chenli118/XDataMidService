using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace XDataMidService.Controllers
{
    public class XDReqResult
    {
        public Exception Exception { get; set; }
        public object Data { get; set; }
    }
    public class XDReqActionResult : IActionResult
    {
        private readonly XDReqResult _result;

        public XDReqActionResult(XDReqResult result)
        {
            _result = result;
        }

        public async Task ExecuteResultAsync(ActionContext context)
        {
            var objectResult = new ObjectResult(_result.Exception ?? _result.Data)
            {
                StatusCode = _result.Exception != null
                    ? StatusCodes.Status500InternalServerError
                    : StatusCodes.Status200OK
            };
            //return Task.FromResult<XDataResponse>(objectResult);
            await objectResult.ExecuteResultAsync(context);
        }
    }
}
