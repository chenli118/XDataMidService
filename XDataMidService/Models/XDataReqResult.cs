using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace XDataMidService.Models
{

    [Serializable]
    public class XDataResponse
    {
        public int HttpStatusCode { get; set; }
        public string ResultContext { get; set; }
    }
    public class XDataReqResult: IActionResult
    {
        string _content, _reasonPhrase;
        int _statusCode;
        HttpRequestMessage _request;

        public XDataReqResult(string content,string reasonPhrase, int statusCode, HttpRequestMessage request)
        {
            _content = content;
            _reasonPhrase = reasonPhrase;
            _statusCode = statusCode;
            _request = request;
        }
        public Task ExecuteAsync(ActionContext context=null)
        {
            return ExecuteResultAsync(context);
        }

        public Task ExecuteResultAsync(ActionContext context)
        {
            var response = new XDataResponse()
            {
                ResultContext = _content,
                HttpStatusCode = _statusCode
            };
            return Task.FromResult<XDataResponse>(response);
        }
    } 
}
