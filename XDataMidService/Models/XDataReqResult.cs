using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace XDataMidService.Models
{
    public class XDataReqResult
    {
        string _content, _reasonPhrase;
        HttpStatusCode _statusCode;
        HttpRequestMessage _request;

        public XDataReqResult(string content,string reasonPhrase, HttpStatusCode statusCode, HttpRequestMessage request)
        {
            _content = content;
            _reasonPhrase = reasonPhrase;
            _statusCode = statusCode;
            _request = request;
        }
        public Task<HttpResponseMessage> ExecuteAsync(CancellationToken cancellationToken = default)
        {
            var response = new HttpResponseMessage()
            {
                Content = new StringContent(_content),
                StatusCode =_statusCode,
                ReasonPhrase = _reasonPhrase,
                RequestMessage = _request
            };
            return Task.FromResult(response);
        }

    } 
}
