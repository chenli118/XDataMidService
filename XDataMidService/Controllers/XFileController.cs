using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace XDataMidService.Controllers
{
    [ApiController] 
    [Route("[controller]")]
    public class XFileController : ControllerBase
    {

        private readonly ILogger<XFileController> _logger;

        public XFileController(ILogger<XFileController> logger)
        {
            _logger = logger;
        }
        // GET: api/<controller>
        [HttpGet]
        public IEnumerable<Models.xfile> Get()
        {
            string itemclass = "select * from AuthorizeXFiles";
            var constr = StaticUtil.GetConfigValueByKey("");
            var tab_ic = SqlMapperUtil.SqlWithParams<Models.xfile>(itemclass, null, constr);
            return tab_ic;
        }

        // GET api/<controller>/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            return "value";
        }

        //POST api/<controller>
        //[HttpPost]

        //public void Post([FromBody] Models.Person value)
        //{



        //}
        [HttpPost]
        public void Post([FromBody] Models.xfile xfile)
        {



        }
        // POST api/<controller>
        //[HttpPost] 
        //public void Post(Models.xfile value)
        //{



        //}

        // PUT api/<controller>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/<controller>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
