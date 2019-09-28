using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace XDATA2EAS.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class XFileController : ControllerBase
    {
        // GET: api/<controller>
        [HttpGet]
        public IEnumerable<Models.xfile> Get()
        {
            string itemclass = "select * from AuthorizeXFiles";
            var tab_ic = SqlMapperUtil.SqlWithParams<Models.xfile>(itemclass, null);
            return tab_ic;
        }

        // GET api/<controller>/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            return "value";
        }

        // POST api/<controller>
        [HttpPost]
        public void Post([FromBody]string value)
        {



        }

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
